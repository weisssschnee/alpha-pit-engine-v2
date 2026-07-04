"""Audit historical CN true1min results for forbidden short-leg reward usage.

Phase3CM and Phase3BZ originally used top-minus-bottom / long-short spread
returns. That is a useful ranking diagnostic, but it is not a China A-share
tradable reward because the CN stock line must ban shorting. This script scans
runtime/report artifacts, classifies their shorting policy, and builds a
candidate pack for long-only recheck.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3ei_ban_short_lineage_audit_20260704")
DEFAULT_REPORT_ROOT = Path("reports/phase3ei_ban_short_lineage_audit_20260704")

CANDIDATE_FILE_HINTS = (
    "phase3cm_train_reward.csv",
    "phase3cm_candidate_train_reward_summary.csv",
    "phase3bz_candidate_fragment_summary.csv",
    "phase3bz_fragment_replay_summary.csv",
    "phase3dy_true1min_tplus1_tradable_replay.csv",
    "phase3dy_candidate_tradable_summary.csv",
    "phase3dw",
    "phase3eg",
    "phase3eh",
)
SKIP_ROW_LEVEL_HINTS = (
    "portfolio_pnl_rows",
    "curve_rows",
    "fragment_rows",
    "candidate_progress",
    "split_horizon",
    "shard_meta",
    "chunk_status",
)
RISKY_PATH_HINTS = ("phase3cm", "phase3bz", "phase3eg", "phase3eh")
LONG_ONLY_PATH_HINTS = ("phase3dy", "longonly", "long_only")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _f(value: Any, default: float = float("nan")) -> float:
    try:
        if value in (None, ""):
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _digest(expression: str) -> str:
    return hashlib.sha256(expression.encode("utf-8")).hexdigest()[:24]


def _read_header(path: Path) -> list[str]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle)
            return next(reader, [])
    except Exception:
        return []


def _iter_csv_rows(path: Path, limit: int | None = None) -> tuple[list[str], list[dict[str, str]], str | None]:
    rows: list[dict[str, str]] = []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            header = list(reader.fieldnames or [])
            for idx, row in enumerate(reader):
                rows.append(dict(row))
                if limit is not None and idx + 1 >= limit:
                    break
        return header, rows, None
    except Exception as exc:
        return [], [], str(exc)


def _classify_csv(path: Path, header: list[str], sample_rows: list[dict[str, str]]) -> tuple[str, str, int, int]:
    path_text = str(path).lower()
    portfolio_modes = {str(row.get("portfolio_mode") or "").strip() for row in sample_rows}
    portfolio_modes.discard("")
    short_allowed_values = {str(row.get("short_allowed") or "").strip().lower() for row in sample_rows}
    short_allowed_values.discard("")
    short_counts = [_f(row.get("short_count"), 0.0) for row in sample_rows if "short_count" in row]
    short_positive = sum(1 for value in short_counts if math.isfinite(value) and value > 0)
    short_zero = sum(1 for value in short_counts if math.isfinite(value) and value == 0)

    if portfolio_modes and all(mode.startswith("long_only") for mode in portfolio_modes) and short_positive == 0:
        return "cn_long_only_verified", "portfolio_mode long_only and sample short_count zero", short_positive, short_zero
    if "true" in short_allowed_values or "long_short_spread" in portfolio_modes:
        return "long_short_spread_proxy_only", "short_allowed true or portfolio_mode long_short_spread", short_positive, short_zero
    if short_positive > 0:
        return "short_leg_present_proxy_only", "sample rows have short_count > 0", short_positive, short_zero
    if any(hint in path_text for hint in LONG_ONLY_PATH_HINTS) or any(col.startswith("long_only") for col in header):
        return "likely_cn_long_only", "path or columns indicate long-only", short_positive, short_zero
    if any(hint in path_text for hint in RISKY_PATH_HINTS):
        return "legacy_assume_long_short_recheck_required", "Phase3CM/BZ/EG/EH artifact lacks explicit long-only proof", short_positive, short_zero
    return "unknown_not_prioritized", "no explicit policy marker", short_positive, short_zero


def _is_candidate_level_csv(path: Path, header: list[str]) -> bool:
    name = path.name.lower()
    path_text = str(path).lower()
    if any(hint in name for hint in SKIP_ROW_LEVEL_HINTS) or any(hint in path_text for hint in SKIP_ROW_LEVEL_HINTS):
        return False
    if "expression" not in header and "formula" not in header:
        return False
    if not any(col in header for col in ("train_reward", "optimizer_reward", "fragment_sortino", "day_sortino", "long_only_sortino", "sortino", "score")):
        return False
    return True


def _candidate_score(row: dict[str, str]) -> tuple[float, str]:
    for key in (
        "train_reward",
        "optimizer_reward",
        "train_day_sortino",
        "fragment_sortino",
        "day_sortino",
        "long_only_sortino",
        "sortino",
        "phase3ca_proxy_quality",
        "aligned_ic_mean",
        "score",
    ):
        value = _f(row.get(key))
        if math.isfinite(value):
            return value, key
    return float("-inf"), "none"


def _candidate_decision(row: dict[str, str]) -> str:
    for key in ("train_reward_decision", "decision", "fragment_decision", "final_decision", "status"):
        text = str(row.get(key) or "").strip()
        if text:
            return text
    return ""


def _collect_recheck_candidates(
    csv_files: list[Path],
    policy_by_path: dict[str, str],
    *,
    max_rows_per_file: int,
    top_n: int,
) -> list[dict[str, Any]]:
    candidates: dict[str, dict[str, Any]] = {}
    for path in csv_files:
        header = _read_header(path)
        if not _is_candidate_level_csv(path, header):
            continue
        policy = policy_by_path.get(str(path), "unknown_not_prioritized")
        if policy in {"cn_long_only_verified", "likely_cn_long_only"}:
            continue
        _, rows, error = _iter_csv_rows(path, limit=max_rows_per_file)
        if error:
            continue
        for row in rows:
            expression = str(row.get("expression") or row.get("formula") or "").strip()
            if not expression:
                continue
            expression_hash = str(row.get("expression_hash") or "").strip() or _digest(expression)
            score, metric = _candidate_score(row)
            decision = _candidate_decision(row)
            keep_priority = (
                1 if "FOLLOWUP" in decision.upper() or "READY" in decision.upper() or "PASS" in decision.upper() else 0
            )
            existing = candidates.get(expression_hash)
            if existing is not None:
                old_score = _f(existing.get("prior_score"), float("-inf"))
                old_priority = int(existing.get("prior_decision_priority") or 0)
                if (keep_priority, score) <= (old_priority, old_score):
                    continue
            out = dict(row)
            out["candidate_id"] = row.get("candidate_id") or f"phase3ei_{len(candidates) + 1:05d}"
            out["expression"] = expression
            out["expression_hash"] = expression_hash
            out["phase3ei_prior_short_policy"] = policy
            out["phase3ei_source_path"] = str(path.relative_to(REPO) if path.is_relative_to(REPO) else path)
            out["phase3ei_prior_metric"] = metric
            out["phase3ei_prior_score"] = "" if not math.isfinite(score) else round(score, 10)
            out["phase3ei_prior_decision"] = decision
            out["phase3ei_prior_decision_priority"] = keep_priority
            out["phase3ei_recheck_required"] = True
            out["phase3ei_required_portfolio_mode"] = "long_only_top"
            candidates[expression_hash] = out
    ranked = sorted(
        candidates.values(),
        key=lambda row: (int(row.get("phase3ei_prior_decision_priority") or 0), _f(row.get("phase3ei_prior_score"), -999.0)),
        reverse=True,
    )
    return ranked[:top_n]


def _scan(root: Path, max_sample_rows: int) -> tuple[list[dict[str, Any]], list[Path], dict[str, str]]:
    inventory: list[dict[str, Any]] = []
    candidate_csvs: list[Path] = []
    policy_by_path: dict[str, str] = {}
    for base in (root / "reports", root / "runtime"):
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if not path.is_file():
                continue
            suffix = path.suffix.lower()
            path_text = str(path).lower()
            if suffix not in {".csv", ".json", ".md"}:
                continue
            if not any(hint in path_text for hint in ("phase3cm", "phase3bz", "phase3dy", "phase3eg", "phase3eh")):
                continue
            rel = str(path.relative_to(root) if path.is_relative_to(root) else path)
            row: dict[str, Any] = {
                "path": rel,
                "suffix": suffix,
                "size_bytes": path.stat().st_size,
                "policy_class": "unknown_not_prioritized",
                "reason": "",
                "short_positive_sample_rows": 0,
                "short_zero_sample_rows": 0,
                "candidate_level": False,
            }
            if suffix == ".csv":
                header, sample, error = _iter_csv_rows(path, limit=max_sample_rows)
                row["column_count"] = len(header)
                row["sample_rows"] = len(sample)
                if error:
                    row["policy_class"] = "read_error"
                    row["reason"] = error
                else:
                    policy, reason, short_positive, short_zero = _classify_csv(path, header, sample)
                    row["policy_class"] = policy
                    row["reason"] = reason
                    row["short_positive_sample_rows"] = short_positive
                    row["short_zero_sample_rows"] = short_zero
                    row["candidate_level"] = _is_candidate_level_csv(path, header)
                    if row["candidate_level"]:
                        candidate_csvs.append(path)
                policy_by_path[str(path)] = str(row["policy_class"])
            elif suffix == ".json":
                text = path.read_text(encoding="utf-8", errors="ignore")[:200000].lower()
                if '"short_allowed": false' in text or '"portfolio_mode": "long_only' in text:
                    row["policy_class"] = "cn_long_only_verified"
                    row["reason"] = "json summary says short_allowed false or long_only portfolio_mode"
                elif '"short_allowed": true' in text or "long_short_spread" in text:
                    row["policy_class"] = "long_short_spread_proxy_only"
                    row["reason"] = "json summary says short allowed or long_short_spread"
                elif any(hint in path_text for hint in LONG_ONLY_PATH_HINTS):
                    row["policy_class"] = "likely_cn_long_only"
                    row["reason"] = "path indicates long-only"
                elif any(hint in path_text for hint in RISKY_PATH_HINTS):
                    row["policy_class"] = "legacy_assume_long_short_recheck_required"
                    row["reason"] = "risky phase json without long-only marker"
            else:
                text = path.read_text(encoding="utf-8", errors="ignore")[:200000].lower()
                if "long-only" in text or "long_only" in text:
                    row["policy_class"] = "likely_cn_long_only"
                    row["reason"] = "markdown mentions long-only"
                elif "top/bottom" in text or "long-short" in text or "short_count" in text:
                    row["policy_class"] = "long_short_spread_proxy_only"
                    row["reason"] = "markdown mentions top/bottom or long-short"
                elif any(hint in path_text for hint in RISKY_PATH_HINTS):
                    row["policy_class"] = "legacy_assume_long_short_recheck_required"
                    row["reason"] = "risky phase markdown without long-only marker"
            inventory.append(row)
    return inventory, candidate_csvs, policy_by_path


def _render_markdown(summary: dict[str, Any], inventory: list[dict[str, Any]], recheck: list[dict[str, Any]]) -> str:
    counts = summary["policy_class_counts"]
    lines = [
        "# Phase3EI Ban-Short Lineage Audit",
        "",
        f"Created: `{summary['created_at']}`",
        "",
        "## Decision",
        "",
        "`PHASE3EI_BAN_SHORT_RECHECK_REQUIRED`",
        "",
        "Historical Phase3CM/Phase3BZ/Phase3EG/Phase3EH spread-style outputs are not CN tradable reward evidence unless rechecked under a long-only portfolio mode.",
        "",
        "## Policy Counts",
        "",
        "| policy | files |",
        "|---|---:|",
    ]
    for key, value in counts.items():
        lines.append(f"| `{key}` | {value} |")
    lines.extend(
        [
            "",
            "## Recheck Pack",
            "",
            f"- candidate rows selected: `{summary['recheck_candidate_count']}`",
            "- required mode: `long_only_top`",
            "- spread/legacy rows are retained only as source attribution, not as alpha proof.",
            "",
            "## Top Recheck Candidates By Prior Score",
            "",
            "| rank | candidate | prior policy | prior metric | prior score | decision | expression |",
            "|---:|---|---|---|---:|---|---|",
        ]
    )
    for idx, row in enumerate(recheck[:30], 1):
        expr = str(row.get("expression") or "").replace("|", "/")[:120]
        lines.append(
            f"| {idx} | `{row.get('candidate_id')}` | `{row.get('phase3ei_prior_short_policy')}` | "
            f"`{row.get('phase3ei_prior_metric')}` | {row.get('phase3ei_prior_score')} | "
            f"`{row.get('phase3ei_prior_decision')}` | `{expr}` |"
        )
    risky = [row for row in inventory if "recheck_required" in str(row.get("policy_class")) or "proxy_only" in str(row.get("policy_class"))]
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- `long_short_spread_proxy_only`: valid only as ranking/orthogonality diagnostic.",
            "- `legacy_assume_long_short_recheck_required`: missing explicit long-only proof; treat as invalid for CN reward until rechecked.",
            "- `cn_long_only_verified` / `likely_cn_long_only`: acceptable lineage class, still not automatic alpha promotion.",
            "",
            "## Sample Risky Files",
            "",
            "| policy | file | reason |",
            "|---|---|---|",
        ]
    )
    for row in risky[:50]:
        lines.append(f"| `{row.get('policy_class')}` | `{row.get('path')}` | {row.get('reason')} |")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=REPO)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--max-sample-rows", type=int, default=200)
    parser.add_argument("--max-rows-per-candidate-file", type=int, default=5000)
    parser.add_argument("--top-n-recheck", type=int, default=1024)
    args = parser.parse_args(argv)

    repo = _resolve(args.repo_root)
    output_root = _resolve(args.output_root)
    report_root = _resolve(args.report_root)
    inventory, candidate_csvs, policy_by_path = _scan(repo, max_sample_rows=max(1, int(args.max_sample_rows)))
    recheck = _collect_recheck_candidates(
        candidate_csvs,
        policy_by_path,
        max_rows_per_file=max(1, int(args.max_rows_per_candidate_file)),
        top_n=max(1, int(args.top_n_recheck)),
    )
    counts: dict[str, int] = {}
    for row in inventory:
        key = str(row.get("policy_class"))
        counts[key] = counts.get(key, 0) + 1
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3EI_BAN_SHORT_RECHECK_REQUIRED",
        "repo": str(repo),
        "inventory_file_count": len(inventory),
        "candidate_source_file_count": len(candidate_csvs),
        "recheck_candidate_count": len(recheck),
        "policy_class_counts": dict(sorted(counts.items())),
        "required_portfolio_mode": "long_only_top",
        "invalid_reward_classes": [
            "long_short_spread_proxy_only",
            "short_leg_present_proxy_only",
            "legacy_assume_long_short_recheck_required",
        ],
    }
    for root in (output_root, report_root):
        _write_csv(root / "phase3ei_ban_short_lineage_inventory.csv", inventory)
        _write_csv(root / "phase3ei_ban_short_recheck_candidate_audit.csv", recheck)
        _write_json(root / "phase3ei_ban_short_lineage_summary.json", summary)
    (report_root / "PHASE3EI_BAN_SHORT_LINEAGE_AUDIT_20260704.md").write_text(
        _render_markdown(summary, inventory, recheck),
        encoding="utf-8",
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
