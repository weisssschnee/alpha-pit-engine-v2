"""Build Phase3CA BZ-input candidate audit from fresh true-1min search outputs.

This is a glue layer, not a new alpha proof. It collects top decision CSVs from
fresh Phase3CA search runs and writes a BZ-compatible candidate table so
fragment replay can become the checkpoint gate.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("reports/phase3ca_company_reward_gated_candidates_20260616")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _round(value: Any, ndigits: int = 8) -> float:
    return round(_f(value), ndigits)


def _iter_decision_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.rglob("*top_decisions.csv"))


def _source_label(path: Path, source_root: Path) -> str:
    try:
        rel = path.relative_to(source_root)
        return str(rel.parent).replace("\\", "/") or source_root.name
    except Exception:
        return path.parent.name


def _quality(row: dict[str, Any]) -> float:
    aligned_ic = abs(_f(row.get("aligned_ic_mean") or row.get("abs_aligned_ic_mean")))
    aligned_spread = _f(row.get("aligned_spread_mean") or row.get("spread_mean"))
    hit = _f(row.get("spread_hit_rate"), 0.5)
    positive_h = _f(row.get("positive_horizon_count"))
    turnover = _f(row.get("mean_one_way_turnover"))
    blockers = str(row.get("phase3bp_blocker_flags") or row.get("blocker_flags") or "")
    future = "future_signal_wrong_lag_too_strong" in blockers
    crowded = "signal_corr_abs" in blockers
    score = 0.0
    score += min(0.30, aligned_ic) * 2.2
    score += min(0.002, abs(aligned_spread)) * 80.0
    score += max(0.0, hit - 0.50) * 0.30
    score += min(4.0, positive_h) * 0.035
    score -= max(0.0, turnover - 0.55) * 0.45
    if future:
        score -= 0.80
    if crowded:
        score -= 0.18
    return round(score, 10)


def _blockers(row: dict[str, Any]) -> str:
    return str(row.get("phase3bp_blocker_flags") or row.get("blocker_flags") or "")


def _hard_reject_reason(row: dict[str, Any], *, allow_high_corr: bool) -> str:
    blockers = _blockers(row)
    if "future_signal_wrong_lag_too_strong" in blockers:
        return "future_signal_wrong_lag_too_strong"
    if (not allow_high_corr) and "signal_corr_abs" in blockers:
        return "signal_corr_abs"
    return ""


def _normalize_row(row: dict[str, str], *, run: str, source_file: Path, source_round: str) -> dict[str, Any] | None:
    digest = str(row.get("expression_hash") or "").strip()
    expr = str(row.get("expression") or "").strip()
    if not digest or not expr:
        return None
    quality = _quality(row)
    # BZ only needs expression fields and uses these score columns for ranking.
    # Keep the names explicit to avoid pretending this is a full reward audit.
    proxy_score = quality
    mcmc_proxy = quality - (0.20 if "future_signal_wrong_lag_too_strong" in str(row.get("phase3bp_blocker_flags") or row.get("blocker_flags") or "") else 0.0)
    out = dict(row)
    out.update(
        {
            "run": run,
            "round_id": source_round,
            "source_file": str(source_file),
            "candidate_id": row.get("candidate_id") or digest[:12],
            "expression_hash": digest,
            "expression": expr,
            "proxy_sortino": _round(proxy_score),
            "mcmc_sortino_median": _round(mcmc_proxy),
            "mcmc_prob_sortino_gt_0": 1.0 if mcmc_proxy > 0 else 0.0,
            "reward_audit_decision": "PHASE3CA_CANDIDATE_FOR_FRAGMENT_REPLAY",
            "phase3ca_proxy_quality": _round(quality),
            "metric_boundary": "Phase3CA proxy ranking only; BZ fragment replay is required before followup.",
        }
    )
    return out


def build_candidate_table(source_roots: list[Path], output_root: Path, top_n: int, *, allow_high_corr: bool) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    sources: list[dict[str, Any]] = []
    rejected: dict[str, int] = {}
    for source_root in source_roots:
        root = _resolve(source_root)
        files = _iter_decision_files(root)
        sources.append({"root": str(root), "top_decision_files": len(files)})
        for file in files:
            run = root.name
            source_round = _source_label(file, root)
            for row in _read_csv(file):
                reject_reason = _hard_reject_reason(row, allow_high_corr=allow_high_corr)
                if reject_reason:
                    rejected[reject_reason] = rejected.get(reject_reason, 0) + 1
                    continue
                normalized = _normalize_row(row, run=run, source_file=file, source_round=source_round)
                if not normalized:
                    continue
                digest = str(normalized["expression_hash"])
                if digest in seen:
                    continue
                seen.add(digest)
                rows.append(normalized)

    rows.sort(
        key=lambda row: (
            _f(row.get("mcmc_sortino_median"), -999.0),
            _f(row.get("proxy_sortino"), -999.0),
            _f(row.get("abs_aligned_ic_mean") or row.get("aligned_ic_mean"), -999.0),
        ),
        reverse=True,
    )
    selected = rows[:top_n]
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "phase3ca_bz_candidate_audit.csv", selected)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260616_phase3ca_build_bz_candidate_audit",
        "decision": "PHASE3CA_BZ_CANDIDATE_AUDIT_READY",
        "candidate_count": len(selected),
        "deduped_source_candidate_count": len(rows),
        "top_n": top_n,
        "allow_high_corr": allow_high_corr,
        "hard_rejected_counts": rejected,
        "sources": sources,
        "metric_boundary": "This is a ranking bridge into BZ, not reward proof.",
    }
    _write_json(output_root / "phase3ca_bz_candidate_audit_summary.json", summary)
    (output_root / "PHASE3CA_BZ_CANDIDATE_AUDIT_20260616.md").write_text(
        "\n".join(
            [
                "# Phase3CA BZ Candidate Audit 2026-06-16",
                "",
                f"Decision: `{summary['decision']}`",
                "",
                f"- selected candidates: `{summary['candidate_count']}`",
                f"- deduped source candidates: `{summary['deduped_source_candidate_count']}`",
                "- boundary: proxy ranking only; BZ fragment replay is mandatory",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", action="append", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--top-n", type=int, default=128)
    parser.add_argument("--allow-high-corr", action="store_true", help="Diagnostic override: allow signal-crowded rows into the BZ bridge.")
    args = parser.parse_args(argv)

    summary = build_candidate_table(args.source_root, _resolve(args.output_root), args.top_n, allow_high_corr=bool(args.allow_high_corr))
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
