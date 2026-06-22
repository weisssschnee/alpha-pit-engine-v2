"""Build Phase3CN search feedback memory from Phase3CM reward outputs.

This is the first wiring step after Phase3CM: turn train portfolio Sortino
reward rows into arm/family feedback tables that future BS/BT/BU/CEM/UCB
searchers can consume. It does not run search.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from our_system_phase2.services.candidate_schema import (
    CANONICAL_CANDIDATE_FIELDS,
    normalize_candidate_schema,
    safe_float,
)


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cn_feedback_memory_smoke_20260623")
DEFAULT_REPORT_ROOT = Path("reports/phase3cn_feedback_memory_smoke_20260623")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fieldnames is None:
        fieldnames = []
        seen: set[str] = set()
        for row in rows:
            for key in row:
                if key not in seen:
                    fieldnames.append(key)
                    seen.add(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _round(value: Any, ndigits: int = 8) -> float | None:
    val = safe_float(value)
    if not math.isfinite(val):
        return None
    return round(val, ndigits)


def _median(values: list[float]) -> float | None:
    clean = [value for value in values if math.isfinite(value)]
    if not clean:
        return None
    return float(statistics.median(clean))


def _discover_cm_tables(paths: list[Path], roots: list[Path]) -> list[Path]:
    out: list[Path] = []
    for path in paths:
        resolved = _resolve(path)
        if resolved.exists():
            out.append(resolved)
    for root in roots:
        resolved_root = _resolve(root)
        if not resolved_root.exists():
            continue
        for name in ("phase3cm_train_reward.csv", "phase3cm_candidate_train_reward_summary.csv"):
            out.extend(sorted(resolved_root.rglob(name)))
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in out:
        key = str(path).lower()
        if key not in seen:
            seen.add(key)
            deduped.append(path)
    return deduped


def _load_rows(tables: list[Path]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    seen: set[str] = set()
    for table in tables:
        raw_rows = _read_csv(table)
        sources.append({"path": str(table), "rows": len(raw_rows)})
        for raw in raw_rows:
            normalized = dict(raw)
            normalized.update(normalize_candidate_schema(normalized))
            digest = str(normalized.get("expression_hash") or "")
            if not digest or digest in seen:
                continue
            seen.add(digest)
            rows.append(normalized)
    return rows, sources


def _has_wrong_lag_or_corr(row: dict[str, Any]) -> bool:
    text = "|".join(
        str(row.get(name) or "")
        for name in ("blocker_flags", "train_reward_blockers", "inherited_blockers")
    ).lower()
    return "wrong_lag" in text or "future_signal_wrong_lag" in text or "high_corr" in text or "signal_corr_abs" in text


def _is_clean(row: dict[str, Any], *, train_threshold: float, validation_floor: float, max_turnover: float) -> bool:
    train_reward = safe_float(row.get("train_reward"))
    validation = safe_float(row.get("validation_day_sortino"))
    turnover = safe_float(row.get("train_mean_one_way_turnover") or row.get("mean_one_way_turnover"), 0.0)
    decision = str(row.get("train_reward_decision") or "")
    blockers = str(row.get("train_reward_blockers") or "")
    if not math.isfinite(train_reward) or train_reward <= train_threshold:
        return False
    if math.isfinite(validation) and validation < validation_floor:
        return False
    if math.isfinite(turnover) and turnover > max_turnover:
        return False
    if blockers:
        return False
    if "FOLLOWUP_READY" not in decision and decision:
        return False
    if _has_wrong_lag_or_corr(row):
        return False
    return True


def _is_validation_survivor(row: dict[str, Any], *, validation_floor: float) -> bool:
    validation = safe_float(row.get("validation_day_sortino"))
    prob = safe_float(row.get("validation_mcmc_prob_gt_0"), float("nan"))
    if math.isfinite(validation) and validation >= validation_floor:
        return True
    if math.isfinite(prob) and prob >= 0.55:
        return True
    return False


def _is_rewardhack(row: dict[str, Any]) -> bool:
    proxy = safe_float(row.get("phase3ca_proxy_quality") or row.get("proxy_quality"), float("nan"))
    train = safe_float(row.get("train_reward"), float("nan"))
    return math.isfinite(proxy) and proxy > 0.0 and math.isfinite(train) and train <= 0.0


def _group_rows(rows: list[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get(key) or "unknown"), []).append(row)
    return groups


def _family_tables(
    rows: list[dict[str, Any]],
    *,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
    max_family_share: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    total = max(1, len(rows))
    family_rows: list[dict[str, Any]] = []
    blocked_rows: list[dict[str, Any]] = []
    exploit_rows: list[dict[str, Any]] = []
    for family_id, items in sorted(_group_rows(rows, "family_id").items()):
        train_rewards = [safe_float(row.get("train_reward")) for row in items]
        train_rewards = [value for value in train_rewards if math.isfinite(value)]
        clean_count = sum(1 for row in items if _is_clean(row, train_threshold=train_threshold, validation_floor=validation_floor, max_turnover=max_turnover))
        validation_survivor_count = sum(1 for row in items if _is_validation_survivor(row, validation_floor=validation_floor))
        rewardhack_count = sum(1 for row in items if _is_rewardhack(row))
        wrong_lag_or_corr_count = sum(1 for row in items if _has_wrong_lag_or_corr(row))
        high_turnover_count = sum(1 for row in items if safe_float(row.get("train_mean_one_way_turnover") or row.get("mean_one_way_turnover"), 0.0) > max_turnover)
        family_share = len(items) / total
        status = "normal"
        reasons: list[str] = []
        if family_share > max_family_share:
            status = "downweight"
            reasons.append("top_family_share_cap")
        if rewardhack_count > 0:
            status = "freeze"
            reasons.append("proxy_high_cm_negative")
        if wrong_lag_or_corr_count > 0:
            status = "block"
            reasons.append("wrong_lag_or_high_corr")
        if high_turnover_count > 0 and status != "block":
            status = "freeze"
            reasons.append("high_turnover")
        if clean_count > 0 and validation_survivor_count > 0 and status in {"normal", "downweight"}:
            status = "exploit_allowed"
            reasons.append("cm_positive_validation_survivor")
        exemplar = items[0]
        row = {
            "family_id": family_id,
            "motif_id": exemplar.get("motif_id"),
            "field_family": exemplar.get("field_family"),
            "primitive_family": exemplar.get("primitive_family"),
            "event_state_family": exemplar.get("event_state_family"),
            "horizon_bucket": exemplar.get("horizon_bucket"),
            "turnover_bucket": exemplar.get("turnover_bucket"),
            "candidate_count": len(items),
            "family_share": _round(family_share),
            "median_train_reward": _round(_median(train_rewards)),
            "positive_train_reward_count": sum(1 for value in train_rewards if value > train_threshold),
            "clean_count": clean_count,
            "validation_survivor_count": validation_survivor_count,
            "rewardhack_count": rewardhack_count,
            "wrong_lag_or_corr_count": wrong_lag_or_corr_count,
            "high_turnover_count": high_turnover_count,
            "family_status": status,
            "family_reasons": "|".join(reasons),
        }
        family_rows.append(row)
        if status in {"block", "freeze"}:
            blocked_rows.append(row)
        if status == "exploit_allowed":
            exploit_rows.append(row)
    family_rows.sort(key=lambda row: (str(row.get("family_status") == "exploit_allowed"), safe_float(row.get("median_train_reward"), -999.0)), reverse=True)
    return family_rows, blocked_rows, exploit_rows


def _arm_score_table(
    rows: list[dict[str, Any]],
    family_rows: list[dict[str, Any]],
    *,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
    min_clean_feedback: int,
) -> list[dict[str, Any]]:
    family_by_id = {str(row.get("family_id")): row for row in family_rows}
    out: list[dict[str, Any]] = []
    for arm, items in sorted(_group_rows(rows, "generator_arm").items()):
        train_rewards = [safe_float(row.get("train_reward")) for row in items]
        train_rewards = [value for value in train_rewards if math.isfinite(value)]
        clean_count = sum(1 for row in items if _is_clean(row, train_threshold=train_threshold, validation_floor=validation_floor, max_turnover=max_turnover))
        validation_count = sum(1 for row in items if _is_validation_survivor(row, validation_floor=validation_floor))
        rewardhack_count = sum(1 for row in items if _is_rewardhack(row))
        wrong_lag_count = sum(1 for row in items if _has_wrong_lag_or_corr(row))
        low_turnover_count = sum(1 for row in items if safe_float(row.get("train_mean_one_way_turnover") or row.get("mean_one_way_turnover"), 1.0) <= max_turnover)
        families = {str(row.get("family_id") or "") for row in items}
        allowed_families = sum(1 for family in families if family_by_id.get(family, {}).get("family_status") == "exploit_allowed")
        top_family_count = max((sum(1 for row in items if row.get("family_id") == family) for family in families), default=0)
        count = max(1, len(items))
        positive_rate = sum(1 for value in train_rewards if value > train_threshold) / count
        validation_rate = validation_count / count
        new_family_rate = len(families) / count
        low_turnover_rate = low_turnover_count / count
        rewardhack_rate = rewardhack_count / count
        wrong_lag_rate = wrong_lag_count / count
        top_family_share = top_family_count / count
        median_reward = _median(train_rewards) or 0.0
        arm_score = (
            positive_rate
            + median_reward
            + validation_rate
            + new_family_rate
            + low_turnover_rate
            - rewardhack_rate
            - wrong_lag_rate
            - top_family_share
        )
        update_allowed = clean_count >= min_clean_feedback
        out.append(
            {
                "generator_arm": arm,
                "candidate_count": len(items),
                "clean_feedback_count": clean_count,
                "min_clean_feedback": min_clean_feedback,
                "feedback_update_allowed": str(update_allowed).lower(),
                "positive_train_reward_rate": _round(positive_rate),
                "median_train_reward": _round(median_reward),
                "validation_survival_rate": _round(validation_rate),
                "new_family_rate": _round(new_family_rate),
                "low_turnover_rate": _round(low_turnover_rate),
                "rewardhack_family_rate": _round(rewardhack_rate),
                "wrong_lag_reject_rate": _round(wrong_lag_rate),
                "top_family_concentration": _round(top_family_share),
                "exploit_allowed_family_count": allowed_families,
                "arm_score": _round(arm_score),
            }
        )
    out.sort(key=lambda row: safe_float(row.get("arm_score"), -999.0), reverse=True)
    return out


def _render_md(summary: dict[str, Any], arm_rows: list[dict[str, Any]], family_rows: list[dict[str, Any]]) -> str:
    lines = [
        "# Phase3CN Feedback Memory Smoke 2026-06-23",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Scope",
        "",
        "Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.",
        "",
        "## Summary",
        "",
        f"- input tables: `{len(summary['sources'])}`",
        f"- candidates: `{summary['candidate_count']}`",
        f"- families: `{summary['family_count']}`",
        f"- exploit-allowed families: `{summary['exploit_allowed_family_count']}`",
        f"- blocked/frozen families: `{summary['blocked_family_count']}`",
        "",
        "## Arm Scores",
        "",
        "| arm | rows | clean | update | median reward | validation rate | wrong-lag/corr | rewardhack | top family | score |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in arm_rows:
        lines.append(
            f"| `{row.get('generator_arm')}` | {row.get('candidate_count')} | {row.get('clean_feedback_count')} | "
            f"`{row.get('feedback_update_allowed')}` | {row.get('median_train_reward')} | {row.get('validation_survival_rate')} | "
            f"{row.get('wrong_lag_reject_rate')} | {row.get('rewardhack_family_rate')} | {row.get('top_family_concentration')} | {row.get('arm_score')} |"
        )
    lines.extend(
        [
            "",
            "## Top Families",
            "",
            "| family | status | rows | median reward | clean | validation | reasons |",
            "|---|---|---:|---:|---:|---:|---|",
        ]
    )
    for row in family_rows[:30]:
        lines.append(
            f"| `{row.get('family_id')}` | `{row.get('family_status')}` | {row.get('candidate_count')} | "
            f"{row.get('median_train_reward')} | {row.get('clean_count')} | {row.get('validation_survivor_count')} | `{row.get('family_reasons')}` |"
        )
    lines.extend(
        [
            "",
            "## Boundary",
            "",
            "- Holdout fields are carried through as read-only metadata and are not used in arm_score.",
            "- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.",
            "- Proxy-high but CM-negative families are frozen or blocked before exploit.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_feedback_memory(
    *,
    cm_tables: list[Path],
    cm_roots: list[Path],
    output_root: Path,
    report_root: Path,
    train_threshold: float,
    validation_floor: float,
    max_turnover: float,
    max_family_share: float,
    min_clean_feedback: int,
) -> dict[str, Any]:
    tables = _discover_cm_tables(cm_tables, cm_roots)
    if not tables:
        raise RuntimeError("no Phase3CM reward tables found")
    rows, sources = _load_rows(tables)
    if not rows:
        raise RuntimeError("Phase3CM reward tables had no usable rows")
    feedback_rows = [{field: row.get(field, "") for field in CANONICAL_CANDIDATE_FIELDS} for row in rows]
    family_rows, blocked_rows, exploit_rows = _family_tables(
        feedback_rows,
        train_threshold=train_threshold,
        validation_floor=validation_floor,
        max_turnover=max_turnover,
        max_family_share=max_family_share,
    )
    arm_rows = _arm_score_table(
        feedback_rows,
        family_rows,
        train_threshold=train_threshold,
        validation_floor=validation_floor,
        max_turnover=max_turnover,
        min_clean_feedback=min_clean_feedback,
    )
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260623_phase3cn_feedback_memory_smoke",
        "decision": "PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY",
        "candidate_count": len(feedback_rows),
        "family_count": len(family_rows),
        "arm_count": len(arm_rows),
        "blocked_family_count": len(blocked_rows),
        "exploit_allowed_family_count": len(exploit_rows),
        "min_clean_feedback": min_clean_feedback,
        "train_threshold": train_threshold,
        "validation_floor": validation_floor,
        "max_turnover": max_turnover,
        "max_family_share": max_family_share,
        "sources": sources,
        "metric_boundary": "feedback memory only; no search generation; holdout is read-only and excluded from arm score",
    }
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "phase3cn_search_feedback_memory.csv", feedback_rows, CANONICAL_CANDIDATE_FIELDS)
    _write_csv(output_root / "phase3cn_arm_score_table.csv", arm_rows)
    _write_csv(output_root / "phase3cn_family_score_table.csv", family_rows)
    _write_csv(output_root / "phase3cn_blocked_family_table.csv", blocked_rows)
    _write_csv(output_root / "phase3cn_exploit_allowed_family_table.csv", exploit_rows)
    _write_json(output_root / "phase3cn_feedback_memory_summary.json", summary)
    _write_csv(report_root / "phase3cn_search_feedback_memory.csv", feedback_rows, CANONICAL_CANDIDATE_FIELDS)
    _write_csv(report_root / "phase3cn_arm_score_table.csv", arm_rows)
    _write_csv(report_root / "phase3cn_family_score_table.csv", family_rows)
    _write_csv(report_root / "phase3cn_blocked_family_table.csv", blocked_rows)
    _write_csv(report_root / "phase3cn_exploit_allowed_family_table.csv", exploit_rows)
    _write_json(report_root / "phase3cn_feedback_memory_summary.json", summary)
    (report_root / "PHASE3CN_FEEDBACK_MEMORY_SMOKE_20260623.md").write_text(
        _render_md(summary, arm_rows, family_rows),
        encoding="utf-8",
    )
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cm-table", action="append", type=Path, default=[])
    parser.add_argument("--cm-root", action="append", type=Path, default=[])
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--train-threshold", type=float, default=0.0)
    parser.add_argument("--validation-floor", type=float, default=0.0)
    parser.add_argument("--max-turnover", type=float, default=0.75)
    parser.add_argument("--max-family-share", type=float, default=0.25)
    parser.add_argument("--min-clean-feedback", type=int, default=8)
    args = parser.parse_args(argv)

    summary = build_feedback_memory(
        cm_tables=args.cm_table,
        cm_roots=args.cm_root,
        output_root=_resolve(args.output_root),
        report_root=_resolve(args.report_root),
        train_threshold=args.train_threshold,
        validation_floor=args.validation_floor,
        max_turnover=args.max_turnover,
        max_family_share=args.max_family_share,
        min_clean_feedback=args.min_clean_feedback,
    )
    print(json.dumps({"status": "ok", **summary}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
