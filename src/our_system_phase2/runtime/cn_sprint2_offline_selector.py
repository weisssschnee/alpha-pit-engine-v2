"""Build Sprint-2 frozen strict evidence and compare auditable pre-strict selectors."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.services.strict_priority_selector import (
    apply_development_eligibility,
    current_scalar_score,
    selector_comparison,
)


EXPERIMENT_ID = "20260713_cn_sprint2_offline_selector_001"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(descriptor)
    temporary = Path(name)
    try:
        frame.to_csv(temporary, index=False)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes"}


def _source_rows(source: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    root = Path(str(source["root"]))
    proposal_path = root / "candidate_proposals.csv"
    strict_path = root / "strict_metrics.csv"
    for path, hash_key, size_key in (
        (proposal_path, "candidate_proposals_sha256", "candidate_proposals_size"),
        (strict_path, "strict_metrics_sha256", "strict_metrics_size"),
    ):
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(source[size_key]) or _sha256(path) != str(source[hash_key]):
            raise ValueError(f"frozen evidence identity mismatch: {path}")
    proposals = pd.read_csv(proposal_path, low_memory=False)
    strict = pd.read_csv(strict_path, low_memory=False)
    return proposals, strict, {
        "id": source["id"],
        "candidate_proposals": {"path": str(proposal_path), "size": proposal_path.stat().st_size, "sha256": _sha256(proposal_path)},
        "strict_metrics": {"path": str(strict_path), "size": strict_path.stat().st_size, "sha256": _sha256(strict_path)},
    }


def _strict_evidence(
    proposals: pd.DataFrame,
    strict: pd.DataFrame,
    *,
    run_id: str,
    seed_set: str,
) -> pd.DataFrame:
    strict = strict.copy()
    strict["horizon_bars"] = pd.to_numeric(strict["horizon_bars"], errors="coerce")
    time_block_available = {
        "proxy_worst_time_block_abs_ic", "proxy_time_block_stability"
    }.issubset(strict.columns)
    for column in (
        "ic_mean", "ic_abs_mean", "ic_standard_error", "mean_one_way_turnover",
    ):
        if column not in strict:
            strict[column] = np.nan
        strict[column] = pd.to_numeric(strict[column], errors="coerce")
    if "ic_abs_lcb95" not in strict:
        strict["ic_abs_lcb95"] = (
            strict["ic_abs_mean"] - 1.96 * strict["ic_standard_error"].fillna(0.0)
        ).clip(lower=0.0)
    else:
        strict["ic_abs_lcb95"] = pd.to_numeric(strict["ic_abs_lcb95"], errors="coerce")
    if "cost_adjusted_abs_ic" not in strict:
        strict["cost_adjusted_abs_ic"] = (
            strict["ic_abs_mean"] - 0.0025 * strict["mean_one_way_turnover"].fillna(0.0)
        )
    else:
        strict["cost_adjusted_abs_ic"] = pd.to_numeric(
            strict["cost_adjusted_abs_ic"], errors="coerce"
        )
    for column in ("proxy_worst_time_block_abs_ic", "proxy_time_block_stability"):
        if column not in strict:
            strict[column] = np.nan
        strict[column] = pd.to_numeric(strict[column], errors="coerce")
    h5 = strict[strict["horizon_bars"].eq(5)].copy()
    if h5.empty:
        raise ValueError(f"frozen strict evidence lacks H5 rows: {run_id}")
    benchmark = h5[h5["lane_id"].eq("benchmark_competitor")]["cost_adjusted_abs_ic"]
    benchmark_median = float(benchmark.median()) if len(benchmark) else 0.0
    aggregate = strict.groupby("candidate_id", sort=False).agg(
        strict_min_lcb=("ic_abs_lcb95", "min"),
        strict_min_cost_adjusted=("cost_adjusted_abs_ic", "min"),
        strict_max_turnover=("mean_one_way_turnover", "max"),
        strict_horizon_count=("horizon_bars", "nunique"),
        strict_signed_min=("ic_mean", "min"),
        strict_signed_max=("ic_mean", "max"),
    ).reset_index()
    evidence = h5.merge(aggregate, on="candidate_id", how="left", validate="one_to_one")
    proposal_columns = [column for column in proposals.columns if column not in evidence.columns or column == "candidate_id"]
    evidence = evidence.merge(
        proposals[proposal_columns], on="candidate_id", how="left", validate="one_to_one"
    )
    same_sign = evidence["strict_signed_min"].mul(evidence["strict_signed_max"]).gt(0)
    block_ok = (
        evidence["proxy_worst_time_block_abs_ic"].fillna(0).gt(0)
        & evidence["proxy_time_block_stability"].fillna(0).ge(0.5)
        if time_block_available
        else pd.Series(True, index=evidence.index)
    )
    evidence["strict_benchmark_increment"] = evidence["cost_adjusted_abs_ic"] - benchmark_median
    evidence["strict_stable"] = (
        evidence["strict_min_lcb"].gt(0)
        & evidence["strict_min_cost_adjusted"].gt(0)
        & evidence["strict_max_turnover"].le(0.75)
        & evidence["strict_horizon_count"].ge(4)
        & block_ok
        & same_sign
    )
    evidence["strict_priority_hit"] = (
        evidence["strict_benchmark_increment"].gt(0) & evidence["strict_stable"]
    ).astype(int)
    evidence["positive_benchmark_increment"] = evidence["strict_benchmark_increment"].gt(0).astype(int)
    evidence["positive_cost_adjusted_evidence"] = evidence["cost_adjusted_abs_ic"].gt(0).astype(int)
    evidence["strict_pareto_feasible"] = (
        evidence["strict_benchmark_increment"].ge(0)
        & evidence["strict_min_lcb"].gt(0)
        & evidence["strict_max_turnover"].le(0.75)
    ).astype(int)
    evidence["run_id"] = run_id
    evidence["seed_set"] = seed_set
    evidence["benchmark_h5_cost_adjusted_median"] = benchmark_median
    evidence["time_block_evidence_available"] = bool(time_block_available)
    evidence["strict_schema_version"] = (
        "strict_v2_lcb_cost_time_blocks" if time_block_available else "strict_v1_derived_lcb_cost_no_time_blocks"
    )
    primitive = (
        evidence["primitive_family"].fillna("unknown").astype(str)
        if "primitive_family" in evidence
        else pd.Series("unknown", index=evidence.index, dtype=object)
    )
    family_fallback = evidence["lane_id"].astype(str) + ":" + primitive
    if "family_id" in evidence:
        evidence["family_id"] = evidence["family_id"].fillna(family_fallback).astype(str)
    else:
        evidence["family_id"] = family_fallback
    return evidence


def _eligibility_audit(
    source_frames: list[tuple[str, pd.DataFrame]],
    evidence: pd.DataFrame,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    source_reports = []
    all_rows: list[dict[str, Any]] = []
    for run_id, frame in source_frames:
        records = frame.to_dict("records")
        for row in records:
            row["legal"] = _boolean(row.get("legal"))
            row["materialized"] = _boolean(row.get("materialized"))
            row["survivor"] = _boolean(row.get("survivor"))
        audited = apply_development_eligibility(records, contract)
        all_rows.extend(audited)
        failures: dict[str, int] = {}
        for row in audited:
            for reason in row["development_eligibility_reasons"]:
                failures[reason] = failures.get(reason, 0) + 1
        lanes = []
        for lane in sorted({str(row.get("lane_id")) for row in audited}):
            rows = [row for row in audited if str(row.get("lane_id")) == lane]
            eligible = sum(bool(row["development_eligible"]) for row in rows)
            lanes.append({"lane_id": lane, "proposal_count": len(rows), "development_eligible_count": eligible, "rate": eligible / max(1, len(rows))})
        source_reports.append(
            {
                "run_id": run_id,
                "proposal_count": len(audited),
                "development_eligible_count": sum(bool(row["development_eligible"]) for row in audited),
                "legacy_survivor_disagreement_count": sum(
                    bool(row["development_eligible"]) != bool(row["legacy_survivor"]) for row in audited
                ),
                "gate_failure_counts": failures,
                "lane_rates": lanes,
            }
        )
    scores = current_scalar_score(evidence.to_dict("records"))
    labels = evidence["strict_priority_hit"].to_numpy(dtype=int)
    order = np.argsort(scores, kind="stable")
    decile = max(1, int(np.ceil(len(labels) * 0.10)))
    return {
        "renamed_layer": "DEVELOPMENT_ELIGIBLE",
        "interpretation": "legality and evaluability only; not a quality survivor",
        "sources": source_reports,
        "strict_relationship": {
            "strict_evidence_count": int(len(evidence)),
            "top_scalar_decile_hit_rate": float(labels[order[-decile:]].mean()),
            "bottom_scalar_decile_hit_rate": float(labels[order[:decile]].mean()),
            "overall_hit_rate": float(labels.mean()),
        },
        "gate_contract": dict(contract),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    repo = args.repo.resolve()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if manifest["status"] != "CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_ACTIVE":
        raise ValueError("Sprint-2 manifest is not active")
    boundaries = manifest["immutable_boundaries"]
    if any(
        boundaries[key]
        for key in (
            "new_data_fields_allowed", "plate_industry_enabled", "validation_allowed",
            "holdout_allowed", "forward_2026_allowed", "candidate_promotion_allowed",
            "permanent_memory_allowed", "cross_sprint_elite_value_allowed",
        )
    ):
        raise ValueError("Sprint-2 immutable boundary drift")
    started = datetime.now(timezone.utc)
    inputs = []
    evidence_frames = []
    proposal_frames: list[tuple[str, pd.DataFrame]] = []
    for source in manifest["offline_evidence"]["sources"]:
        proposals, strict, identity = _source_rows(source)
        inputs.append(identity)
        proposal_frames.append((str(source["id"]), proposals))
        evidence_frames.append(
            _strict_evidence(
                proposals,
                strict,
                run_id=str(source["id"]),
                seed_set=str(source["seed_set"]),
            )
        )
    evidence = pd.concat(evidence_frames, ignore_index=True, sort=False)
    labels = evidence["strict_priority_hit"].astype(int).tolist()
    rows = evidence.to_dict("records")
    comparison = selector_comparison(rows, labels)
    eligibility_contract = {
        "minimum_finite_ratio": 0.25,
        "minimum_signal_unique": 8,
        "minimum_eligible_cross_sections": 40,
        "minimum_proxy_reward": 0.0,
    }
    eligibility = _eligibility_audit(proposal_frames, evidence, eligibility_contract)
    output = args.output_root.resolve()
    evidence_path = output / "offline_strict_evidence_v1.csv"
    selector_path = output / "selector_oof_report_v1.json"
    model_path = output / "strict_priority_model_v1.json"
    eligibility_path = output / "development_eligible_audit_v1.json"
    manifest_path = output / "run_manifest.json"
    _atomic_csv(evidence_path, evidence)
    _atomic_json(selector_path, {key: value for key, value in comparison.items() if key != "full_model"})
    _atomic_json(model_path, comparison["full_model"])
    _atomic_json(eligibility_path, eligibility)
    outputs = [evidence_path, selector_path, model_path, eligibility_path]
    record = {
        "experiment_id": EXPERIMENT_ID,
        "objective": "test whether auditable pre-strict selectors enrich frozen strict development hits",
        "status": "COMPLETED",
        "mode": "offline_frozen_development_evidence",
        "started_at": started.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "repo_sha": manifest["baseline"]["repo_sha"],
        "inputs": inputs,
        "parameters": {
            "target_horizon_bars": manifest["offline_evidence"]["target_horizon_bars"],
            "cross_fit_protocols": manifest["offline_evidence"]["cross_fit_protocols"],
            "strict_evidence_count": len(evidence),
        },
        "commands": [
            f"python -m our_system_phase2.runtime.cn_sprint2_offline_selector --manifest {args.manifest} --output-root {output}"
        ],
        "outputs": [
            {"path": str(path), "size": path.stat().st_size, "sha256": _sha256(path), "purpose": path.stem, "state": "final"}
            for path in outputs
        ],
        "reproducibility": "YES_FOR_FROZEN_INPUTS",
        "continuation": "selector may enter Repair CANARY only if OOF gate passed; never persist as cross-sprint memory",
        "failure": None,
        "decision": "STRICT_PRIORITY_SELECTOR_READY" if comparison["gate"]["passed"] else "KEEP_INTERPRETABLE_RULE_SELECTOR",
        "boundaries": {
            "validation_rows_read": 0,
            "holdout_rows_read": 0,
            "forward_rows_read": 0,
            "candidate_promotion_made": False,
            "cross_sprint_memory_written": False,
        },
    }
    _atomic_json(manifest_path, record)
    print(json.dumps({"status": record["status"], "decision": record["decision"], "output_root": str(output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
