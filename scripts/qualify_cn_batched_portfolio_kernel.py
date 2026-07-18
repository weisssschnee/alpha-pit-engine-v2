from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence


RESULT_FILE = "CN_STREAMING_BACKEND_RESULT.json"
EXACT_ARTIFACTS = (
    "CN_FROZEN_EXECUTION_PLAN.json",
    "CN_SHARED_DAG_PLAN.json",
    "CN_STREAMING_REWARD_ATOMS.csv",
    "CN_STREAMING_REDUCER_CONTRACT.json",
)
COMPARABILITY_RESULT_FIELDS = (
    "backend",
    "phase",
    "input_binding_hash",
    "split_manifest_hash",
    "execution_plan_hash",
    "dag_plan_hash",
)
SEMANTIC_RESULT_FIELDS = (
    "schema_version",
    "status",
    "backend",
    "block_row_guard_status",
    "blocks_processed",
    "candidate_count",
    "candidate_rewards",
    "coordinate_rows_retained",
    "dag_plan_hash",
    "eligible_train_date_count",
    "expression_audits",
    "max_block_rows_contract",
    "max_observed_block_rows",
    "validation_reads",
    "holdout_reads",
    "forward_2026_reads",
    "input_binding_hash",
    "pair_count",
    "pair_results",
    "promotion",
    "reward_atoms",
    "rows_processed",
    "split_manifest_hash",
    "split_rows",
    "strict_stage_a",
    "support_identities",
)
PAIR_IDENTITY_FIELDS = (
    "pair_id",
    "primary_candidate_id",
    "control_candidate_id",
    "primary_receipt_hash",
    "control_receipt_hash",
    "pair_receipt_hash",
)
COMPUTE_PHASES = (
    "expression_value_dag",
    "cross_sectional_rank_mapping",
    "turnover_and_cost",
)
EXPRESSION_AUDIT_TELEMETRY_FIELDS = {
    "cache_current_bytes",
    "cache_peak_bytes",
    "cache_entry_count",
}
ACCESS_COUNT_FIELDS = (
    "validation_reads",
    "holdout_reads",
    "forward_2026_reads",
)
NON_TRAIN_COUNT_FIELDS = (
    "curve_count",
    "day_count",
    "rank_ic_obs",
    "day_mcmc_iterations",
)
NON_TRAIN_METRIC_FIELDS = (
    "day_mcmc_prob_sortino_gt_0",
    "day_mcmc_sortino_median",
    "day_mcmc_sortino_p25",
    "day_sortino",
    "max_drawdown",
    "mean_one_way_turnover",
    "minute_sortino",
    "net_hit_rate",
    "net_mean_return",
    "rank_ic_hit_rate",
    "rank_ic_loss",
    "rank_ic_mean",
    "raw_mean_return",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require_sha256(value: Any, label: str) -> str:
    normalized = str(value or "").strip().lower()
    if len(normalized) != 64 or any(
        character not in "0123456789abcdef" for character in normalized
    ):
        raise ValueError(f"{label} must be a 64-character SHA-256")
    return normalized


def _require_zero(value: Mapping[str, Any], field: str, label: str) -> None:
    if field not in value or value[field] != 0:
        raise ValueError(f"{label} does not prove {field}=0")


def _validate_result_access_contract(
    result: Mapping[str, Any], label: str
) -> dict[str, Any]:
    for field in ACCESS_COUNT_FIELDS:
        _require_zero(result, field, label)
    if result.get("promotion") != "FORBIDDEN":
        raise ValueError(f"{label} does not forbid candidate promotion")
    if result.get("strict_stage_a") != "NOT_AUTHORIZED":
        raise ValueError(f"{label} does not keep strict Stage A unauthorized")

    split_manifest_hash = _require_sha256(
        result.get("split_manifest_hash"), f"{label} split_manifest_hash"
    )
    declared_role = result.get("data_role")
    if declared_role is not None and str(declared_role) not in {
        "development",
        "development_train_only",
    }:
        raise ValueError(f"{label} has non-development data_role={declared_role!r}")
    try:
        eligible_train_dates = int(result.get("eligible_train_date_count"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} is missing eligible_train_date_count") from exc
    if eligible_train_dates <= 0:
        raise ValueError(f"{label} has no eligible development/train dates")

    split_rows = result.get("split_rows")
    if not isinstance(split_rows, list) or not split_rows:
        raise ValueError(f"{label} has no split_rows evidence")
    train_rows = 0
    non_train_rows = 0
    for index, raw in enumerate(split_rows):
        if not isinstance(raw, Mapping):
            raise ValueError(f"{label} split_rows[{index}] is not an object")
        split = str(raw.get("split") or "").strip().lower()
        if split == "train":
            train_rows += 1
            continue
        if split not in {"validation", "holdout"}:
            raise ValueError(f"{label} split_rows[{index}] has forbidden split={split!r}")
        non_train_rows += 1
        for field in NON_TRAIN_COUNT_FIELDS:
            if raw.get(field) not in (None, 0, 0.0):
                raise ValueError(
                    f"{label} split_rows[{index}] contains nonzero {split} {field}"
                )
        for field in NON_TRAIN_METRIC_FIELDS:
            if raw.get(field) is not None:
                raise ValueError(
                    f"{label} split_rows[{index}] contains populated {split} {field}"
                )
    if train_rows == 0:
        raise ValueError(f"{label} has no development/train split rows")
    return {
        "status": "VERIFIED",
        "data_role": "development_train_only",
        "data_role_verification": (
            "declared_and_verified" if declared_role is not None else "inferred_from_result"
        ),
        "split_manifest_hash": split_manifest_hash,
        "eligible_train_date_count": eligible_train_dates,
        "train_split_rows": train_rows,
        "empty_non_train_split_rows": non_train_rows,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }


def _normalize_for_digest(value: Any) -> Any:
    """Make JSON's non-standard NaN/Infinity values stable and comparable."""
    if isinstance(value, float) and not math.isfinite(value):
        if math.isnan(value):
            label = "nan"
        elif value > 0:
            label = "positive_infinity"
        else:
            label = "negative_infinity"
        return {"__non_finite_float__": label}
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_for_digest(item)
            for key, item in sorted(value.items(), key=lambda row: str(row[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_for_digest(item) for item in value]
    return value


def _stable_digest(value: Any) -> str:
    payload = json.dumps(
        _normalize_for_digest(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _same(left: Any, right: Any) -> bool:
    return _stable_digest(left) == _stable_digest(right)


def _semantic_result_value(result: Mapping[str, Any], field: str) -> Any:
    value = result.get(field)
    if field != "expression_audits":
        return value
    normalized = []
    for raw in value or []:
        row = {
            key: item
            for key, item in raw.items()
            if key not in EXPRESSION_AUDIT_TELEMETRY_FIELDS
            and not key.endswith("_wall_seconds")
            and not key.endswith("_cpu_seconds")
            and not key.endswith("_effective_cores")
        }
        normalized.append(row)
    return normalized


def _pair_identity(result: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for pair in result.get("pair_results") or []:
        rows.append({field: pair.get(field) for field in PAIR_IDENTITY_FIELDS})
    return sorted(rows, key=lambda row: tuple(str(row.get(field)) for field in PAIR_IDENTITY_FIELDS))


def _candidate_identity(result: Mapping[str, Any]) -> list[str]:
    return sorted(str(row.get("candidate_id")) for row in result.get("candidate_rewards") or [])


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0.0 else None


def _phase_value(result: Mapping[str, Any], phase: str, field: str) -> float | None:
    phases = result.get("phase_totals") or {}
    row = phases.get(phase) or {}
    return _finite_positive(row.get(field))


def _phase_sum(result: Mapping[str, Any], field: str) -> float | None:
    values = [_phase_value(result, phase, field) for phase in COMPUTE_PHASES]
    finite = [value for value in values if value is not None]
    return sum(finite) if finite else None


def _speedup(reference: float | None, candidate: float | None) -> float | None:
    if reference is None or candidate is None:
        return None
    return reference / candidate


def _artifact_hashes(root: Path) -> tuple[dict[str, str | None], list[str]]:
    hashes: dict[str, str | None] = {}
    missing: list[str] = []
    for name in EXACT_ARTIFACTS:
        path = root / name
        if not path.is_file():
            hashes[name] = None
            missing.append(name)
        else:
            hashes[name] = _sha256(path)
    return hashes, missing


def compare_kernel_runs(reference_root: Path, candidate_root: Path) -> dict[str, Any]:
    reference_root = reference_root.resolve()
    candidate_root = candidate_root.resolve()
    reference = _read_json(reference_root / RESULT_FILE)
    candidate = _read_json(candidate_root / RESULT_FILE)
    reference_access = _validate_result_access_contract(reference, "reference result")
    candidate_access = _validate_result_access_contract(candidate, "candidate result")
    if reference_access["split_manifest_hash"] != candidate_access["split_manifest_hash"]:
        raise ValueError("reference and candidate split_manifest_hash differ")
    reference_hashes, reference_missing = _artifact_hashes(reference_root)
    candidate_hashes, candidate_missing = _artifact_hashes(candidate_root)
    candidate_plan = (
        _read_json(candidate_root / "CN_FROZEN_EXECUTION_PLAN.json")
        if candidate_hashes["CN_FROZEN_EXECUTION_PLAN.json"] is not None
        else {}
    )

    comparability: dict[str, bool] = {
        f"result:{field}": _same(reference.get(field), candidate.get(field))
        for field in COMPARABILITY_RESULT_FIELDS
    }
    comparability["candidate_identity"] = _same(
        _candidate_identity(reference), _candidate_identity(candidate)
    )
    comparability["pair_identity"] = _same(
        _pair_identity(reference), _pair_identity(candidate)
    )
    for artifact in ("CN_FROZEN_EXECUTION_PLAN.json", "CN_SHARED_DAG_PLAN.json"):
        comparability[f"artifact:{artifact}"] = (
            reference_hashes[artifact] is not None
            and reference_hashes[artifact] == candidate_hashes[artifact]
        )
    comparability["required_artifacts_present"] = not (
        reference_missing or candidate_missing
    )
    comparable = all(comparability.values())

    semantic_parity: dict[str, bool] = {
        f"result:{field}": _same(
            _semantic_result_value(reference, field),
            _semantic_result_value(candidate, field),
        )
        for field in SEMANTIC_RESULT_FIELDS
    }
    for artifact in EXACT_ARTIFACTS:
        semantic_parity[f"artifact:{artifact}"] = (
            reference_hashes[artifact] is not None
            and reference_hashes[artifact] == candidate_hashes[artifact]
        )
    parity_exact = all(semantic_parity.values())

    reference_wall = _finite_positive(reference.get("wall_seconds"))
    candidate_wall = _finite_positive(candidate.get("wall_seconds"))
    reference_mapping_wall = _phase_value(
        reference, "cross_sectional_rank_mapping", "wall_seconds"
    )
    candidate_mapping_wall = _phase_value(
        candidate, "cross_sectional_rank_mapping", "wall_seconds"
    )
    reference_compute_wall = _phase_sum(reference, "wall_seconds")
    candidate_compute_wall = _phase_sum(candidate, "wall_seconds")
    candidate_mapping_cpu = _phase_value(
        candidate, "cross_sectional_rank_mapping", "cpu_seconds"
    )
    mapping_effective_cores = (
        candidate_mapping_cpu / candidate_mapping_wall
        if candidate_mapping_cpu is not None and candidate_mapping_wall is not None
        else None
    )
    allocated_compute_threads = int(candidate_plan.get("compute_threads") or 0)
    minimum_effective_cores = 0.5 * allocated_compute_threads
    mapping_parallelism_engaged = (
        allocated_compute_threads > 0
        and mapping_effective_cores is not None
        and mapping_effective_cores >= minimum_effective_cores
        and candidate.get("parallelism_status") == "PARALLELISM_ENGAGED"
    )
    wall_speedup = _speedup(reference_wall, candidate_wall)
    reference_peak_rss = _finite_positive(reference.get("peak_rss_bytes"))
    candidate_peak_rss = _finite_positive(candidate.get("peak_rss_bytes"))

    if not comparable:
        status = "CN_BATCHED_PORTFOLIO_KERNEL_SUBSET_NOT_COMPARABLE"
    elif not parity_exact:
        status = "CN_BATCHED_PORTFOLIO_KERNEL_PARITY_FAILED"
    elif (
        wall_speedup is not None
        and wall_speedup >= 2.0
        and mapping_parallelism_engaged
    ):
        status = "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"
    elif (
        wall_speedup is not None
        and wall_speedup >= 1.3
        and mapping_parallelism_engaged
    ):
        status = "CN_BATCHED_PORTFOLIO_KERNEL_PARTIALLY_QUALIFIED"
    else:
        status = "CN_BATCHED_PORTFOLIO_KERNEL_NOT_QUALIFIED"

    return {
        "schema_version": "cn_batched_portfolio_kernel_subset_qualification_v1",
        "status": status,
        "reference_root": str(reference_root),
        "candidate_root": str(candidate_root),
        "comparability": comparability,
        "comparable": comparable,
        "semantic_parity": semantic_parity,
        "semantic_parity_exact": parity_exact,
        "reference_artifact_hashes": reference_hashes,
        "candidate_artifact_hashes": candidate_hashes,
        "missing_artifacts": {
            "reference": reference_missing,
            "candidate": candidate_missing,
        },
        "performance": {
            "reference_wall_seconds": reference_wall,
            "candidate_wall_seconds": candidate_wall,
            "wall_speedup": wall_speedup,
            "reference_mapping_wall_seconds": reference_mapping_wall,
            "candidate_mapping_wall_seconds": candidate_mapping_wall,
            "mapping_speedup": _speedup(
                reference_mapping_wall, candidate_mapping_wall
            ),
            "reference_compute_wall_seconds": reference_compute_wall,
            "candidate_compute_wall_seconds": candidate_compute_wall,
            "compute_speedup": _speedup(
                reference_compute_wall, candidate_compute_wall
            ),
            "candidate_mapping_effective_cores": mapping_effective_cores,
            "allocated_compute_threads": allocated_compute_threads,
            "minimum_effective_cores": minimum_effective_cores,
            "mapping_parallelism_engaged": mapping_parallelism_engaged,
            "reference_peak_rss_bytes": reference_peak_rss,
            "candidate_peak_rss_bytes": candidate_peak_rss,
            "peak_rss_delta_bytes": (
                candidate_peak_rss - reference_peak_rss
                if reference_peak_rss is not None and candidate_peak_rss is not None
                else None
            ),
        },
        "thresholds": {
            "qualified_wall_speedup_gte": 2.0,
            "partially_qualified_wall_speedup_gte": 1.3,
        },
        "next_decision": (
            "RUN_146_ACTIVE_BAR_REPLAY"
            if status == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED"
            else "STOP_BEFORE_146_ACTIVE_BAR_REPLAY"
        ),
        "data_role": "development_train_only",
        "split_manifest_hash": reference_access["split_manifest_hash"],
        "result_access_contracts": {
            "reference": reference_access,
            "candidate": candidate_access,
        },
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "strict_stage_a": "NOT_AUTHORIZED",
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    result = compare_kernel_runs(args.reference_root, args.candidate_root)
    _write_json(args.output.resolve(), result)
    print(
        json.dumps(
            {
                "status": result["status"],
                "comparable": result["comparable"],
                "semantic_parity_exact": result["semantic_parity_exact"],
                "wall_speedup": result["performance"]["wall_speedup"],
            },
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
    )
    return 0 if result["status"] == "CN_BATCHED_PORTFOLIO_KERNEL_QUALIFIED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
