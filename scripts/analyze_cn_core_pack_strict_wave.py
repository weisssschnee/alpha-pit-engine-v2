from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _mean(values: Iterable[float | None]) -> float | None:
    finite = [value for value in values if value is not None]
    return statistics.fmean(finite) if finite else None


def _median(values: Iterable[float | None]) -> float | None:
    finite = [value for value in values if value is not None]
    return statistics.median(finite) if finite else None


def _clean(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, Mapping):
        return {str(key): _clean(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value]
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(
        json.dumps(_clean(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    fieldnames = list(rows[0]) if rows else []
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        if fieldnames:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="raise")
            writer.writeheader()
            writer.writerows([_clean(dict(row)) for row in rows])
    temporary.replace(path)


def _route_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    evaluated = [row for row in rows if row["pair_evaluation_status"] == "PAIR_EVALUATED"]
    increments = [_finite(row.get("matched_train_increment")) for row in evaluated]
    rank_increments = [_finite(row.get("matched_rank_ic_increment")) for row in evaluated]
    positive = [value for value in increments if value is not None and value > 0.0]
    positive_rank = [value for value in rank_increments if value is not None and value > 0.0]
    blockers: Counter[str] = Counter()
    for row in rows:
        for blocker in str(row.get("pair_evaluation_blockers") or "").split("|"):
            if blocker:
                blockers[blocker] += 1
    return {
        "pair_count": len(rows),
        "evaluated_pair_count": len(evaluated),
        "blocked_pair_count": len(rows) - len(evaluated),
        "positive_matched_reward_count": len(positive),
        "positive_matched_reward_rate": len(positive) / max(1, len(evaluated)),
        "positive_matched_rank_ic_count": len(positive_rank),
        "positive_matched_rank_ic_rate": len(positive_rank) / max(1, len(evaluated)),
        "mean_matched_train_increment": _mean(increments),
        "median_matched_train_increment": _median(increments),
        "mean_matched_rank_ic_increment": _mean(rank_increments),
        "median_matched_rank_ic_increment": _median(rank_increments),
        "median_pair_support_count": _median(
            [_finite(row.get("pair_support_count")) for row in evaluated]
        ),
        "minimum_pair_support_overlap": min(
            (_finite(row.get("pair_support_overlap")) or 0.0 for row in evaluated),
            default=None,
        ),
        "blockers": dict(sorted(blockers.items())),
    }


def analyze_strict_wave(
    *,
    binding: Mapping[str, Any],
    backend_results: Sequence[tuple[str, Mapping[str, Any]]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    pair_contracts = {str(row["pair_id"]): row for row in binding.get("pairs") or []}
    member_contracts = {
        str(row["candidate_id"]): row for row in binding.get("candidate_members") or []
    }
    if len(pair_contracts) != int(binding.get("pair_count") or 0):
        raise ValueError("binding pair_count does not match unique pair rows")

    pair_rows: list[dict[str, Any]] = []
    backend_summaries: dict[str, Any] = {}
    observed_pair_ids: set[str] = set()
    binding_hash = str(binding.get("binding_hash") or "")

    for backend_name, result in backend_results:
        result_backend = str(result.get("backend") or backend_name)
        if str(result.get("input_binding_hash") or "") != binding_hash:
            raise ValueError(f"{backend_name}: input binding hash mismatch")
        rewards = {
            str(row["candidate_id"]): row for row in result.get("candidate_rewards") or []
        }
        backend_pair_ids: list[str] = []
        for row in result.get("pair_results") or []:
            pair_id = str(row["pair_id"])
            if pair_id in observed_pair_ids:
                raise ValueError(f"pair evaluated by more than one backend: {pair_id}")
            observed_pair_ids.add(pair_id)
            backend_pair_ids.append(pair_id)
            contract = pair_contracts.get(pair_id)
            if contract is None:
                raise ValueError(f"result contains pair absent from binding: {pair_id}")
            primary_id = str(row["primary_candidate_id"])
            control_id = str(row["control_candidate_id"])
            primary = rewards.get(primary_id, {})
            control = rewards.get(control_id, {})
            primary_rank_ic = _finite(primary.get("train_rank_ic_mean"))
            control_rank_ic = _finite(control.get("train_rank_ic_mean"))
            rank_increment = (
                primary_rank_ic - control_rank_ic
                if primary_rank_ic is not None and control_rank_ic is not None
                else None
            )
            primary_turnover = _finite(primary.get("train_mean_one_way_turnover"))
            control_turnover = _finite(control.get("train_mean_one_way_turnover"))
            pair_rows.append(
                {
                    "backend": result_backend,
                    "route_id": str(contract.get("route_id") or ""),
                    "pair_id": pair_id,
                    "primary_candidate_id": primary_id,
                    "control_candidate_id": control_id,
                    "pair_evaluation_status": str(row.get("pair_evaluation_status") or ""),
                    "pair_evaluation_blockers": str(row.get("pair_evaluation_blockers") or ""),
                    "primary_train_reward": _finite(row.get("primary_train_reward")),
                    "control_train_reward": _finite(row.get("control_train_reward")),
                    "matched_train_increment": _finite(row.get("pair_train_reward")),
                    "primary_train_rank_ic": primary_rank_ic,
                    "control_train_rank_ic": control_rank_ic,
                    "matched_rank_ic_increment": rank_increment,
                    "primary_turnover": primary_turnover,
                    "control_turnover": control_turnover,
                    "matched_turnover_increment": (
                        primary_turnover - control_turnover
                        if primary_turnover is not None and control_turnover is not None
                        else None
                    ),
                    "pair_support_count": int(row.get("pair_support_count") or 0),
                    "pair_support_overlap": _finite(row.get("pair_support_overlap")),
                    "clock_namespace": str(contract.get("clock_namespace") or ""),
                    "support_unit": str(contract.get("support_unit") or ""),
                    "primary_expression": str(contract.get("primary_expression") or ""),
                    "control_expression": str(contract.get("control_expression") or ""),
                    "primary_receipt_hash": str(
                        member_contracts.get(primary_id, {}).get("receipt_hash") or ""
                    ),
                    "control_receipt_hash": str(
                        member_contracts.get(control_id, {}).get("receipt_hash") or ""
                    ),
                }
            )

        backend_summaries[result_backend] = {
            "status": str(result.get("status") or ""),
            "pair_count": int(result.get("pair_count") or 0),
            "evaluated_pair_count": sum(
                1
                for row in result.get("pair_results") or []
                if str(row.get("pair_evaluation_status")) == "PAIR_EVALUATED"
            ),
            "blocked_pair_count": sum(
                1
                for row in result.get("pair_results") or []
                if str(row.get("pair_evaluation_status")) != "PAIR_EVALUATED"
            ),
            "wall_seconds": _finite(result.get("wall_seconds")),
            "peak_rss_bytes": int(result.get("peak_rss_bytes") or 0),
            "parallelism_status": str(result.get("parallelism_status") or ""),
            "hot_path_bottleneck": str(result.get("hot_path_bottleneck") or ""),
            "validation_reads": int(result.get("validation_reads") or 0),
            "holdout_reads": int(result.get("holdout_reads") or 0),
            "forward_2026_reads": int(result.get("forward_2026_reads") or 0),
            "pair_ids": sorted(backend_pair_ids),
        }

    missing_pair_ids = sorted(set(pair_contracts) - observed_pair_ids)
    unexpected_pair_ids = sorted(observed_pair_ids - set(pair_contracts))
    by_route_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pair_rows:
        by_route_rows[row["route_id"]].append(row)
    by_route = {
        route_id: _route_summary(rows)
        for route_id, rows in sorted(by_route_rows.items())
    }
    total_pairs = len(pair_rows)
    route_shares = {
        route_id: summary["pair_count"] / max(1, total_pairs)
        for route_id, summary in by_route.items()
    }
    access_reads = {
        role: sum(int(summary[role]) for summary in backend_summaries.values())
        for role in ("validation_reads", "holdout_reads", "forward_2026_reads")
    }
    pit_contract_complete = all(
        member.get("observable_time_contract")
        and member.get("pit_source_lag_contract")
        for member in binding.get("candidate_members") or []
    )
    infrastructure_gate = (
        not missing_pair_ids
        and not unexpected_pair_ids
        and all(
            summary["status"] == "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED"
            for summary in backend_summaries.values()
        )
    )
    resource_gate = all(
        summary["parallelism_status"] != "PARALLELISM_NOT_ENGAGED"
        for summary in backend_summaries.values()
    )
    overall = _route_summary(pair_rows)
    new_matched_mechanism_observed = overall["positive_matched_reward_count"] > 0
    summary = {
        "schema_version": "cn_core_pack_strict_wave_analysis_v1",
        "status": (
            "CN_CORE_PACK_STRICT_WAVE_ANALYZED"
            if infrastructure_gate and not any(access_reads.values()) and pit_contract_complete
            else "CN_CORE_PACK_STRICT_WAVE_ANALYSIS_FAILED_CLOSED"
        ),
        "binding_hash": binding_hash,
        "bound_pair_count": int(binding.get("pair_count") or 0),
        "observed_pair_count": total_pairs,
        "missing_pair_ids": missing_pair_ids,
        "unexpected_pair_ids": unexpected_pair_ids,
        "backend_summaries": backend_summaries,
        "overall": overall,
        "by_route": by_route,
        "route_pair_shares": route_shares,
        "maximum_route_pair_share": max(route_shares.values(), default=0.0),
        "access_reads": access_reads,
        "gates": {
            "no_access_or_pit_violation": not any(access_reads.values())
            and pit_contract_complete,
            "pit_contract_complete": pit_contract_complete,
            "no_infrastructure_failed_pairs": infrastructure_gate,
            "resource_gates_pass": resource_gate,
            "new_matched_mechanism_observed": new_matched_mechanism_observed,
            "route_dominance_evidence_only": {
                "maximum_route_pair_share": max(route_shares.values(), default=0.0),
                "automatic_threshold": None,
                "reason": "the frozen contract names a dominance gate but does not define a numeric cap",
            },
        },
        "research_boundaries": {
            "data_role": str(binding.get("data_role") or ""),
            "promotion": str(binding.get("promotion") or ""),
            "cross_sprint_memory": str(binding.get("cross_sprint_memory") or ""),
            "strict_stage_a": str(binding.get("strict_stage_a") or ""),
        },
    }
    return summary, sorted(pair_rows, key=lambda row: (row["route_id"], row["pair_id"]))


def _parse_backend_result(value: str) -> tuple[str, Path]:
    name, separator, raw_path = value.partition("=")
    if not separator or not name or not raw_path:
        raise argparse.ArgumentTypeError("backend result must use NAME=PATH")
    return name, Path(raw_path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path, required=True)
    parser.add_argument(
        "--backend-result",
        action="append",
        type=_parse_backend_result,
        required=True,
        help="repeatable NAME=PATH input",
    )
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    args = parser.parse_args()

    binding = _read_json(args.binding)
    results = [(name, _read_json(path)) for name, path in args.backend_result]
    summary, rows = analyze_strict_wave(binding=binding, backend_results=results)
    summary["artifacts"] = {
        "binding": {"path": str(args.binding), "sha256": _sha256(args.binding)},
        "backend_results": [
            {"backend": name, "path": str(path), "sha256": _sha256(path)}
            for name, path in args.backend_result
        ],
        "pair_csv": str(args.output_csv),
    }
    _write_csv(args.output_csv, rows)
    _write_json(args.output_json, summary)
    print(json.dumps({"status": summary["status"], "pair_count": len(rows)}))
    return 0 if summary["status"] == "CN_CORE_PACK_STRICT_WAVE_ANALYZED" else 2


if __name__ == "__main__":
    raise SystemExit(main())
