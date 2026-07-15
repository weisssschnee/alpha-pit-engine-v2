"""Cluster compositional sketches and perform label-free diversity admission."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import json
import math
import sys
import zlib
from collections import Counter, defaultdict
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_behavior_admission import (  # noqa: E402
    ADMISSION_VERSION,
    select_diversity_admission,
    stable_order_key,
)
from our_system_phase2.services.deterministic_signal_sketch import (  # noqa: E402
    cluster_distribution,
    cluster_sketches,
    sketch_similarity,
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fields.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _decode_f4(payload: str) -> np.ndarray:
    return np.frombuffer(
        zlib.decompress(base64.b64decode(payload)), dtype="<f4"
    ).copy()


def _load_workers(root: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    paths = sorted(root.glob("worker_*.csv"))
    if not paths:
        raise FileNotFoundError(f"no sketch worker CSVs under {root}")
    for path in paths:
        rows.extend(_read_csv(path))
    deduped: dict[tuple[str, str], dict[str, str]] = {}
    for row in rows:
        key = (str(row.get("candidate_id") or ""), str(row.get("coordinate_set") or ""))
        if key[0] and key[1] in {"A", "B"}:
            if key in deduped and deduped[key] != row:
                raise ValueError(f"conflicting sketch rows: {key}")
            deduped[key] = row
    return [deduped[key] for key in sorted(deduped)]


def _fidelity(
    rows: list[dict[str, str]], labels: Mapping[str, int], *, maximum_pairs: int
) -> dict[str, Any]:
    exact = sorted(
        [row for row in rows if row.get("exact_rank_vector") and row["candidate_id"] in labels],
        key=lambda row: row["candidate_id"],
    )
    pair_indices = list(combinations(range(len(exact)), 2))
    if len(pair_indices) > maximum_pairs:
        pair_indices = sorted(
            pair_indices,
            key=lambda pair: stable_order_key(
                {"left": exact[pair[0]]["candidate_id"], "right": exact[pair[1]]["candidate_id"]}
            ),
        )[:maximum_pairs]
    exact_corrs: list[float] = []
    sketch_corrs: list[float] = []
    same_cluster: list[bool] = []
    same_exact: list[bool] = []
    boundary_errors: list[bool] = []
    for left_index, right_index in pair_indices:
        left, right = exact[left_index], exact[right_index]
        lrank, rrank = _decode_f4(left["exact_rank_vector"]), _decode_f4(right["exact_rank_vector"])
        valid = np.isfinite(lrank) & np.isfinite(rrank)
        if int(valid.sum()) < 8 or np.std(lrank[valid]) <= 0 or np.std(rrank[valid]) <= 0:
            continue
        exact_corr = float(np.corrcoef(lrank[valid], rrank[valid])[0, 1])
        sketch_corr = float(sketch_similarity(left, right)["rank_corr"])
        if not math.isfinite(exact_corr) or not math.isfinite(sketch_corr):
            continue
        exact_corrs.append(exact_corr)
        sketch_corrs.append(sketch_corr)
        same_cluster.append(labels[left["candidate_id"]] == labels[right["candidate_id"]])
        exact_equivalent = abs(exact_corr) >= 0.95
        same_exact.append(exact_equivalent)
        if abs(abs(exact_corr) - 0.95) <= 0.02:
            boundary_errors.append((abs(sketch_corr) >= 0.95) != exact_equivalent)
    observed_same = [value for value, selected in zip(same_exact, same_cluster, strict=True) if selected]
    return {
        "sampled_candidate_count": len(exact),
        "evaluated_pair_count": len(exact_corrs),
        "sketch_exact_correlation": round(float(np.corrcoef(sketch_corrs, exact_corrs)[0, 1]), 8)
        if len(exact_corrs) >= 3
        else None,
        "cluster_pair_purity": round(float(np.mean(observed_same)), 8) if observed_same else 1.0,
        "boundary_pair_count": len(boundary_errors),
        "boundary_misclassification_rate": round(float(np.mean(boundary_errors)), 8)
        if boundary_errors
        else 0.0,
    }


def _clock_registry(
    *, clock: str, rows: list[dict[str, str]], expected_candidates: set[str]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    by_set = {
        name: {row["candidate_id"]: row for row in rows if row["coordinate_set"] == name}
        for name in ("A", "B")
    }
    for name in ("A", "B"):
        if set(by_set[name]) != expected_candidates:
            raise RuntimeError(
                f"{clock}/{name} sketch closure mismatch: expected {len(expected_candidates)}, observed {len(by_set[name])}"
            )
    coordinate_count = {name: int(next(iter(by_set[name].values()))["coordinate_count"]) for name in ("A", "B")}
    joint_min = max(128, int(math.ceil(0.05 * min(coordinate_count.values()))))
    eligible = {
        candidate_id
        for candidate_id in expected_candidates
        if min(int(by_set["A"][candidate_id]["finite_count"]), int(by_set["B"][candidate_id]["finite_count"]))
        >= joint_min
    }
    labels = {
        name: cluster_sketches([by_set[name][candidate_id] for candidate_id in sorted(eligible)])
        for name in ("A", "B")
    }
    pair_to_consensus: dict[tuple[int, int], int] = {}
    registry: list[dict[str, Any]] = []
    for candidate_id in sorted(expected_candidates):
        if candidate_id in eligible:
            pair = (labels["A"][candidate_id], labels["B"][candidate_id])
            consensus = pair_to_consensus.setdefault(pair, len(pair_to_consensus) + 1)
            exact_behavior = hashlib.sha256(
                (
                    by_set["A"][candidate_id]["exact_sketch_hash"]
                    + "|"
                    + by_set["B"][candidate_id]["exact_sketch_hash"]
                ).encode()
            ).hexdigest()[:24]
        else:
            pair = (0, 0)
            consensus = 0
            exact_behavior = ""
        registry.append(
            {
                "candidate_id": candidate_id,
                "clock_namespace": clock,
                "signal_coverage_status": "qualified" if candidate_id in eligible else "coverage_limited",
                "finite_count_a": int(by_set["A"][candidate_id]["finite_count"]),
                "finite_count_b": int(by_set["B"][candidate_id]["finite_count"]),
                "exact_behavior_identity": exact_behavior,
                "cluster_a": pair[0],
                "cluster_b": pair[1],
                "clock_cluster_id": consensus,
            }
        )
    a_labels = [row["cluster_a"] for row in registry if row["clock_cluster_id"] > 0]
    b_labels = [row["cluster_b"] for row in registry if row["clock_cluster_id"] > 0]
    dist_a, dist_b = cluster_distribution(a_labels), cluster_distribution(b_labels)
    summary = {
        "clock_namespace": clock,
        "candidate_count": len(expected_candidates),
        "joint_finite_min": joint_min,
        "coverage_qualified_count": len(eligible),
        "coverage_limited_count": len(expected_candidates) - len(eligible),
        "exact_behavior_identity_count": len(
            {row["exact_behavior_identity"] for row in registry if row["exact_behavior_identity"]}
        ),
        "consensus": cluster_distribution(
            row["clock_cluster_id"] for row in registry if row["clock_cluster_id"] > 0
        ),
        "ab_stability": {
            "adjusted_rand_index": round(float(adjusted_rand_score(a_labels, b_labels)), 8),
            "normalized_mutual_information": round(float(normalized_mutual_info_score(a_labels, b_labels)), 8),
            "top1_share_absolute_difference": round(abs(float(dist_a["top1_share"]) - float(dist_b["top1_share"])), 8),
        },
        "fidelity": {
            name: _fidelity(list(by_set[name].values()), labels[name], maximum_pairs=50_000)
            for name in ("A", "B")
        },
    }
    fidelity_rows = list(summary["fidelity"].values())
    summary["fidelity_gates"] = {
        "sketch_exact_correlation": all(
            row["sketch_exact_correlation"] is not None
            and float(row["sketch_exact_correlation"]) >= 0.90
            for row in fidelity_rows
        ),
        "cluster_pair_purity": all(float(row["cluster_pair_purity"]) >= 0.90 for row in fidelity_rows),
        "boundary_misclassification": all(
            float(row["boundary_misclassification_rate"]) <= 0.10 for row in fidelity_rows
        ),
        "coordinate_ari": float(summary["ab_stability"]["adjusted_rand_index"]) >= 0.80,
        "coordinate_nmi": float(summary["ab_stability"]["normalized_mutual_information"]) >= 0.80,
        "top1_stability": float(summary["ab_stability"]["top1_share_absolute_difference"]) <= 0.03,
    }
    summary["fidelity_gates"]["all_pass"] = all(summary["fidelity_gates"].values())
    return registry, summary


def _distribution(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    materialized = list(rows)
    labels = [int(row["behavior_cluster_id"]) for row in materialized if int(row["behavior_cluster_id"]) > 0]
    return {
        "candidate_count": len(materialized),
        "coverage_qualified_count": len(labels),
        "coverage_limited_count": len(materialized) - len(labels),
        "exact_behavior_identity_count": len(
            {row["exact_behavior_identity"] for row in materialized if row.get("exact_behavior_identity")}
        ),
        **cluster_distribution(labels),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--maximum-pairs", type=int, default=12_000)
    args = parser.parse_args()
    root = args.runtime_root.resolve()
    receipts = _read_jsonl(root / "CN_PAIR_RECEIPTS.jsonl")
    preadmission = json.loads((root / "CN_STRUCTURAL_PREADMISSION.json").read_text(encoding="utf-8"))
    selected = set(str(value) for value in preadmission["exact_identities"])
    receipts = [row for row in receipts if str(row["exact_identity"]) in selected]
    by_candidate = {str(row["candidate_id"]): row for row in receipts}
    active_generation = _read_csv(root / "signal_sketch/active_generation.csv")
    session_generation = _read_csv(root / "signal_sketch/session_generation.csv")
    active_registry, active_summary = _clock_registry(
        clock="active_bar", rows=_load_workers(root / "signal_sketch/active"),
        expected_candidates={row["candidate_id"] for row in active_generation},
    )
    session_registry, session_summary = _clock_registry(
        clock="stock_session", rows=_load_workers(root / "signal_sketch/session"),
        expected_candidates={row["candidate_id"] for row in session_generation},
    )
    registry_rows = [*active_registry, *session_registry]
    diagnostic = {
        "status": "SIGNAL_SKETCH_DIAGNOSTIC_COMPLETE_ADMISSION_GATE_PENDING",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "labels_or_returns_used": False,
        "validation_holdout_forward_read": False,
        "clock_summaries": {
            "active_bar": active_summary,
            "stock_session": session_summary,
        },
    }
    clock_offset = 0
    for clock in ("active_bar", "stock_session"):
        subset = [row for row in registry_rows if row["clock_namespace"] == clock]
        maximum = max((int(row["clock_cluster_id"]) for row in subset), default=0)
        for row in subset:
            row["behavior_cluster_id"] = (
                clock_offset + int(row["clock_cluster_id"]) if int(row["clock_cluster_id"]) > 0 else 0
            )
        clock_offset += maximum
    # Persist the registry only after the clock-local cluster identifiers have
    # been mapped into the globally unique behavior-cluster namespace.  The
    # diagnostic remains available even when the downstream admission gate
    # fails, but it must never expose ambiguous clock-local IDs as authority.
    _write_csv(root / "CN_SIGNAL_CLUSTER_REGISTRY.csv", registry_rows)
    _write_json(root / "CN_SIGNAL_SKETCH_DIAGNOSTIC.json", diagnostic)
    behavior_by_candidate = {row["candidate_id"]: row for row in registry_rows}
    admission_inputs: list[dict[str, Any]] = []
    for candidate_id, receipt in by_candidate.items():
        behavior = behavior_by_candidate[candidate_id]
        admission_inputs.append(
            {
                "candidate_id": candidate_id,
                "control_candidate_id": receipt["control_candidate_id"],
                "pair_id": receipt["pair_id"],
                "exact_identity": receipt["exact_identity"],
                "route_id": receipt["route_id"],
                "skeleton_id": receipt["skeleton_id"],
                "policy_id": receipt["policy_id"],
                "seed": int(receipt["seed"]),
                "source_family_signature": "|".join(sorted(receipt["source_families"])),
                "operator_path_hash": receipt["operator_path_hash"],
                "expression_depth": int(receipt["expression_depth"]),
                "clock_namespace": behavior["clock_namespace"],
                "signal_coverage_status": behavior["signal_coverage_status"],
                "exact_behavior_identity": behavior["exact_behavior_identity"],
                "behavior_cluster_id": int(behavior["behavior_cluster_id"]),
            }
        )
    admitted = select_diversity_admission(admission_inputs, maximum_pairs=args.maximum_pairs)
    if not (active_summary["fidelity_gates"]["all_pass"] and session_summary["fidelity_gates"]["all_pass"]):
        raise RuntimeError("signal sketch fidelity/stability gates failed; diversity admission is not authoritative")
    _write_csv(root / "CN_DIVERSITY_ADMISSION.csv", admitted)
    route_metrics = {
        route_id: _distribution(row for row in admission_inputs if row["route_id"] == route_id)
        for route_id in sorted({row["route_id"] for row in admission_inputs})
    }
    for route_id, metrics in route_metrics.items():
        route_clusters = {
            row["behavior_cluster_id"] for row in admission_inputs
            if row["route_id"] == route_id and row["behavior_cluster_id"] > 0
        }
        if route_id in {"FIRSTN_PATH", "MARKET_REGIME_CONDITION", "INTRADAY_STATE_TRANSITION"}:
            baseline = {
                row["behavior_cluster_id"] for row in admission_inputs
                if row["route_id"] == "MINUTE_STATIC" and row["behavior_cluster_id"] > 0
            }
            metrics["new_cluster_count_vs_clock_baseline"] = len(route_clusters - baseline)
            metrics["clock_baseline_route"] = "MINUTE_STATIC"
        elif route_id == "DISCLOSURE_EVENT":
            baseline = {
                row["behavior_cluster_id"] for row in admission_inputs
                if row["route_id"] in {"SLOW_CROSS_SECTIONAL_LEVEL", "SLOW_TEMPORAL_CHANGE"}
                and row["behavior_cluster_id"] > 0
            }
            metrics["new_cluster_count_vs_clock_baseline"] = len(route_clusters - baseline)
            metrics["clock_baseline_route"] = "SLOW_LEVEL_AND_CHANGE"
        else:
            metrics["new_cluster_count_vs_clock_baseline"] = None
            metrics["clock_baseline_route"] = None
        metrics["admitted_pair_count"] = sum(1 for row in admitted if row["route_id"] == route_id)
    summary = {
        "status": "COMPOSITIONAL_SIGNAL_ADMISSION_COMPLETE",
        "admission_version": ADMISSION_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "labels_or_returns_used": False,
        "validation_holdout_forward_read": False,
        "structural_preadmission_pairs": len(admission_inputs),
        "behavior_coverage_qualified_pairs": sum(1 for row in admission_inputs if row["behavior_cluster_id"] > 0),
        "behavior_coverage_limited_pairs": sum(1 for row in admission_inputs if row["behavior_cluster_id"] == 0),
        "admitted_pairs": len(admitted),
        "admission_natural_underfill": len(admitted) < args.maximum_pairs,
        "clock_summaries": {"active_bar": active_summary, "stock_session": session_summary},
        "overall": _distribution(admission_inputs),
        "route_metrics": route_metrics,
        "admission_policy_counts": dict(Counter(row["policy_id"] for row in admitted)),
        "admission_seed_counts": dict(Counter(str(row["seed"]) for row in admitted)),
        "artifacts": {
            "cluster_registry_sha256": _sha256(root / "CN_SIGNAL_CLUSTER_REGISTRY.csv"),
            "diversity_admission_sha256": _sha256(root / "CN_DIVERSITY_ADMISSION.csv"),
        },
    }
    _write_json(root / "CN_BEHAVIOR_CLUSTERS.json", summary)
    waterfall_path = root / "CN_ADMISSION_WATERFALL.csv"
    waterfall = _read_csv(waterfall_path)
    for row in waterfall:
        metrics = route_metrics[str(row["route_id"])]
        row["behavior_unique"] = metrics["cluster_count"]
        row["materialization_pass"] = metrics["candidate_count"]
        row["support_pass"] = metrics["coverage_qualified_count"]
    _write_csv(waterfall_path, waterfall)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
