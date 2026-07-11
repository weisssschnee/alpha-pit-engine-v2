from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from our_system_phase2.services.signal_vector_semantics import classify_candidate_signal_semantics


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def distribution_metrics(values: Iterable[str]) -> dict[str, Any]:
    counts = Counter(str(value or "<missing>") for value in values)
    total = sum(counts.values())
    if not total:
        return {
            "cluster_count": 0,
            "n_eff": 0.0,
            "top_cluster_share": 0.0,
            "entropy_nats": 0.0,
            "normalized_entropy": 0.0,
            "trial_multiplicity": 0.0,
            "effective_trial_multiplicity": 0.0,
            "largest_clusters": [],
        }
    probabilities = np.asarray(list(counts.values()), dtype=float) / total
    entropy = float(-np.sum(probabilities * np.log(probabilities)))
    n_eff = float(1.0 / np.sum(probabilities * probabilities))
    return {
        "cluster_count": len(counts),
        "n_eff": round(n_eff, 8),
        "top_cluster_share": round(float(probabilities.max()), 8),
        "entropy_nats": round(entropy, 8),
        "normalized_entropy": round(entropy / math.log(len(counts)), 8) if len(counts) > 1 else 0.0,
        "trial_multiplicity": round(total / len(counts), 8),
        "effective_trial_multiplicity": round(total / n_eff, 8),
        "largest_clusters": [{"key": key, "count": count} for key, count in counts.most_common(10)],
    }


def _lineage_key(row: dict[str, Any]) -> str:
    parent = str(row.get("parent_id") or "").strip()
    if parent:
        return "parent:" + parent
    return "|".join(
        [
            str(row.get("generator_arm") or "unknown_arm"),
            str(row.get("family_id") or "unknown_family"),
            str(row.get("motif_id") or "unknown_motif"),
        ]
    )


def _stage_metrics(stage: str, path: Path, rows: list[dict[str, Any]]) -> dict[str, Any]:
    exact_key = next(
        (key for key in ("expression_hash", "candidate_id", "family_id", "arm_id") if rows and key in rows[0]),
        "row_identity",
    )
    views: dict[str, Any] = {}
    for key in (
        "ast_skeleton",
        "family_id",
        "motif_id",
        "generator_arm",
        "generator_route",
        "field_family",
        "primitive_family",
        "event_state_family",
    ):
        if rows and key in rows[0]:
            views[key] = distribution_metrics(row.get(key, "") for row in rows)
    lineage = distribution_metrics(_lineage_key(row) for row in rows)
    skeleton = views.get("ast_skeleton")
    significant = bool(
        skeleton
        and len(rows)
        and (
            float(skeleton["n_eff"]) / len(rows) < 0.25
            or float(skeleton["top_cluster_share"]) > 0.25
        )
    )
    return {
        "stage": stage,
        "source": str(path),
        "row_count": len(rows),
        "exact_identity_key": exact_key,
        "exact_identity_count": (
            len({str(row.get(exact_key) or "") for row in rows})
            if exact_key != "row_identity"
            else len(rows)
        ),
        "lineage": lineage,
        "cluster_views": views,
        "signal": {"status": "not_measured", "reason": "no semantic-only progress artifact supplied"},
        "behaviour": {"status": "not_applicable"},
        "significant_structural_collapse": significant,
    }


def _signal_metrics(
    candidates: list[dict[str, Any]],
    progress_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    candidates = sorted(candidates, key=lambda row: str(row.get("candidate_id") or ""))
    decisions = classify_candidate_signal_semantics(candidates, progress_rows)
    signatures: dict[str, str] = {}
    for candidate_id, rows in _group(progress_rows, "candidate_id").items():
        parts = sorted(
            f"{row.get('shard_index')}:{row.get('sample_block_index')}:{row.get('signal_rank_hash')}:{row.get('signal_valid_mask_hash')}"
            for row in rows
        )
        signatures[candidate_id] = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    passed = [candidate_id for candidate_id, row in decisions.items() if row.get("signal_semantic_decision") == "PASS"]
    equivalent = [candidate_id for candidate_id, row in decisions.items() if row.get("signal_semantic_decision") == "REJECT_SIGNAL_EQUIVALENT"]
    missing = [candidate_id for candidate_id, row in decisions.items() if row.get("signal_semantic_decision") == "REJECT_MISSING_SIGNAL_DIAGNOSTICS"]
    constant = [candidate_id for candidate_id, row in decisions.items() if row.get("signal_semantic_decision") == "REJECT_CONSTANT_SIGNAL"]
    owners = [str(decisions[candidate_id].get("signal_equivalent_to") or candidate_id) for candidate_id in passed + equivalent]
    return {
        "status": "measured",
        "candidate_count": len(candidates),
        "diagnostic_candidate_count": len(signatures),
        "exact_signal_identity_count": len(set(signatures.values())),
        "signal_cluster_metrics": distribution_metrics(owners),
        "passed_cluster_owner_count": len(passed),
        "equivalent_candidate_count": len(equivalent),
        "missing_diagnostic_count": len(missing),
        "constant_candidate_count": len(constant),
    }


def _group(rows: Iterable[dict[str, Any]], key: str) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key) or "")].append(row)
    return grouped


def _greedy_clique_clusters(correlation: np.ndarray, threshold: float) -> list[int]:
    clusters: list[list[int]] = []
    for index in range(len(correlation)):
        owner = None
        for cluster_index, members in enumerate(clusters):
            if all(float(correlation[index, member]) >= threshold for member in members):
                owner = cluster_index
                break
        if owner is None:
            clusters.append([index])
        else:
            clusters[owner].append(index)
    labels = [0] * len(correlation)
    for label, members in enumerate(clusters, 1):
        for member in members:
            labels[member] = label
    return labels


def _behaviour_metrics(
    reward_rows: list[dict[str, Any]],
    reward_atom_paths: list[Path],
    split_manifest: Path,
    *,
    min_days: int,
    correlation_threshold: float,
) -> dict[str, Any]:
    split_rows = _read_csv(split_manifest)
    manifest_roles: dict[str, str] = {}
    for row in split_rows:
        trade_date = str(row.get("trade_date") or "")
        role = str(row.get("split") or "")
        if not trade_date or role not in {"train", "validation", "holdout"}:
            raise RuntimeError(f"invalid fixed split manifest row: {row}")
        previous = manifest_roles.setdefault(trade_date, role)
        if previous != role:
            raise RuntimeError(f"fixed split manifest assigns {trade_date} to both {previous} and {role}")
    development_dates = sorted(date for date, role in manifest_roles.items() if role == "train")
    date_index = {value: index for index, value in enumerate(development_dates)}
    reward_rows = sorted(reward_rows, key=lambda row: str(row.get("candidate_id") or ""))
    candidate_ids = [str(row.get("candidate_id") or "") for row in reward_rows]
    candidate_index = {value: index for index, value in enumerate(candidate_ids)}
    hash_to_candidate = {str(row.get("expression_hash") or ""): str(row.get("candidate_id") or "") for row in reward_rows}
    matrix = np.full((len(candidate_ids), len(development_dates)), np.nan, dtype=float)
    seen = np.zeros_like(matrix, dtype=np.int16)
    atom_rows = 0
    used_rows = 0
    raw_split_label_conflict_count = 0
    manifest_non_development_rows_skipped = 0
    for path in reward_atom_paths:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            for row in csv.DictReader(fh):
                atom_rows += 1
                if str(row.get("horizon_min")) != "all":
                    continue
                trade_date = str(row.get("trade_date") or "")
                candidate_id = hash_to_candidate.get(str(row.get("expression_hash") or ""))
                manifest_role = manifest_roles.get(trade_date)
                if manifest_role in {"validation", "holdout"}:
                    manifest_non_development_rows_skipped += 1
                    continue
                if trade_date not in date_index or candidate_id not in candidate_index:
                    continue
                if str(row.get("split") or "") != "train":
                    raw_split_label_conflict_count += 1
                i = candidate_index[candidate_id]
                j = date_index[trade_date]
                value = float(row.get("daily_net_return") or 0.0)
                matrix[i, j] = value if seen[i, j] == 0 else matrix[i, j] + value
                seen[i, j] += 1
                used_rows += 1
    coverage = np.sum(np.isfinite(matrix), axis=1)
    eligible = np.where(coverage >= min_days)[0]
    if not len(eligible):
        return {
            "status": "insufficient_coverage",
            "candidate_count": len(candidate_ids),
            "eligible_candidate_count": 0,
            "min_days": min_days,
            "reward_atom_rows": atom_rows,
            "used_reward_atom_rows": used_rows,
        }
    observed = matrix[eligible]
    exact_hashes: list[str] = []
    for row in observed:
        mask = np.isfinite(row)
        payload = np.packbits(mask).tobytes() + np.round(row[mask], 10).astype("<f8").tobytes()
        exact_hashes.append(hashlib.sha256(payload).hexdigest())
    count = len(observed)
    correlation = np.eye(count, dtype=float)
    pairwise_eligible = 0
    for left in range(count):
        for right in range(left + 1, count):
            common = np.isfinite(observed[left]) & np.isfinite(observed[right])
            if int(common.sum()) < min_days:
                continue
            x = observed[left, common]
            y = observed[right, common]
            if float(np.std(x)) <= 1e-15 or float(np.std(y)) <= 1e-15:
                continue
            value = float(np.corrcoef(x, y)[0, 1])
            correlation[left, right] = correlation[right, left] = value
            pairwise_eligible += 1
    labels = _greedy_clique_clusters(correlation, correlation_threshold)
    cluster_metrics = distribution_metrics(str(label) for label in labels)
    assignments = [
        {
            "candidate_id": candidate_ids[int(source_index)],
            "coverage_days": int(coverage[int(source_index)]),
            "exact_behaviour_hash": exact_hash,
            "behaviour_cluster_id": int(label),
        }
        for source_index, exact_hash, label in zip(eligible, exact_hashes, labels, strict=True)
    ]
    return {
        "status": "measured_train_only",
        "definition": (
            f"deterministic greedy all-pairs Pearson correlation clique >= {correlation_threshold} "
            f"on pairwise >= {min_days} fixed-calendar train/development dates"
        ),
        "candidate_count": len(candidate_ids),
        "eligible_candidate_count": len(eligible),
        "ineligible_candidate_count": len(candidate_ids) - len(eligible),
        "coverage_min_days": int(coverage[eligible].min()),
        "coverage_max_days": int(coverage[eligible].max()),
        "exact_behaviour_identity_count": len(set(exact_hashes)),
        "behaviour_cluster_metrics": cluster_metrics,
        "candidate_assignments": assignments,
        "pairwise_eligible_count": pairwise_eligible,
        "reward_atom_rows": atom_rows,
        "used_reward_atom_rows": used_rows,
        "split_assignment_source": "fixed_manifest_overrides_raw_atom_split_label",
        "raw_split_label_conflict_count": raw_split_label_conflict_count,
        "manifest_non_development_rows_skipped": manifest_non_development_rows_skipped,
        "non_development_dates_used": 0,
    }


def _parse_mapping(items: list[str]) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"expected NAME=PATH, observed {item!r}")
        name, raw_path = item.split("=", 1)
        result[name] = Path(raw_path).resolve()
    return result


def _flatten_stage(stage: dict[str, Any]) -> dict[str, Any]:
    skeleton = stage.get("cluster_views", {}).get("ast_skeleton", {})
    family = stage.get("cluster_views", {}).get("family_id", {})
    behaviour = stage.get("behaviour", {}).get("behaviour_cluster_metrics", {})
    signal = stage.get("signal", {}).get("signal_cluster_metrics", {})
    selected_multiplicity = (
        behaviour.get("trial_multiplicity")
        or signal.get("trial_multiplicity")
        or skeleton.get("trial_multiplicity")
        or stage.get("lineage", {}).get("trial_multiplicity", "")
    )
    selected_effective_multiplicity = (
        behaviour.get("effective_trial_multiplicity")
        or signal.get("effective_trial_multiplicity")
        or skeleton.get("effective_trial_multiplicity")
        or stage.get("lineage", {}).get("effective_trial_multiplicity", "")
    )
    return {
        "stage": stage["stage"],
        "row_count": stage["row_count"],
        "exact_identity_count": stage["exact_identity_count"],
        "signal_status": stage.get("signal", {}).get("status"),
        "signal_cluster_count": signal.get("cluster_count", ""),
        "signal_n_eff": signal.get("n_eff", ""),
        "behaviour_status": stage.get("behaviour", {}).get("status"),
        "behaviour_cluster_count": behaviour.get("cluster_count", ""),
        "behaviour_n_eff": behaviour.get("n_eff", ""),
        "behaviour_top_cluster_share": behaviour.get("top_cluster_share", ""),
        "ast_skeleton_count": skeleton.get("cluster_count", ""),
        "ast_skeleton_n_eff": skeleton.get("n_eff", ""),
        "ast_skeleton_top_cluster_share": skeleton.get("top_cluster_share", ""),
        "family_count": family.get("cluster_count", ""),
        "lineage_entropy_nats": stage.get("lineage", {}).get("entropy_nats", ""),
        "lineage_n_eff": stage.get("lineage", {}).get("n_eff", ""),
        "trial_multiplicity": selected_multiplicity,
        "effective_trial_multiplicity": selected_effective_multiplicity,
        "significant_structural_collapse": stage.get("significant_structural_collapse", False),
    }


def _render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# EVALRESET Search Collapse Audit",
        "",
        f"Created: {summary['created_at']}",
        "",
        "All behaviour clustering uses fixed-calendar train/development dates only. Validation, holdout, and forward data are excluded.",
        "",
        "| stage | rows | exact | signal clusters | behaviour clusters | skeletons | skeleton N_eff | top skeleton | lineage entropy | effective multiplicity |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for stage in summary["stages"]:
        flat = _flatten_stage(stage)
        lines.append(
            f"| {flat['stage']} | {flat['row_count']} | {flat['exact_identity_count']} | "
            f"{flat['signal_cluster_count'] or 'N/A'} | {flat['behaviour_cluster_count'] or 'N/A'} | "
            f"{flat['ast_skeleton_count'] or 'N/A'} | {flat['ast_skeleton_n_eff'] or 'N/A'} | "
            f"{flat['ast_skeleton_top_cluster_share'] or 'N/A'} | {flat['lineage_entropy_nats']} | {flat['effective_trial_multiplicity']} |"
        )
    lines.extend(
        [
            "",
            "## Collapse Status",
            "",
            f"- Earliest observed structural redundancy: `{summary['earliest_observed_structural_redundancy']['stage']}`",
            f"- Structural basis: {summary['earliest_observed_structural_redundancy']['basis']}",
            f"- First signal-level collapse: `{summary['first_signal_level_collapse']['stage']}`",
            f"- Signal-level status: {summary['first_signal_level_collapse']['basis']}",
            "",
            "## Coverage Limits",
            "",
            "- Signal cluster fields are `N/A` unless a semantic-only progress artifact is supplied.",
            "- Behaviour clusters exclude candidates with fewer than the configured minimum train/development dates.",
            "- Family and motif are lineage/grammar views, not economic hypothesis identities.",
        ]
    )
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", action="append", default=[], help="NAME=CSV in pipeline order")
    parser.add_argument("--signal-progress", action="append", default=[], help="NAME=CSV")
    parser.add_argument("--strict-reward-stage", default="strict_reward")
    parser.add_argument("--reward-atoms", action="append", type=Path, default=[])
    parser.add_argument("--split-manifest", type=Path)
    parser.add_argument("--behaviour-min-days", type=int, default=250)
    parser.add_argument("--behaviour-correlation-threshold", type=float, default=0.95)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)

    stage_paths = _parse_mapping(args.stage)
    signal_paths = _parse_mapping(args.signal_progress)
    stages: list[dict[str, Any]] = []
    stage_rows: dict[str, list[dict[str, Any]]] = {}
    for name, path in stage_paths.items():
        rows = _read_csv(path)
        stage_rows[name] = rows
        stage = _stage_metrics(name, path, rows)
        if name in signal_paths:
            stage["signal"] = _signal_metrics(rows, _read_csv(signal_paths[name]))
        stages.append(stage)

    strict_stage = next((stage for stage in stages if stage["stage"] == args.strict_reward_stage), None)
    if strict_stage and args.reward_atoms and args.split_manifest:
        strict_stage["behaviour"] = _behaviour_metrics(
            stage_rows[args.strict_reward_stage],
            [path.resolve() for path in args.reward_atoms],
            args.split_manifest.resolve(),
            min_days=max(3, args.behaviour_min_days),
            correlation_threshold=max(0.0, min(1.0, args.behaviour_correlation_threshold)),
        )

    first = next((stage for stage in stages if stage["significant_structural_collapse"]), None)
    earliest_structural = {
        "stage": first["stage"] if first else "not_identified",
        "basis": (
            "AST-skeleton N_eff / candidate rows < 0.25 or top skeleton share > 0.25; "
            "this identifies redundancy only and is not evidence of signal-level collapse"
            if first
            else "no stage crossed the structural redundancy threshold"
        ),
    }
    first_signal = {
        "stage": "not_identified",
        "basis": "signal sketches and fidelity/stability validation are not complete",
    }
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "EVALRESET_COLLAPSE_AUDIT_DIAGNOSTIC_ONLY",
        "stage_order": list(stage_paths),
        "earliest_observed_structural_redundancy": earliest_structural,
        "first_signal_level_collapse": first_signal,
        "behaviour_data_role": "development/train_only",
        "validation_holdout_forward_used_for_clustering": False,
        "stages": stages,
    }
    output_root = args.output_root.resolve()
    _write_json(output_root / "search_collapse_audit.json", summary)
    _write_csv(output_root / "search_collapse_stage_metrics.csv", [_flatten_stage(stage) for stage in stages])
    (output_root / "SEARCH_COLLAPSE_AUDIT.md").write_text(_render_markdown(summary), encoding="utf-8")
    print(
        json.dumps(
            {
                "output_root": str(output_root),
                "earliest_observed_structural_redundancy": earliest_structural,
                "first_signal_level_collapse": first_signal,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
