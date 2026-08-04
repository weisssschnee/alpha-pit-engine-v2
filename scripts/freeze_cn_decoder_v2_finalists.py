"""Freeze train-only Decoder V2 finalists without reading evaluation data.

The freeze is intentionally narrower than a generic portfolio search.  It
binds one already-closed decoder treatment, applies the four economic gates
accepted by ADR 0014, preserves the original frozen pair order, and keeps the
first occurrence of each authoritative economic mechanism.  No train result is
recomputed and no validation, holdout, or 2026 asset is opened.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)


DECODER_ID = "TOPK_10_EQUAL"
EXPECTED_SOURCE_PAIRS = 32
EXPECTED_SOURCE_MEMBERS = 64
ECONOMIC_GATE_COLUMNS = (
    "primary_continuous_book_net_reward",
    "matched_continuous_book_net_reward_increment",
    "primary_cumulative_net_return",
    "matched_cumulative_net_return_increment",
)
PROHIBITED_WRITE_FIELDS = (
    "optimizer_feedback_write",
    "scheduler_write",
    "archive_write",
    "promotion",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _verify_self_hash(
    payload: Mapping[str, Any], *, field: str, label: str
) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if not expected or expected != observed:
        raise RuntimeError(
            f"{label} self-hash mismatch: expected={expected} observed={observed}"
        )


def _verify_artifacts(
    payload: Mapping[str, Any], *, root: Path, label: str
) -> None:
    for artifact in payload.get("artifacts") or []:
        path = (root / str(artifact["path"])).resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"{label} artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"{label} artifact hash mismatch: {path}")


def _resolve_source_artifact(
    *, source_root: Path, artifact: Mapping[str, Any], label: str
) -> Path:
    path = (source_root / str(artifact["path"])).resolve()
    if not path.is_file():
        raise FileNotFoundError(path)
    if path.stat().st_size != int(artifact["bytes"]):
        raise RuntimeError(f"{label} size mismatch: {path}")
    if _sha256(path) != str(artifact["sha256"]):
        raise RuntimeError(f"{label} hash mismatch: {path}")
    return path


def _finite_positive(value: Any) -> bool:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return False
    return math.isfinite(number) and number > 0.0


def _gate_blockers(row: Mapping[str, Any]) -> list[str]:
    return [
        f"{column.upper()}_NOT_POSITIVE"
        for column in ECONOMIC_GATE_COLUMNS
        if not _finite_positive(row.get(column))
    ]


def _identity_value(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        return json.dumps(list(value), ensure_ascii=False, sort_keys=True)
    if hasattr(value, "tolist"):
        return json.dumps(value.tolist(), ensure_ascii=False, sort_keys=True)
    if value is None:
        return ""
    return str(value)


def _counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if column not in frame.columns or frame.empty:
        return {}
    values = [_identity_value(value) for value in frame[column].tolist()]
    return dict(sorted(Counter(values).items()))


def freeze_decoder_v2_finalists(
    *,
    decoder_root: Path,
    source_replay_root: Path,
    output_root: Path,
    generator_repo_sha: str,
    expected_selection_payload_sha256: str,
    expected_decoder_contract_payload_sha256: str,
    expected_selected_pairs: int | None = None,
) -> dict[str, Any]:
    decoder_root = Path(decoder_root).resolve()
    source_replay_root = Path(source_replay_root).resolve()
    output_root = Path(output_root).resolve()
    if output_root.exists():
        raise FileExistsError(output_root)
    if len(generator_repo_sha) != 40:
        raise ValueError("generator_repo_sha must be a 40-character Git SHA")

    closure_path = decoder_root / "DECODER_V2_COMPLETE.json"
    closure = _read_json(closure_path)
    _verify_self_hash(
        closure, field="manifest_body_sha256", label="Decoder V2 closure"
    )
    _verify_artifacts(closure, root=decoder_root, label="Decoder V2 closure")
    if str(closure.get("status") or "") != (
        "CN_PORTFOLIO_DECODER_V2_CLOSED_IMMUTABLE_DIAGNOSTIC_ONLY"
    ):
        raise RuntimeError("Decoder V2 evidence is not immutable")
    if str(closure.get("selection_payload_sha256") or "") != (
        expected_selection_payload_sha256
    ):
        raise RuntimeError("Decoder V2 selection binding drift")
    if int(closure.get("pair_count") or 0) != EXPECTED_SOURCE_PAIRS:
        raise RuntimeError("Decoder V2 source pair count drift")
    if int(closure.get("candidate_member_count") or 0) != EXPECTED_SOURCE_MEMBERS:
        raise RuntimeError("Decoder V2 source member count drift")
    if int(closure.get("pair_metric_count") or 0) != 96:
        raise RuntimeError("Decoder V2 pair metric count drift")
    if str(closure.get("accounting_invariants_status") or "") != "PASS":
        raise RuntimeError("Decoder V2 accounting invariants are not PASS")
    if str(closure.get("baseline_parity_status") or "") != "PASS":
        raise RuntimeError("Decoder V2 baseline parity is not PASS")
    if any(
        int(closure.get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("sealed reads detected in Decoder V2 closure")

    binding_path = decoder_root / "input_binding.json"
    binding = _read_json(binding_path)
    contracts = {
        str(item["decoder_id"]): item
        for item in binding.get("decoder_contract") or []
    }
    decoder_contract = contracts.get(DECODER_ID)
    if decoder_contract is None:
        raise RuntimeError(f"{DECODER_ID} contract is missing")
    if str(decoder_contract.get("payload_sha256") or "") != (
        expected_decoder_contract_payload_sha256
    ):
        raise RuntimeError("decoder contract payload drift")
    expected_contract = {
        "decoder_id": DECODER_ID,
        "selection": "TOP_K",
        "top_k": 10,
        "weighting": "EQUAL",
        "target_refresh_clock": "EACH_SESSION_OPEN_FROM_PRIOR_CLOSE_SIGNAL",
        "session_end_policy": "FINAL_CLOSE_MARK_NO_FORCED_SALE",
    }
    for key, value in expected_contract.items():
        if decoder_contract.get(key) != value:
            raise RuntimeError(f"decoder contract drift at {key}")

    source_freeze_path = (
        source_replay_root
        / "prepared"
        / "finalist_replay_then_oos_freeze.json"
    )
    source_freeze = _read_json(source_freeze_path)
    _verify_self_hash(
        source_freeze,
        field="manifest_body_sha256",
        label="source frozen cohort",
    )
    if str(source_freeze.get("status") or "") != "FROZEN_UNCHANGED_FIXED_COHORT":
        raise RuntimeError("source cohort is not frozen")
    if str(source_freeze.get("selection_payload_sha256") or "") != (
        expected_selection_payload_sha256
    ):
        raise RuntimeError("source frozen selection drift")
    if int(source_freeze.get("pair_count") or 0) != EXPECTED_SOURCE_PAIRS:
        raise RuntimeError("source frozen pair count drift")
    if int(source_freeze.get("candidate_member_count") or 0) != (
        EXPECTED_SOURCE_MEMBERS
    ):
        raise RuntimeError("source frozen member count drift")
    pair_input_path = _resolve_source_artifact(
        source_root=source_replay_root,
        artifact=source_freeze["pair_artifact"],
        label="source pair artifact",
    )
    candidate_input_path = _resolve_source_artifact(
        source_root=source_replay_root,
        artifact=source_freeze["candidate_artifact"],
        label="source candidate artifact",
    )

    metrics_path = decoder_root / "decoder_pair_metrics.parquet"
    metrics = pd.read_parquet(metrics_path).where(pd.notna, None)
    decoder_metrics = metrics[metrics["decoder_id"].eq(DECODER_ID)].copy()
    if len(decoder_metrics) != EXPECTED_SOURCE_PAIRS:
        raise RuntimeError(f"{DECODER_ID} metric slice count drift")
    if decoder_metrics["pair_id"].astype(str).duplicated().any():
        raise RuntimeError(f"{DECODER_ID} metric pair duplicates")
    missing_gates = sorted(set(ECONOMIC_GATE_COLUMNS).difference(metrics.columns))
    if missing_gates:
        raise RuntimeError(f"Decoder V2 economic gate columns missing: {missing_gates}")

    source_pairs = pd.read_parquet(pair_input_path).where(pd.notna, None)
    source_candidates = pd.read_parquet(candidate_input_path).where(pd.notna, None)
    if "keep_review_rank" not in source_pairs.columns:
        raise RuntimeError("source frozen pairs lack keep_review_rank authority")
    if source_pairs["keep_review_rank"].duplicated().any():
        raise RuntimeError("source keep_review_rank duplicates")
    source_pairs = source_pairs.sort_values(
        ["keep_review_rank", "pair_id"], kind="mergesort"
    ).reset_index(drop=True)
    source_pair_ids = source_pairs["pair_id"].astype(str).tolist()
    source_candidate_ids = source_candidates["candidate_id"].astype(str).tolist()
    if set(source_pair_ids) != set(map(str, source_freeze["pair_ids"])):
        raise RuntimeError("source frozen pair identity drift")
    if set(source_candidate_ids) != set(map(str, source_freeze["candidate_ids"])):
        raise RuntimeError("source frozen candidate identity drift")
    if set(decoder_metrics["pair_id"].astype(str)) != set(source_pair_ids):
        raise RuntimeError("Decoder V2/source pair identity drift")

    source = source_pairs.merge(
        decoder_metrics,
        on="pair_id",
        how="left",
        sort=False,
        validate="one_to_one",
        suffixes=("", "_decoder"),
    )
    if source["pair_id"].astype(str).tolist() != source_pair_ids:
        raise RuntimeError("Decoder finalist merge reordered source pairs")
    for member in ("primary_candidate_id", "control_candidate_id"):
        decoder_column = f"{member}_decoder"
        if decoder_column in source.columns and not (
            source[member].astype(str) == source[decoder_column].astype(str)
        ).all():
            raise RuntimeError(f"Decoder/source {member} drift")

    source["economic_admission_blockers"] = source.apply(
        lambda row: "|".join(_gate_blockers(row)), axis=1
    )
    source["source_pair_order"] = range(1, len(source) + 1)
    selected_rows: list[int] = []
    duplicate_mechanism_rows: list[int] = []
    seen_mechanisms: set[str] = set()
    for index, row in source.iterrows():
        if str(row["economic_admission_blockers"]):
            continue
        mechanism_id = str(row.get("economic_mechanism_id") or "")
        if not mechanism_id:
            raise RuntimeError("eligible pair has no economic mechanism identity")
        if mechanism_id in seen_mechanisms:
            duplicate_mechanism_rows.append(index)
            continue
        selected_rows.append(index)
        seen_mechanisms.add(mechanism_id)
    selected = source.loc[selected_rows].copy().reset_index(drop=True)
    if expected_selected_pairs is not None and len(selected) != int(
        expected_selected_pairs
    ):
        raise RuntimeError(
            "Decoder finalist count drift: "
            f"expected={expected_selected_pairs} observed={len(selected)}"
        )
    selected_ids = selected["pair_id"].astype(str).tolist()
    selected["finalist_order"] = range(1, len(selected) + 1)
    selected["finalist_outcome"] = "FROZEN_DECODER_V2_TRAIN_ONLY_FINALIST"
    selected["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected["promotion_eligible"] = False

    order = dict(zip(selected_ids, range(1, len(selected_ids) + 1)))
    selected_candidates = source_candidates[
        source_candidates["pair_id"].astype(str).isin(selected_ids)
    ].copy()
    if len(selected_candidates) != len(selected) * 2:
        raise RuntimeError("Decoder finalist candidate member count drift")
    role_order = {"PRIMARY": 0, "CONTROL": 1}
    selected_candidates["_pair_order"] = selected_candidates[
        "pair_id"
    ].astype(str).map(order)
    selected_candidates["_role_order"] = selected_candidates[
        "pair_member_role"
    ].astype(str).map(role_order)
    if selected_candidates["_role_order"].isna().any():
        raise RuntimeError("Decoder finalist candidate role drift")
    selected_candidates = selected_candidates.sort_values(
        ["_pair_order", "_role_order", "candidate_id"], kind="mergesort"
    ).drop(columns=["_pair_order", "_role_order"]).reset_index(drop=True)
    expected_selected_candidate_ids: list[str] = []
    for row in selected.to_dict(orient="records"):
        expected_selected_candidate_ids.extend(
            [str(row["primary_candidate_id"]), str(row["control_candidate_id"])]
        )
    if selected_candidates["candidate_id"].astype(str).tolist() != (
        expected_selected_candidate_ids
    ):
        raise RuntimeError("Decoder finalist candidate primary/control order drift")
    selected_candidates["finalist_order"] = selected_candidates[
        "pair_id"
    ].astype(str).map(order)
    selected_candidates["finalist_outcome"] = (
        "FROZEN_DECODER_V2_TRAIN_ONLY_FINALIST"
    )
    selected_candidates["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    selected_candidates["promotion_eligible"] = False

    review = source.copy()
    review["finalist_outcome"] = "ECONOMIC_ADMISSION_BLOCKED_NO_BACKFILL"
    review.loc[duplicate_mechanism_rows, "finalist_outcome"] = (
        "DUPLICATE_ECONOMIC_MECHANISM_FIRST_OCCURRENCE_KEPT"
    )
    review.loc[
        review["pair_id"].astype(str).isin(selected_ids), "finalist_outcome"
    ] = "FROZEN_DECODER_V2_TRAIN_ONLY_FINALIST"
    review["finalist_order"] = review["pair_id"].astype(str).map(order)
    review["evidence_scope"] = "DEVELOPMENT_TRAIN_ONLY"
    review["promotion_eligible"] = False

    output_root.mkdir(parents=True)
    contract = {
        "schema_version": "cn_decoder_v2_train_only_finalist_freeze_v1",
        "status": "ACTIVE_TRAIN_ONLY_DECODER_FINALIST_FREEZE",
        "generator_repo_sha": generator_repo_sha,
        "decoder_root": str(decoder_root),
        "decoder_closure_sha256": _sha256(closure_path),
        "decoder_closure_payload_sha256": str(closure["manifest_body_sha256"]),
        "decoder_id": DECODER_ID,
        "decoder_contract_payload_sha256": str(
            decoder_contract["payload_sha256"]
        ),
        "source_replay_root": str(source_replay_root),
        "source_freeze_sha256": _sha256(source_freeze_path),
        "source_selection_payload_sha256": expected_selection_payload_sha256,
        "source_pair_artifact_sha256": _sha256(pair_input_path),
        "source_candidate_artifact_sha256": _sha256(candidate_input_path),
        "expected_selected_pairs": expected_selected_pairs,
        "actual_selected_pairs": len(selected),
        "selection_order_policy": "ORIGINAL_FROZEN_PAIR_ORDER",
        "mechanism_policy": "AUTHORITATIVE_ID_FIRST_OCCURRENCE",
        "eligibility": {column: ">0_FINITE" for column in ECONOMIC_GATE_COLUMNS},
        "blocked_backfill": "FORBIDDEN",
        "authority_boundary": {
            "evidence_scope": "DEVELOPMENT_TRAIN_ONLY",
            "financial_result_recomputed": False,
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
            "optimizer_feedback_write": "FORBIDDEN",
            "scheduler_write": "FORBIDDEN",
            "archive_write": "FORBIDDEN",
            "promotion": "FORBIDDEN",
            "oos_execution_requires_independent_gatekeeper": True,
        },
    }
    contract["contract_payload_sha256"] = _payload_sha256(contract)
    contract_path = _write_json(output_root / "finalist_contract.json", contract)
    review_path = output_root / "finalist_review_ledger.parquet"
    pair_path = output_root / "finalist_pairs.parquet"
    candidate_path = output_root / "finalist_candidates.parquet"
    review.to_parquet(review_path, index=False)
    selected.to_parquet(pair_path, index=False)
    selected_candidates.to_parquet(candidate_path, index=False)

    identity_columns = (
        "route_id",
        "portfolio_exposure_family_id",
        "structural_family_id",
        "signal_cluster_id",
        "operator_family",
        "skeleton_id",
        "source_field_ids",
        "operator_paths",
        "financial_hypothesis",
    )
    concentration = {column: _counts(selected, column) for column in identity_columns}
    exposure_counts = concentration["portfolio_exposure_family_id"]
    max_exposure_count = max(exposure_counts.values(), default=0)
    selection_payload = {
        "pair_ids": selected_ids,
        "economic_mechanism_ids": selected[
            "economic_mechanism_id"
        ].astype(str).tolist(),
        "decoder_id": DECODER_ID,
        "decoder_contract_payload_sha256": str(
            decoder_contract["payload_sha256"]
        ),
        "decoder_closure_sha256": _sha256(closure_path),
        "source_freeze_sha256": _sha256(source_freeze_path),
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "selection_order_policy": "ORIGINAL_FROZEN_PAIR_ORDER",
        "economic_gate_columns": list(ECONOMIC_GATE_COLUMNS),
    }
    summary = {
        "schema_version": "cn_decoder_v2_train_only_finalist_freeze_v1",
        "status": "DECODER_V2_TRAIN_ONLY_FINALISTS_FROZEN_NO_BACKFILL",
        "decoder_id": DECODER_ID,
        "source_pairs": len(source),
        "eligible_four_gate_pairs": int(
            source["economic_admission_blockers"].astype(str).eq("").sum()
        ),
        "selected_pairs": len(selected),
        "selected_candidate_members": len(selected_candidates),
        "selected_pair_ids": selected_ids,
        "selected_economic_mechanism_unique": int(
            selected["economic_mechanism_id"].astype(str).nunique()
        ),
        "duplicate_mechanism_rows_excluded": len(duplicate_mechanism_rows),
        "blocked_rows_backfilled": 0,
        "family_concentration": concentration,
        "maximum_portfolio_exposure_family_count": max_exposure_count,
        "maximum_portfolio_exposure_family_share": (
            float(max_exposure_count / len(selected)) if len(selected) else 0.0
        ),
        "selection_payload_sha256": _payload_sha256(selection_payload),
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    summary_path = _write_json(output_root / "finalist_summary.json", summary)
    artifacts = [
        _artifact(path, root=output_root)
        for path in (
            contract_path,
            review_path,
            pair_path,
            candidate_path,
            summary_path,
        )
    ]
    manifest = {
        "schema_version": "cn_decoder_v2_train_only_finalist_freeze_v1",
        "status": "DECODER_V2_TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "generator_repo_sha": generator_repo_sha,
        "decoder_closure_sha256": _sha256(closure_path),
        "decoder_id": DECODER_ID,
        "decoder_contract_payload_sha256": str(
            decoder_contract["payload_sha256"]
        ),
        "source_freeze_sha256": _sha256(source_freeze_path),
        "source_selection_payload_sha256": expected_selection_payload_sha256,
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "selected_pairs": len(selected),
        "selected_candidate_members": len(selected_candidates),
        "artifacts": artifacts,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = _write_json(output_root / "finalist_manifest.json", manifest)
    return {
        "output_root": str(output_root),
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "selection_payload_sha256": summary["selection_payload_sha256"],
        "selected_pairs": len(selected),
        "selected_candidate_members": len(selected_candidates),
        "selected_pair_ids": selected_ids,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--decoder-root", type=Path, required=True)
    parser.add_argument("--source-replay-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--generator-repo-sha", required=True)
    parser.add_argument("--expected-selection-payload-sha256", required=True)
    parser.add_argument(
        "--expected-decoder-contract-payload-sha256", required=True
    )
    parser.add_argument("--expected-selected-pairs", type=int)
    args = parser.parse_args()
    result = freeze_decoder_v2_finalists(
        decoder_root=args.decoder_root,
        source_replay_root=args.source_replay_root,
        output_root=args.output_root,
        generator_repo_sha=args.generator_repo_sha,
        expected_selection_payload_sha256=(
            args.expected_selection_payload_sha256
        ),
        expected_decoder_contract_payload_sha256=(
            args.expected_decoder_contract_payload_sha256
        ),
        expected_selected_pairs=args.expected_selected_pairs,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
