"""Independently verify a train-only finalist freeze."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scripts.freeze_cn_productive_keep_review_cohort import (
    _payload_sha256,
    _sha256,
    _write_json,
)
from our_system_phase2.services.finalist_economic_admission import (
    strict_replay_blockers,
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


def verify_train_replay_finalists(
    *, finalist_root: Path, receipt_path: Path
) -> dict[str, Any]:
    finalist_root = Path(finalist_root).resolve()
    receipt_path = Path(receipt_path).resolve()
    manifest_path = finalist_root / "finalist_manifest.json"
    manifest = _read_json(manifest_path)
    _verify_self_hash(
        manifest, field="manifest_payload_sha256", label="finalist manifest"
    )
    if str(manifest.get("status") or "") != (
        "TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("finalist manifest is not immutable")
    for artifact in manifest.get("artifacts") or []:
        path = finalist_root / str(artifact["path"])
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"finalist artifact size mismatch: {path}")
        if _sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"finalist artifact hash mismatch: {path}")

    contract = _read_json(finalist_root / "finalist_contract.json")
    summary = _read_json(finalist_root / "finalist_summary.json")
    _verify_self_hash(
        contract, field="contract_payload_sha256", label="finalist contract"
    )
    if any(
        int(contract["authority_boundary"].get(field) or 0) != 0
        for field in ("validation_reads", "holdout_reads", "forward_2026_reads")
    ):
        raise RuntimeError("sealed reads in finalist contract")
    if any(
        str(contract["authority_boundary"].get(field) or "") != "FORBIDDEN"
        for field in (
            "optimizer_feedback_write",
            "scheduler_write",
            "archive_write",
            "promotion",
            "automatic_report_only_oos",
            "successor_search",
        )
    ):
        raise RuntimeError("finalist prohibited-write boundary drift")

    replay_root = Path(contract["strict_replay_root"])
    replay_input_root = Path(contract["replay_input_root"])
    replay_closure_path = replay_root / "REPLAY_COMPLETE.json"
    input_manifest_path = replay_input_root / "keep_review_manifest.json"
    if _sha256(replay_closure_path) != str(
        contract["strict_replay_closure_sha256"]
    ):
        raise RuntimeError("strict replay binding drift")
    if _sha256(input_manifest_path) != str(
        contract["replay_input_manifest_sha256"]
    ):
        raise RuntimeError("replay input binding drift")

    input_pairs = pd.read_parquet(
        replay_input_root / "keep_review_pairs.parquet"
    ).where(pd.notna, None)
    replay_pairs = pd.read_parquet(
        replay_root / "pair_replay_results.parquet"
    ).where(pd.notna, None)
    source = input_pairs.merge(
        replay_pairs,
        on="pair_id",
        how="left",
        sort=False,
        validate="one_to_one",
        suffixes=("", "_replay"),
    )
    eligible = source[
        source.apply(lambda row: not strict_replay_blockers(row), axis=1)
    ].copy()
    ranking_columns = [
        "primary_a_share_executable_net_reward",
        "a_share_executable_net_increment",
    ]
    ascending = [False, False]
    for column in (
        "train_stability_score",
        "train_stability_floor",
        "train_stability_median",
        "search_score",
    ):
        if column in eligible.columns:
            ranking_columns.append(column)
            ascending.append(False)
    ranking_columns.append("pair_id")
    ascending.append(True)
    eligible = eligible.sort_values(
        ranking_columns, ascending=ascending, kind="mergesort"
    ).reset_index(drop=True)

    expected_ids: list[str] = []
    seen_mechanisms: set[str] = set()
    maximum = int(contract["target_pairs_maximum"])
    for _, row in eligible.iterrows():
        mechanism_id = str(row["economic_mechanism_id"])
        if mechanism_id in seen_mechanisms:
            continue
        if len(expected_ids) >= maximum:
            break
        expected_ids.append(str(row["pair_id"]))
        seen_mechanisms.add(mechanism_id)

    finalist_pairs = pd.read_parquet(
        finalist_root / "finalist_pairs.parquet"
    ).where(pd.notna, None)
    finalist_candidates = pd.read_parquet(
        finalist_root / "finalist_candidates.parquet"
    ).where(pd.notna, None)
    observed_ids = finalist_pairs["pair_id"].astype(str).tolist()
    if observed_ids != expected_ids:
        raise RuntimeError(
            f"finalist deterministic selection mismatch: "
            f"expected={expected_ids} observed={observed_ids}"
        )
    if finalist_pairs["economic_mechanism_id"].astype(str).nunique() != len(
        finalist_pairs
    ):
        raise RuntimeError("finalist mechanisms are not unique")
    if not (
        finalist_pairs["a_share_replay_status"].astype(str)
        == "PAIR_REPLAY_COMPLETE"
    ).all():
        raise RuntimeError("blocked pair entered finalist freeze")
    if not (
        finalist_pairs["a_share_executable_net_increment"].astype(float) > 0
    ).all():
        raise RuntimeError("nonpositive pair entered finalist freeze")
    if not (
        finalist_pairs["primary_a_share_executable_net_reward"].astype(float)
        > 0
    ).all():
        raise RuntimeError("nonpositive primary reward entered finalist freeze")
    if len(finalist_candidates) != len(finalist_pairs) * 2:
        raise RuntimeError("finalist candidate member count mismatch")
    if set(finalist_candidates["pair_id"].astype(str)) != set(observed_ids):
        raise RuntimeError("finalist candidate pair binding mismatch")
    if int(summary["blocked_rows_backfilled"]) != 0:
        raise RuntimeError("blocked finalist backfill detected")
    if int(summary["selected_pairs"]) != len(finalist_pairs):
        raise RuntimeError("finalist summary pair count drift")
    if str(manifest["selection_payload_sha256"]) != str(
        summary["selection_payload_sha256"]
    ):
        raise RuntimeError("finalist selection payload binding drift")

    receipt = {
        "schema_version": "cn_train_replay_finalist_verification_v1",
        "status": "PASS_INDEPENDENT_VERIFICATION",
        "finalist_root": str(finalist_root),
        "finalist_manifest_sha256": _sha256(manifest_path),
        "finalist_manifest_payload_sha256": str(
            manifest["manifest_payload_sha256"]
        ),
        "strict_replay_closure_sha256": _sha256(replay_closure_path),
        "selected_pairs": len(finalist_pairs),
        "selected_candidate_members": len(finalist_candidates),
        "selected_pair_ids": observed_ids,
        "selected_economic_mechanism_unique": int(
            finalist_pairs["economic_mechanism_id"].nunique()
        ),
        "blocked_rows_backfilled": 0,
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_eligible": False,
    }
    receipt["receipt_payload_sha256"] = _payload_sha256(receipt)
    _write_json(receipt_path, receipt)
    return {
        "receipt_path": str(receipt_path),
        "receipt_file_sha256": _sha256(receipt_path),
        "receipt_payload_sha256": receipt["receipt_payload_sha256"],
        "status": receipt["status"],
        "selected_pairs": len(finalist_pairs),
        "selected_pair_ids": observed_ids,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--finalist-root", type=Path, required=True)
    parser.add_argument("--receipt-path", type=Path, required=True)
    args = parser.parse_args()
    result = verify_train_replay_finalists(
        finalist_root=args.finalist_root, receipt_path=args.receipt_path
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
