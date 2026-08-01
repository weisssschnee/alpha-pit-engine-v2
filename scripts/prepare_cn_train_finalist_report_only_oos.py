"""Prepare and verify a report-only OOS freeze from closed train finalists.

This adapter never reruns train replay.  It derives a one-pair replay evidence
view from the immutable source replay and emits the compatibility artifacts
consumed by ``run_cn_finalist_replay_then_oos.py oos``.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from scripts.run_cn_finalist_replay_then_oos import (
    _artifact,
    _read_json,
    _resolved_artifact,
    _sha256,
    _stable_hash,
    _verify_declared_artifacts,
    _verify_payload_hash,
    _write_json,
)


EXPECTED_PAIR_COUNT = 1
EXPECTED_MEMBER_COUNT = 2
FORBIDDEN_WRITES = (
    "feedback_write",
    "scheduler_write",
    "archive_write",
    "promotion",
)


def _verify_zero_sealed_reads(payload: Mapping[str, Any], *, label: str) -> None:
    for field in ("validation_reads", "holdout_reads", "forward_2026_reads"):
        if int(payload.get(field) or 0) != 0:
            raise RuntimeError(f"{label} contains sealed reads: {field}")


def _verify_forbidden_writes(
    payload: Mapping[str, Any], *, label: str
) -> None:
    for field in FORBIDDEN_WRITES:
        if str(payload.get(field) or "") != "FORBIDDEN":
            raise RuntimeError(f"{label} write boundary drift: {field}")


def _ordered_candidates(
    candidates: pd.DataFrame, pair_ids: list[str]
) -> pd.DataFrame:
    required = {"candidate_id", "pair_id", "pair_member_role", "route_id"}
    missing = required.difference(candidates.columns)
    if missing:
        raise RuntimeError(
            "finalist candidate columns missing: " + ",".join(sorted(missing))
        )
    source = candidates.copy()
    source["pair_id"] = source["pair_id"].astype(str)
    source["candidate_id"] = source["candidate_id"].astype(str)
    source["pair_member_role"] = (
        source["pair_member_role"].astype(str).str.upper()
    )
    if source["candidate_id"].duplicated().any():
        raise RuntimeError("duplicate candidate identity in finalist freeze")
    rows: list[pd.Series] = []
    for pair_id in pair_ids:
        group = source[source["pair_id"] == pair_id]
        if len(group) != 2:
            raise RuntimeError(f"finalist pair member count drift: {pair_id}")
        by_role = {
            str(row["pair_member_role"]): row
            for _, row in group.iterrows()
        }
        if set(by_role) != {"PRIMARY", "CONTROL"}:
            raise RuntimeError(f"finalist pair role drift: {pair_id}")
        rows.extend((by_role["PRIMARY"], by_role["CONTROL"]))
    ordered = pd.DataFrame(rows).reset_index(drop=True)
    if len(ordered) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("single-finalist candidate count drift")
    return ordered


def _read_finalist_authority(finalist_root: Path) -> tuple[dict[str, Any], ...]:
    manifest_path = finalist_root / "finalist_manifest.json"
    contract_path = finalist_root / "finalist_contract.json"
    summary_path = finalist_root / "finalist_summary.json"
    manifest = _read_json(manifest_path)
    contract = _read_json(contract_path)
    summary = _read_json(summary_path)
    _verify_payload_hash(
        manifest,
        field="manifest_payload_sha256",
        label="train finalist manifest",
    )
    _verify_payload_hash(
        contract,
        field="contract_payload_sha256",
        label="train finalist contract",
    )
    _verify_declared_artifacts(manifest, root=finalist_root)
    if str(manifest.get("status") or "") != (
        "TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("train finalist freeze is not immutable")
    if int(manifest.get("selected_pairs") or 0) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("train finalist pair count is not exactly one")
    if int(manifest.get("selected_candidate_members") or 0) != (
        EXPECTED_MEMBER_COUNT
    ):
        raise RuntimeError("train finalist member count is not exactly two")
    if int(summary.get("blocked_rows_backfilled") or 0) != 0:
        raise RuntimeError("train finalist freeze backfilled blocked rows")
    if str(manifest["selection_payload_sha256"]) != str(
        summary["selection_payload_sha256"]
    ):
        raise RuntimeError("train finalist selection binding drift")
    boundary = contract["authority_boundary"]
    _verify_zero_sealed_reads(boundary, label="train finalist contract")
    for field in (
        "optimizer_feedback_write",
        "scheduler_write",
        "archive_write",
        "promotion",
        "successor_search",
    ):
        if str(boundary.get(field) or "") != "FORBIDDEN":
            raise RuntimeError(f"train finalist boundary drift: {field}")
    return manifest, contract, summary


def prepare(
    *, finalist_root: Path, output_root: Path, generator_repo_sha: str
) -> dict[str, Any]:
    finalist_root = Path(finalist_root).resolve()
    output_root = Path(output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    for owned_path in (
        output_root / "prepared",
        output_root / "derived_replay_evidence",
        output_root / "FINALIST_REPORT_ONLY_OOS_PREPARED.json",
    ):
        if owned_path.exists():
            raise FileExistsError(
                f"OOS preparation output already exists: {owned_path}"
            )
    manifest, finalist_contract, summary = _read_finalist_authority(
        finalist_root
    )

    strict_replay_root = Path(finalist_contract["strict_replay_root"]).resolve()
    source_replay_closure_path = strict_replay_root / "REPLAY_COMPLETE.json"
    source_replay_closure = _read_json(source_replay_closure_path)
    _verify_payload_hash(
        source_replay_closure,
        field="manifest_body_sha256",
        label="source strict replay closure",
    )
    _verify_declared_artifacts(
        source_replay_closure, root=strict_replay_root
    )
    if str(source_replay_closure.get("status") or "") != (
        "A_SHARE_REPLAY_CLOSED_IMMUTABLE"
    ):
        raise RuntimeError("source strict replay is not closed immutable")
    _verify_zero_sealed_reads(
        source_replay_closure, label="source strict replay"
    )
    _verify_forbidden_writes(
        source_replay_closure, label="source strict replay"
    )
    if _sha256(source_replay_closure_path) != str(
        finalist_contract["strict_replay_closure_sha256"]
    ):
        raise RuntimeError("train finalist/source replay closure drift")

    source_campaign_root = strict_replay_root.parent
    source_freeze_path = (
        source_campaign_root
        / "prepared"
        / "finalist_replay_then_oos_freeze.json"
    )
    source_freeze = _read_json(source_freeze_path)
    _verify_payload_hash(
        source_freeze,
        field="manifest_body_sha256",
        label="source replay freeze",
    )
    if str(source_freeze.get("status") or "") != (
        "FROZEN_UNCHANGED_FIXED_COHORT"
    ):
        raise RuntimeError("source replay freeze status drift")
    source_contract_path = _resolved_artifact(
        source_campaign_root, source_freeze["execution_contract_artifact"]
    )
    source_execution_contract = _read_json(source_contract_path)
    _verify_payload_hash(
        source_execution_contract,
        field="contract_payload_sha256",
        label="source replay execution contract",
    )

    finalist_pairs = pd.read_parquet(
        finalist_root / "finalist_pairs.parquet"
    ).where(pd.notna, None)
    finalist_candidates = pd.read_parquet(
        finalist_root / "finalist_candidates.parquet"
    ).where(pd.notna, None)
    if len(finalist_pairs) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("single-finalist pair artifact count drift")
    pair_ids = finalist_pairs["pair_id"].astype(str).tolist()
    ordered_candidates = _ordered_candidates(finalist_candidates, pair_ids)
    if not (
        finalist_pairs["a_share_replay_status"].astype(str)
        == "PAIR_REPLAY_COMPLETE"
    ).all():
        raise RuntimeError("replay-blocked pair entered OOS freeze")
    if not (
        finalist_pairs["a_share_executable_net_increment"].astype(float)
        > 0.0
    ).all():
        raise RuntimeError("nonpositive train finalist entered OOS freeze")
    candidate_ids = ordered_candidates["candidate_id"].astype(str).tolist()
    if not set(candidate_ids).issubset(
        set(map(str, source_freeze["candidate_ids"]))
    ):
        raise RuntimeError("finalist candidates are not in source replay freeze")
    if not set(pair_ids).issubset(set(map(str, source_freeze["pair_ids"]))):
        raise RuntimeError("finalist pair is not in source replay freeze")

    source_pair_results = pd.read_parquet(
        strict_replay_root / "pair_replay_results.parquet"
    ).where(pd.notna, None)
    source_candidate_results = pd.read_parquet(
        strict_replay_root / "candidate_replay_results.parquet"
    ).where(pd.notna, None)
    replay_pairs = source_pair_results[
        source_pair_results["pair_id"].astype(str).isin(pair_ids)
    ].copy()
    replay_candidates = source_candidate_results[
        source_candidate_results["candidate_id"].astype(str).isin(candidate_ids)
    ].copy()
    if len(replay_pairs) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("source replay pair evidence count drift")
    if len(replay_candidates) != EXPECTED_MEMBER_COUNT:
        raise RuntimeError("source replay candidate evidence count drift")
    if str(replay_pairs.iloc[0]["a_share_replay_status"]) != (
        "PAIR_REPLAY_COMPLETE"
    ):
        raise RuntimeError("source replay evidence is not complete")
    if float(replay_pairs.iloc[0]["a_share_executable_net_increment"]) <= 0:
        raise RuntimeError("source replay evidence is not train-positive")

    prepared_root = output_root / "prepared"
    derived_replay_root = output_root / "derived_replay_evidence"
    prepared_root.mkdir(parents=True)
    derived_replay_root.mkdir(parents=True)
    candidate_path = prepared_root / "finalist_candidates.parquet"
    candidate_table_path = prepared_root / "finalist_stock_session_candidates.csv"
    pair_path = prepared_root / "finalist_pairs.parquet"
    ordered_candidates.to_parquet(candidate_path, index=False)
    ordered_candidates.to_csv(candidate_table_path, index=False)
    finalist_pairs.to_parquet(pair_path, index=False)

    selection_hash = str(manifest["selection_payload_sha256"])
    protected_source_hashes = dict(
        source_execution_contract["protected_source_hashes"]
    )
    for path in (
        finalist_root / "finalist_manifest.json",
        finalist_root / "finalist_pairs.parquet",
        finalist_root / "finalist_candidates.parquet",
        source_replay_closure_path,
        source_freeze_path,
        source_contract_path,
    ):
        protected_source_hashes[str(path.resolve())] = _sha256(path)

    execution_contract = {
        key: value
        for key, value in source_execution_contract.items()
        if key
        not in {
            "schema_version",
            "status",
            "repo_sha",
            "selection_payload_sha256",
            "pair_count",
            "candidate_member_count",
            "route_counts",
            "sequence",
            "research_replay_authorized",
            "report_only_oos_authorized",
            "protected_source_hashes",
            "contract_payload_sha256",
        }
    }
    execution_contract.update(
        {
            "schema_version": "cn_single_train_finalist_report_only_oos_contract_v1",
            "status": "ACTIVE_FROZEN_TRAIN_FINALIST_REPORT_ONLY_OOS",
            "repo_sha": generator_repo_sha,
            "selection_payload_sha256": selection_hash,
            "source_replay_selection_payload_sha256": str(
                source_freeze["selection_payload_sha256"]
            ),
            "pair_count": EXPECTED_PAIR_COUNT,
            "candidate_member_count": EXPECTED_MEMBER_COUNT,
            "route_counts": {
                str(route): int(count // 2)
                for route, count in ordered_candidates["route_id"]
                .value_counts()
                .items()
            },
            "sequence": [
                "REUSE_IMMUTABLE_A_SHARE_TRAIN_REPLAY_EVIDENCE",
                "UNCHANGED_SINGLE_PAIR_REPORT_ONLY_VALIDATION",
            ],
            "interstage_filtering": "FORBIDDEN",
            "cohort_mutation": "FORBIDDEN",
            "research_replay_authorized": False,
            "train_replay_recomputed": False,
            "report_only_oos_authorized": True,
            "economic_claim_authorized": False,
            "protected_source_hashes": protected_source_hashes,
        }
    )
    execution_contract["contract_payload_sha256"] = _stable_hash(
        execution_contract
    )
    execution_contract_path = _write_json(
        prepared_root / "replay_then_oos_execution_contract.json",
        execution_contract,
    )

    replay_pair_path = derived_replay_root / "pair_replay_results.parquet"
    replay_candidate_path = (
        derived_replay_root / "candidate_replay_results.parquet"
    )
    replay_pairs.to_parquet(replay_pair_path, index=False)
    replay_candidates.to_parquet(replay_candidate_path, index=False)
    derived_replay_closure = {
        "schema_version": "cn_single_finalist_derived_replay_evidence_v1",
        "status": "A_SHARE_REPLAY_CLOSED_IMMUTABLE",
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "selection_payload_sha256": selection_hash,
        "source_selection_payload_sha256": str(
            source_freeze["selection_payload_sha256"]
        ),
        "source_replay_closure": str(source_replay_closure_path),
        "source_replay_closure_sha256": _sha256(source_replay_closure_path),
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "train_reads": int(source_replay_closure["train_reads"]),
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "artifacts": [
            _artifact(replay_pair_path),
            _artifact(replay_candidate_path),
            _artifact(source_replay_closure_path),
        ],
    }
    derived_replay_closure["manifest_body_sha256"] = _stable_hash(
        derived_replay_closure
    )
    derived_replay_closure_path = _write_json(
        derived_replay_root / "REPLAY_COMPLETE.json",
        derived_replay_closure,
    )

    freeze = {
        "schema_version": "cn_finalist_replay_then_oos_freeze_v1",
        "status": "FROZEN_UNCHANGED_FIXED_COHORT",
        "repo_sha": generator_repo_sha,
        "selection_payload_sha256": selection_hash,
        "source_selection_payload_sha256": str(
            source_freeze["selection_payload_sha256"]
        ),
        "pair_count": EXPECTED_PAIR_COUNT,
        "candidate_member_count": EXPECTED_MEMBER_COUNT,
        "candidate_ids": candidate_ids,
        "pair_ids": pair_ids,
        "candidate_artifact": _artifact(candidate_path, root=output_root),
        "candidate_table_artifact": _artifact(
            candidate_table_path, root=output_root
        ),
        "pair_artifact": _artifact(pair_path, root=output_root),
        "execution_contract_artifact": _artifact(
            execution_contract_path, root=output_root
        ),
        "derived_replay_closure_artifact": _artifact(
            derived_replay_closure_path, root=output_root
        ),
        "protected_source_hashes": protected_source_hashes,
        "interstage_filtering": "FORBIDDEN",
        "train_replay_recomputed": False,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    freeze["manifest_body_sha256"] = _stable_hash(freeze)
    freeze_path = _write_json(
        prepared_root / "finalist_replay_then_oos_freeze.json", freeze
    )

    closure = {
        "schema_version": "cn_single_train_finalist_oos_prepare_closure_v1",
        "status": "SINGLE_TRAIN_FINALIST_REPORT_ONLY_OOS_PREPARED",
        "prepared_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo_sha": generator_repo_sha,
        "finalist_root": str(finalist_root),
        "finalist_manifest_sha256": _sha256(
            finalist_root / "finalist_manifest.json"
        ),
        "selection_payload_sha256": selection_hash,
        "pair_ids": pair_ids,
        "candidate_ids": candidate_ids,
        "source_replay_closure_sha256": _sha256(source_replay_closure_path),
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "artifacts": [
            _artifact(path)
            for path in (
                candidate_path,
                candidate_table_path,
                pair_path,
                execution_contract_path,
                derived_replay_closure_path,
                freeze_path,
            )
        ],
    }
    closure["manifest_body_sha256"] = _stable_hash(closure)
    closure_path = _write_json(
        output_root / "FINALIST_REPORT_ONLY_OOS_PREPARED.json", closure
    )
    return {
        "status": closure["status"],
        "closure_path": str(closure_path),
        "closure_sha256": _sha256(closure_path),
        "freeze_path": str(freeze_path),
        "freeze_sha256": _sha256(freeze_path),
        "derived_replay_root": str(derived_replay_root),
        "selection_payload_sha256": selection_hash,
        "pair_ids": pair_ids,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }


def verify(*, output_root: Path, receipt_path: Path) -> dict[str, Any]:
    output_root = Path(output_root).resolve()
    receipt_path = Path(receipt_path).resolve()
    closure_path = output_root / "FINALIST_REPORT_ONLY_OOS_PREPARED.json"
    closure = _read_json(closure_path)
    _verify_payload_hash(
        closure,
        field="manifest_body_sha256",
        label="single-finalist OOS preparation closure",
    )
    _verify_declared_artifacts(closure, root=output_root)
    if str(closure.get("status") or "") != (
        "SINGLE_TRAIN_FINALIST_REPORT_ONLY_OOS_PREPARED"
    ):
        raise RuntimeError("single-finalist OOS preparation is not closed")
    _verify_zero_sealed_reads(closure, label="OOS preparation closure")
    _verify_forbidden_writes(closure, label="OOS preparation closure")

    freeze_path = (
        output_root / "prepared" / "finalist_replay_then_oos_freeze.json"
    )
    freeze = _read_json(freeze_path)
    _verify_payload_hash(
        freeze,
        field="manifest_body_sha256",
        label="single-finalist OOS freeze",
    )
    if int(freeze.get("pair_count") or 0) != EXPECTED_PAIR_COUNT:
        raise RuntimeError("single-finalist OOS freeze pair count drift")
    if int(freeze.get("candidate_member_count") or 0) != (
        EXPECTED_MEMBER_COUNT
    ):
        raise RuntimeError("single-finalist OOS freeze member count drift")
    if bool(freeze.get("train_replay_recomputed")):
        raise RuntimeError("train replay was recomputed in OOS preparation")
    _verify_zero_sealed_reads(freeze, label="single-finalist OOS freeze")
    _verify_forbidden_writes(freeze, label="single-finalist OOS freeze")
    for path, expected in freeze["protected_source_hashes"].items():
        if _sha256(Path(path)) != str(expected):
            raise RuntimeError(f"protected source hash drift: {path}")

    candidate_path = _resolved_artifact(
        output_root, freeze["candidate_artifact"]
    )
    pair_path = _resolved_artifact(output_root, freeze["pair_artifact"])
    candidates = pd.read_parquet(candidate_path)
    pairs = pd.read_parquet(pair_path)
    if candidates["candidate_id"].astype(str).tolist() != list(
        map(str, freeze["candidate_ids"])
    ):
        raise RuntimeError("single-finalist candidate order drift")
    if pairs["pair_id"].astype(str).tolist() != list(
        map(str, freeze["pair_ids"])
    ):
        raise RuntimeError("single-finalist pair order drift")

    replay_root = output_root / "derived_replay_evidence"
    replay_closure_path = replay_root / "REPLAY_COMPLETE.json"
    replay_closure = _read_json(replay_closure_path)
    _verify_payload_hash(
        replay_closure,
        field="manifest_body_sha256",
        label="derived replay evidence closure",
    )
    _verify_declared_artifacts(replay_closure, root=replay_root)
    if bool(replay_closure.get("financial_result_recomputed")):
        raise RuntimeError("derived replay evidence recomputed finance")
    if str(replay_closure["selection_payload_sha256"]) != str(
        freeze["selection_payload_sha256"]
    ):
        raise RuntimeError("derived replay/freeze selection drift")

    receipt = {
        "schema_version": "cn_single_train_finalist_oos_preflight_verification_v1",
        "status": "PASS_ZERO_FINANCIAL_SINGLE_PAIR_OOS_FREEZE",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "preparation_closure_sha256": _sha256(closure_path),
        "freeze_sha256": _sha256(freeze_path),
        "derived_replay_closure_sha256": _sha256(replay_closure_path),
        "selection_payload_sha256": str(
            freeze["selection_payload_sha256"]
        ),
        "pair_ids": list(map(str, freeze["pair_ids"])),
        "candidate_ids": list(map(str, freeze["candidate_ids"])),
        "financial_result_recomputed": False,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    receipt["receipt_payload_sha256"] = _stable_hash(receipt)
    _write_json(receipt_path, receipt)
    return {
        "status": receipt["status"],
        "receipt_path": str(receipt_path),
        "receipt_sha256": _sha256(receipt_path),
        "receipt_payload_sha256": receipt["receipt_payload_sha256"],
        "selection_payload_sha256": receipt["selection_payload_sha256"],
        "pair_ids": receipt["pair_ids"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--finalist-root", type=Path, required=True)
    prepare_parser.add_argument("--output-root", type=Path, required=True)
    prepare_parser.add_argument("--repo-sha", required=True)
    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--output-root", type=Path, required=True)
    verify_parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(
            finalist_root=args.finalist_root,
            output_root=args.output_root,
            generator_repo_sha=args.repo_sha,
        )
    else:
        result = verify(output_root=args.output_root, receipt_path=args.receipt)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
