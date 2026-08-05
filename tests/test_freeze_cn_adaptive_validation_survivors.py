from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_adaptive_validation_survivors import freeze_survivors
from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)
from scripts.verify_cn_adaptive_validation_survivors import verify_survivors


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    train = tmp_path / "train"
    oos = tmp_path / "oos"
    train.mkdir()
    oos.mkdir()
    pairs = pd.DataFrame(
        [
            {
                "finalist_order": index,
                "pair_id": f"pair-{index:02d}",
                "primary_candidate_id": f"p-{index:02d}",
                "control_candidate_id": f"c-{index:02d}",
                "decoder_id": "TOPK_10_EQUAL",
            }
            for index in range(1, 23)
        ]
    )
    candidates = pd.DataFrame(
        [
            {
                "candidate_id": row[f"{role.lower()}_candidate_id"],
                "pair_id": row["pair_id"],
                "pair_member_role": role.upper(),
            }
            for row in pairs.to_dict(orient="records")
            for role in ("primary", "control")
        ]
    )
    pair_path = train / "finalist_pairs.parquet"
    candidate_path = train / "finalist_candidates.parquet"
    pairs.to_parquet(pair_path, index=False)
    candidates.to_parquet(candidate_path, index=False)
    train_manifest = {
        "status": "DECODER_V2_TRAIN_ONLY_FINALIST_FREEZE_CLOSED_IMMUTABLE",
        "selected_pairs": 22,
        "selected_candidate_members": 44,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "artifacts": [
            _artifact(pair_path, root=train),
            _artifact(candidate_path, root=train),
        ],
    }
    train_manifest["manifest_payload_sha256"] = _payload_sha256(train_manifest)
    train_manifest_path = _write_json(train / "finalist_manifest.json", train_manifest)

    survivor_orders = {2, 6, 7, 8, 9, 10, 11, 15, 18, 21}
    oos_pairs = pairs.assign(
        decoder_policy_sha256="d" * 64,
        all_four_economic_gates_positive=pairs["finalist_order"].isin(survivor_orders),
    )
    oos_pair_path = oos / "oos_pair_metrics.parquet"
    oos_pairs.to_parquet(oos_pair_path, index=False)
    oos_closure = {
        "status": "DECODER_V2_TOPK10_EQUAL_REPORT_ONLY_OOS_CLOSED_IMMUTABLE",
        "pair_count": 22,
        "candidate_member_count": 44,
        "decoder_id": "TOPK_10_EQUAL",
        "interstage_filter_applied": False,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
        "artifacts": [_artifact(oos_pair_path, root=oos)],
    }
    oos_closure["manifest_body_sha256"] = _payload_sha256(oos_closure)
    oos_closure_path = _write_json(oos / "OOS_COMPLETE.json", oos_closure)

    split = tmp_path / "split.csv"
    with split.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["trade_date", "split", "optimizer_usage"],
        )
        writer.writeheader()
        for index in range(48):
            writer.writerow(
                {
                    "trade_date": f"2025-12-{index + 1:02d}",
                    "split": "holdout",
                    "optimizer_usage": "report_only",
                }
            )
    return train, oos, split


def test_freeze_is_exact_ordered_no_backfill_and_independently_verified(
    tmp_path: Path,
) -> None:
    train, oos, split = _fixture(tmp_path)
    output = tmp_path / "confirmation"
    result = freeze_survivors(
        train_finalist_root=train,
        oos_root=oos,
        split_manifest=split,
        output_root=output,
        generator_repo_sha="a" * 40,
        source_train_authority_root="D:\\train",
        source_oos_authority_root="D:\\oos",
        expected_train_manifest_sha256=_sha256(train / "finalist_manifest.json"),
        expected_oos_closure_sha256=_sha256(oos / "OOS_COMPLETE.json"),
    )
    assert result["selected_pair_ids"] == [
        "pair-02",
        "pair-06",
        "pair-07",
        "pair-08",
        "pair-09",
        "pair-10",
        "pair-11",
        "pair-15",
        "pair-18",
        "pair-21",
    ]
    summary = json.loads(
        (output / "confirmation_summary.json").read_text(encoding="utf-8")
    )
    assert summary["blocked_rows_backfilled"] == 0
    assert summary["selector_frozen"] is False
    assert summary["holdout_market_or_label_rows_read"] == 0
    verified = verify_survivors(
        confirmation_root=output,
        train_finalist_root=train,
        oos_root=oos,
        split_manifest=split,
        receipt_path=tmp_path / "receipt.json",
    )
    assert verified["status"] == (
        "PASS_INDEPENDENT_FIXED_SURVIVOR_CONFIRMATORY_FREEZE"
    )


def test_freeze_fails_closed_when_survivor_count_drifts(tmp_path: Path) -> None:
    train, oos, split = _fixture(tmp_path)
    metrics = pd.read_parquet(oos / "oos_pair_metrics.parquet")
    metrics.loc[metrics["finalist_order"].eq(1), "all_four_economic_gates_positive"] = True
    metrics.to_parquet(oos / "oos_pair_metrics.parquet", index=False)
    closure = json.loads((oos / "OOS_COMPLETE.json").read_text(encoding="utf-8"))
    closure["artifacts"] = [_artifact(oos / "oos_pair_metrics.parquet", root=oos)]
    closure.pop("manifest_body_sha256")
    closure["manifest_body_sha256"] = _payload_sha256(closure)
    _write_json(oos / "OOS_COMPLETE.json", closure)
    try:
        freeze_survivors(
            train_finalist_root=train,
            oos_root=oos,
            split_manifest=split,
            output_root=tmp_path / "confirmation",
            generator_repo_sha="b" * 40,
            source_train_authority_root="D:\\train",
            source_oos_authority_root="D:\\oos",
            expected_train_manifest_sha256=_sha256(train / "finalist_manifest.json"),
            expected_oos_closure_sha256=_sha256(oos / "OOS_COMPLETE.json"),
        )
    except RuntimeError as exc:
        assert "survivor count drift" in str(exc)
    else:
        raise AssertionError("expected survivor count drift failure")
