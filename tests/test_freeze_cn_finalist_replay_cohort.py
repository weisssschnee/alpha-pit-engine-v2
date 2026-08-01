from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.freeze_cn_finalist_replay_cohort import freeze_replay_cohort
from scripts.freeze_cn_productive_keep_review_cohort import (
    _artifact,
    _payload_sha256,
    _sha256,
    _write_json,
)


def _write_parent(root: Path) -> None:
    root.mkdir()
    rows = []
    members = []
    for index in range(8):
        pair_id = f"pair-{index:02d}"
        rows.append(
            {
                "pair_id": pair_id,
                "keep_review_rank": index + 1,
                "train_stability_score": 1.0 - index * 0.05,
                "train_stability_floor": 0.9 - index * 0.04,
                "train_stability_median": 0.8 - index * 0.03,
                "search_score": 0.7 - index * 0.02,
                "economic_mechanism_id": f"mechanism-{index}",
                "portfolio_exposure_family_id": (
                    "exposure-a" if index < 4 else f"exposure-{index}"
                ),
            }
        )
        for role in ("primary", "control"):
            members.append(
                {
                    "pair_id": pair_id,
                    "candidate_id": f"{pair_id}-{role}",
                    "pair_member_role": role,
                    "keep_review_rank": index + 1,
                }
            )
    pairs = pd.DataFrame(rows)
    candidates = pd.DataFrame(members)
    contract = {
        "schema_version": "cn_productive_keep_review_freeze_v3",
        "status": "FROZEN_DEVELOPMENT_KEEP_REVIEW_ONLY",
        "selection_mode": "finalist_funnel_v1",
        "cohort_pairs": 8,
        "cohort_candidate_members": 16,
    }
    contract["contract_payload_sha256"] = _payload_sha256(contract)
    contract_path = _write_json(root / "keep_review_contract.json", contract)
    review_path = root / "productive_review_ledger.parquet"
    pair_path = root / "keep_review_pairs.parquet"
    candidate_path = root / "keep_review_candidates.parquet"
    summary_path = _write_json(
        root / "keep_review_summary.json", {"selected_pairs": 8}
    )
    pairs.to_parquet(review_path, index=False)
    pairs.to_parquet(pair_path, index=False)
    candidates.to_parquet(candidate_path, index=False)
    manifest = {
        "schema_version": "cn_productive_keep_review_freeze_v3",
        "status": "KEEP_REVIEW_COHORT_CLOSED_IMMUTABLE",
        "selection_payload_sha256": "parent-selection",
        "contract_payload_sha256": contract["contract_payload_sha256"],
        "artifacts": [
            _artifact(path, root=root)
            for path in (
                contract_path,
                review_path,
                pair_path,
                candidate_path,
                summary_path,
            )
        ],
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    _write_json(root / "keep_review_manifest.json", manifest)


def _write_funnel_contract(path: Path) -> None:
    payload = {
        "schema_version": "cn_train_only_finalist_funnel_contract_v1",
        "status": "FROZEN_USER_AUTHORIZED_TRAIN_ONLY_FUNNEL",
        "review_pool": {"target_pairs": 8},
        "replay_stage": {"input_pairs": 4},
    }
    payload["contract_payload_sha256"] = _payload_sha256(payload)
    _write_json(path, payload)


def test_freeze_replay_cohort_applies_exposure_cap_and_preserves_members(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "parent"
    output = tmp_path / "output"
    funnel = tmp_path / "funnel.json"
    _write_parent(parent)
    _write_funnel_contract(funnel)

    result = freeze_replay_cohort(
        parent_review_root=parent,
        output_root=output,
        funnel_contract_path=funnel,
        pair_count=4,
    )

    assert result["selected_pairs"] == 4
    assert result["selected_candidate_members"] == 8
    assert result["selected_economic_mechanism_unique"] == 4
    pairs = pd.read_parquet(output / "keep_review_pairs.parquet")
    assert pairs["pair_id"].tolist() == [
        "pair-00",
        "pair-04",
        "pair-05",
        "pair-06",
    ]
    assert pairs["portfolio_exposure_family_id"].value_counts().max() == 1
    candidates = pd.read_parquet(output / "keep_review_candidates.parquet")
    assert len(candidates) == 8
    manifest_path = output / "keep_review_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    declared = dict(manifest)
    expected = declared.pop("manifest_payload_sha256")
    assert _payload_sha256(declared) == expected
    assert _sha256(manifest_path) == result["manifest_file_sha256"]
    assert manifest["validation_reads"] == 0
    assert manifest["holdout_reads"] == 0
    assert manifest["forward_2026_reads"] == 0
