from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from scripts.build_cn_verified_partial_campaign_history import (
    _stable_hash,
    build_verified_history,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_checkpoint(root: Path, index: int, prior: str) -> str:
    checkpoint = root / "checkpoints" / f"checkpoint_{index:03d}"
    checkpoint.mkdir(parents=True)
    schedule = {
        "formal_fresh_exact_asks": 3,
        "routes": [
            {"route_id": "SLOW_TEMPORAL_CHANGE", "asked_pairs": 2},
            {"route_id": "FIRSTN_PATH", "asked_pairs": 1},
        ],
    }
    (checkpoint / "route_schedule.json").write_text(
        json.dumps(schedule), encoding="utf-8"
    )
    pd.DataFrame(
        [
            {"exact_identity": f"exact-{index}-a"},
            {"exact_identity": f"exact-{index}-b"},
        ]
    ).to_parquet(checkpoint / "candidate_attempts.parquet", index=False)
    pd.DataFrame(
        [
            {
                "pair_id": f"pair-{index}",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "behavior_status": "RESOLVED",
                "portfolio_behavior_signature_id": f"sig-{index}",
                "portfolio_behavior_family_id": f"family-{index}",
            }
        ]
    ).to_parquet(checkpoint / "full_behavior.parquet", index=False)
    paths = [
        checkpoint / "route_schedule.json",
        checkpoint / "candidate_attempts.parquet",
        checkpoint / "full_behavior.parquet",
    ]
    manifest = {
        "schema_version": "cn_iterative_search_v1_batch_manifest_v1",
        "batch_id": f"checkpoint_{index:03d}",
        "status": "BATCH_CLOSED_IMMUTABLE",
        "input_hashes": {"prior_checkpoint_manifest": prior},
        "artifacts": [
            {
                "path": path.name,
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for path in paths
        ],
        "access_receipts": [],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion": "FORBIDDEN",
        "evaluation_name": "test",
    }
    manifest["manifest_payload_hash"] = _stable_hash(manifest)
    manifest_path = checkpoint / "batch_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return _sha256(manifest_path)


def _write_base_archives(root: Path) -> tuple[Path, Path]:
    candidate = root / "base_candidate.parquet"
    behavior = root / "base_behavior.parquet"
    pd.DataFrame([{"exact_identity": "base-exact"}]).to_parquet(
        candidate, index=False
    )
    pd.DataFrame(
        [
            {
                "pair_id": "base-pair",
                "route_id": "SLOW_TEMPORAL_CHANGE",
                "behavior_status": "RESOLVED",
                "portfolio_behavior_signature_id": "base-sig",
                "portfolio_behavior_family_id": "base-family",
            }
        ]
    ).to_parquet(behavior, index=False)
    return candidate, behavior


def test_build_verified_partial_history_excludes_unclosed_next_checkpoint(
    tmp_path: Path,
) -> None:
    campaign = tmp_path / "campaign"
    prior = "GENESIS"
    prior = _write_checkpoint(campaign, 1, prior)
    _write_checkpoint(campaign, 2, prior)
    incomplete = campaign / "checkpoints" / "checkpoint_003"
    incomplete.mkdir(parents=True)
    (incomplete / "route_schedule.json").write_text("{}", encoding="utf-8")
    candidate, behavior = _write_base_archives(tmp_path)

    receipt = build_verified_history(
        campaign_root=campaign,
        base_candidate_archive=candidate,
        base_behavior_archive=behavior,
        output_root=tmp_path / "out",
        checkpoint_count=2,
        expected_route_mix={
            "SLOW_TEMPORAL_CHANGE": 2,
            "FIRSTN_PATH": 1,
        },
    )

    assert receipt["status"] == "ZERO_FINANCIAL_HISTORY_REFRESH_PASS"
    assert receipt["excluded_unclosed_checkpoint"] == "checkpoint_003"
    assert receipt["exact_identity_count"] == 5
    assert receipt["behavior_identity_row_count"] == 3
    assert receipt["reward_rows_imported"] == 0
    assert receipt["optimizer_state_imported"] is False


def test_build_verified_partial_history_rejects_closed_checkpoint_beyond_boundary(
    tmp_path: Path,
) -> None:
    campaign = tmp_path / "campaign"
    prior = _write_checkpoint(campaign, 1, "GENESIS")
    _write_checkpoint(campaign, 2, prior)
    candidate, behavior = _write_base_archives(tmp_path)

    try:
        build_verified_history(
            campaign_root=campaign,
            base_candidate_archive=candidate,
            base_behavior_archive=behavior,
            output_root=tmp_path / "out",
            checkpoint_count=1,
            expected_route_mix={
                "SLOW_TEMPORAL_CHANGE": 2,
                "FIRSTN_PATH": 1,
            },
        )
    except RuntimeError as exc:
        assert "boundary is stale" in str(exc)
    else:
        raise AssertionError("expected closed checkpoint boundary rejection")
