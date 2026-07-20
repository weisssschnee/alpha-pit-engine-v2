from __future__ import annotations

import hashlib
import json

import pandas as pd

from scripts.prepare_cn_phase3cm_session_reuse_source import (
    MANIFEST_NAME,
    prepare_reuse_source,
)


def _sha(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_prepare_reuse_source_preserves_exact_augmented_shards(tmp_path) -> None:
    base_root = tmp_path / "base"
    augmented_root = tmp_path / "augmented"
    output_root = tmp_path / "reuse"
    base_root.mkdir()
    augmented_root.mkdir()
    base_shards = []
    augmented_shards = []
    for shard in range(16):
        frame = pd.DataFrame(
            {
                "trade_time": pd.to_datetime(["2025-01-02 15:00"]),
                "code": [f"{shard:06d}.SZ"],
                "source_shard": [shard],
                "source_row_identity": [0],
                "duplicate_ordinal": [0],
                "old_field": [float(shard)],
            }
        )
        base_file = base_root / f"shard_{shard:02d}.parquet"
        frame.to_parquet(base_file, index=False)
        augmented = frame.assign(fund_field=float(shard + 1))
        augmented_file = augmented_root / f"shard_{shard:02d}.parquet"
        augmented.to_parquet(augmented_file, index=False)
        base_shards.append(
            {
                "source_shard": shard,
                "output_path": str(base_file),
                "output_sha256": _sha(base_file),
                "rows": 1,
            }
        )
        receipt = {
            "schema_version": "cn_phase3cm_session_sidecar_augmentation_v1",
            "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
            "shard_index": shard,
            "source": {"path": str(base_file), "sha256": _sha(base_file), "rows": 1},
            "output": {
                "path": str(augmented_file),
                "sha256": _sha(augmented_file),
                "bytes": augmented_file.stat().st_size,
            },
            "source_stable_key_digest": f"key-{shard}",
            "source_payload_digest": f"payload-{shard}",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
        augmented_shards.append(receipt)
        (augmented_root / f"shard_{shard:02d}.augmentation.json").write_text(
            json.dumps(receipt), encoding="utf-8"
        )

    base_manifest = base_root / MANIFEST_NAME
    base_manifest.write_text(
        json.dumps(
            {
                "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
                "data_role": "development_train_only",
                "split_manifest_hash": "a" * 64,
                "eligible_train_date_count": 1,
                "thread_environment": {},
                "shards": base_shards,
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            }
        ),
        encoding="utf-8",
    )
    augmentation_manifest = augmented_root / "CN_SESSION_SIDECAR_AUGMENTATION_MANIFEST.json"
    augmentation_manifest.write_text(
        json.dumps(
            {
                "status": "SESSION_SIDECAR_AUGMENTATION_PARITY_PASS",
                "shards": augmented_shards,
                "validation_reads": 0,
                "holdout_reads": 0,
                "forward_2026_reads": 0,
            }
        ),
        encoding="utf-8",
    )

    receipt = prepare_reuse_source(
        base_manifest_path=base_manifest,
        augmentation_manifest_path=augmentation_manifest,
        output_root=output_root,
    )

    assert receipt["status"] == "CN_PHASE3CM_SESSION_REUSE_SOURCE_READY"
    assert receipt["shard_count"] == 16
    layout = json.loads((output_root / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert layout["status"] == "TIME_MAJOR_LAYOUT_PARITY_PASS"
    assert layout["sidecar_rows"] == 16
    assert "fund_field" in layout["fields"]
    for shard in range(16):
        assert _sha(output_root / f"shard_{shard:02d}.parquet") == _sha(
            augmented_root / f"shard_{shard:02d}.parquet"
        )
