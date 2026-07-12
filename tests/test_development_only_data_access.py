from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from our_system_phase2.runtime.build_development_only_true1min_release import _table_hash, build_release
from our_system_phase2.services.development_only_data_access import (
    PANEL_RELATIVE_PATH,
    RELEASE_MANIFEST_VERSION,
    cache_provenance,
    canonical_json_hash,
    initialize_cache_root,
    read_development_panel,
    release_hash,
    schema_hash,
    sha256_file,
    validate_development_release,
)


def _frame(dates: list[str], *, code_offset: int = 0) -> pd.DataFrame:
    rows = []
    for day in dates:
        for code in range(code_offset, code_offset + 2):
            for minute in range(3):
                timestamp = pd.Timestamp(day) + pd.Timedelta(hours=9, minutes=30 + minute)
                rows.append(
                    {
                        "code": f"{code:06d}",
                        "trade_time": timestamp,
                        "date": timestamp,
                        "signal_time": timestamp,
                        "close": float(code + minute + 1),
                        "feature": float(minute),
                    }
                )
    return pd.DataFrame(rows)


def _write_groups(path: Path, groups: list[pd.DataFrame], *, write_statistics: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = pa.Table.from_pandas(groups[0], preserve_index=False).schema
    with pq.ParquetWriter(path, schema, compression="zstd", write_statistics=write_statistics) as writer:
        for group in groups:
            writer.write_table(pa.Table.from_pandas(group, schema=schema, preserve_index=False), row_group_size=len(group))


def _split(path: Path) -> None:
    pd.DataFrame(
        {
            "trade_date": ["2025-04-01", "2025-04-02", "2025-07-08", "2025-10-27"],
            "split": ["train", "train", "validation", "holdout"],
        }
    ).to_csv(path, index=False)


@pytest.fixture()
def built_release(tmp_path: Path) -> tuple[Path, Path, Path, dict]:
    source = tmp_path / "source"
    output = tmp_path / "development"
    split = tmp_path / "split.csv"
    source_manifest = tmp_path / "source_summary.json"
    _split(split)
    source_files = []
    for shard in range(2):
        path = source / f"shard_{shard:02d}" / PANEL_RELATIVE_PATH
        _write_groups(
            path,
            [
                _frame(["2025-04-01", "2025-07-08"], code_offset=shard * 10),
                _frame(["2025-04-02", "2025-10-27"], code_offset=shard * 10),
            ],
        )
        source_files.append({"output_panel": str(path.resolve())})
    source_file_manifest = tmp_path / "source_files.csv"
    pd.DataFrame(source_files).to_csv(source_file_manifest, index=False)
    source_manifest.write_text(
        json.dumps(
            {
                "release": "approved_2024_2025",
                "output_root": str(source.resolve()),
                "shard_count": 2,
                "panel_rel": str(PANEL_RELATIVE_PATH),
                "manifest": str(source_file_manifest.resolve()),
                "hard_rules": [
                    "ctx_* sidecars are previous-available only via source_date < exec_date",
                    "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute",
                    "original shard root is not modified",
                ],
            }
        ),
        encoding="utf-8",
    )
    manifest = build_release(
        source,
        output,
        split,
        source_manifest,
        release_id="test_development_release",
        expected_source_release_manifest_sha256=sha256_file(source_manifest),
        expected_source_file_manifest_sha256=sha256_file(source_file_manifest),
        expected_shards=2,
    )
    return output, output / "development_only_release_manifest.json", split, manifest


def test_development_only_release_passes_and_matches_source_train_subset(built_release) -> None:
    output, manifest_path, split, manifest = built_release
    validated = validate_development_release(
        output, manifest_path, split, expected_release_hash=manifest["release_hash"]
    )
    assert len(validated.files) == 2
    assert manifest["totals"]["rows"] == 24
    assert manifest["totals"]["excluded_non_development_rows"] == 24
    assert manifest["consistency_audit"]["source_train_subset_equals_output"] is True
    assert all(
        group["source_train_subset_content_sha256"] == group["output_content_sha256"]
        for file_row in manifest["files"]
        for group in file_row["row_groups"]
    )


def test_logical_table_hash_is_stable_across_null_and_nan_parquet_roundtrip(tmp_path: Path) -> None:
    table = pa.table(
        {
            "float_values": pa.array([1.0, float("nan"), None, -0.0], type=pa.float32()),
            "double_values": pa.array([None, float("nan"), 2.0, 0.0], type=pa.float64()),
            "text": pa.array(["a", None, "", "b"], type=pa.large_string()),
        }
    )
    path = tmp_path / "roundtrip.parquet"
    pq.write_table(table, path, compression="zstd")
    observed = pq.read_table(path)
    assert _table_hash(table) == _table_hash(observed)

def test_fail_closed_read_writes_zero_forbidden_access_ledger(built_release, tmp_path: Path) -> None:
    output, manifest_path, split, manifest = built_release
    validated = validate_development_release(output, manifest_path, split)
    ledger = tmp_path / "ledger.json"
    frame, entries = read_development_panel(
        validated,
        trade_date=pd.Timestamp("2025-04-01"),
        row_group_index=0,
        columns=["feature"],
        read_ledger_path=ledger,
        loader_sha="loader-test-sha",
    )
    payload = json.loads(ledger.read_text(encoding="utf-8"))

    assert len(frame) == 12
    assert len(entries) == 2
    assert payload["forbidden_file_open_count"] == 0
    assert payload["forbidden_row_group_read_count"] == 0
    assert payload["validation_rows_read"] == 0
    assert payload["holdout_rows_read"] == 0
    assert payload["forward_rows_read"] == 0


def test_actual_read_role_mapping_rejects_post_preflight_contamination(
    built_release, tmp_path: Path, monkeypatch
) -> None:
    output, manifest_path, split, _ = built_release
    validated = validate_development_release(output, manifest_path, split)
    original = pq.ParquetFile

    class ContaminatedParquet:
        def __init__(self, path):
            self.inner = original(path)
            self.metadata = self.inner.metadata
            self.schema_arrow = self.inner.schema_arrow

        def read_row_group(self, row_group_id, columns=None):
            table = self.inner.read_row_group(row_group_id, columns=columns)
            frame = table.to_pandas()
            offsets = pd.to_datetime(frame["trade_time"]).dt.time
            frame["trade_time"] = [
                pd.Timestamp.combine(pd.Timestamp("2025-07-08").date(), value) for value in offsets
            ]
            frame["date"] = frame["trade_time"]
            frame["signal_time"] = frame["trade_time"]
            return pa.Table.from_pandas(frame, schema=table.schema, preserve_index=False)

    monkeypatch.setattr(pq, "ParquetFile", ContaminatedParquet)
    ledger = tmp_path / "contaminated_ledger.json"
    with pytest.raises(PermissionError, match="forbidden roles"):
        read_development_panel(
            validated,
            trade_date=pd.Timestamp("2025-04-01"),
            row_group_index=0,
            columns=["feature"],
            read_ledger_path=ledger,
            loader_sha="loader-test-sha",
        )
    assert not ledger.exists()


def _manual_manifest(root: Path, split: Path, file_path: Path, *, row_groups: list[dict]) -> Path:
    parquet = pq.ParquetFile(file_path)
    payload = {
        "manifest_version": RELEASE_MANIFEST_VERSION,
        "release_id": "manual",
        "release_root": str(root.resolve()),
        "data_role": "development",
        "created_at": "2026-07-12T00:00:00+00:00",
        "split_manifest_path": str(split.resolve()),
        "split_manifest_sha256": sha256_file(split),
        "schema_sha256": schema_hash(parquet.schema_arrow),
        "file_count": 1,
        "files": [
            {
                "relative_path": file_path.relative_to(root).as_posix(),
                "data_role": "development",
                "rows": parquet.metadata.num_rows,
                "size": file_path.stat().st_size,
                "sha256": sha256_file(file_path),
                "row_groups": row_groups,
            }
        ],
    }
    payload["release_hash"] = release_hash(payload)
    path = root / "development_only_release_manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_mixed_validation_row_group_is_rejected_before_predicate_read(tmp_path: Path) -> None:
    root = tmp_path / "mixed"
    split = tmp_path / "split.csv"
    _split(split)
    file_path = root / "shard_00" / PANEL_RELATIVE_PATH
    mixed = _frame(["2025-04-01", "2025-07-08"])
    _write_groups(file_path, [mixed])
    manifest_path = _manual_manifest(
        root,
        split,
        file_path,
        row_groups=[
            {
                "row_group_id": 0,
                "rows": len(mixed),
                "min_trade_date": "2025-04-01",
                "max_trade_date": "2025-07-08",
                "data_role": "development",
            }
        ],
    )

    with pytest.raises(PermissionError, match="forbidden data roles"):
        validate_development_release(root, manifest_path, split)


def test_missing_row_group_date_metadata_is_rejected(tmp_path: Path) -> None:
    root = tmp_path / "no_stats"
    split = tmp_path / "split.csv"
    _split(split)
    file_path = root / "shard_00" / PANEL_RELATIVE_PATH
    frame = _frame(["2025-04-01"])
    _write_groups(file_path, [frame], write_statistics=False)
    manifest_path = _manual_manifest(
        root,
        split,
        file_path,
        row_groups=[
            {
                "row_group_id": 0,
                "rows": len(frame),
                "min_trade_date": "2025-04-01",
                "max_trade_date": "2025-04-01",
                "data_role": "development",
            }
        ],
    )

    with pytest.raises(ValueError, match="lacks trade_time min/max metadata"):
        validate_development_release(root, manifest_path, split)


def test_unknown_row_group_date_endpoint_is_rejected_without_partial_outputs(tmp_path: Path) -> None:
    root = tmp_path / "unknown_endpoint"
    split = tmp_path / "split.csv"
    _split(split)
    file_path = root / "shard_00" / PANEL_RELATIVE_PATH
    frame = _frame(["2025-04-03"])
    _write_groups(file_path, [frame])
    manifest_path = _manual_manifest(
        root,
        split,
        file_path,
        row_groups=[
            {
                "row_group_id": 0,
                "rows": len(frame),
                "min_trade_date": "2025-04-03",
                "max_trade_date": "2025-04-03",
                "data_role": "development",
            }
        ],
    )
    candidate_output = tmp_path / "candidate_proposals.csv"
    with pytest.raises(ValueError, match="endpoints are absent"):
        validate_development_release(root, manifest_path, split)
    assert not candidate_output.exists()


def test_mixed_root_is_rejected_before_any_parquet_open(built_release, monkeypatch) -> None:
    output, manifest_path, split, _ = built_release
    extra = output / "validation" / "leak.parquet"
    _write_groups(extra, [_frame(["2025-07-08"])])
    opened = 0
    original = pq.ParquetFile

    def counting_open(*args, **kwargs):
        nonlocal opened
        opened += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(pq, "ParquetFile", counting_open)
    with pytest.raises(PermissionError, match="mixed or incomplete release root"):
        validate_development_release(output, manifest_path, split)
    assert opened == 0


def test_cache_provenance_mismatch_and_legacy_cache_are_rejected(tmp_path: Path) -> None:
    expected = cache_provenance(
        release_hash_value="release-a",
        split_manifest_sha256="split-a",
        loader_code_hash="loader-a",
        field_registry_hash="fields-a",
        data_role="development",
        materializer_hash="materializer-a",
    )
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / "legacy.bin").write_bytes(b"invalidated")
    with pytest.raises(PermissionError, match="fresh cache root"):
        initialize_cache_root(cache, expected, require_fresh=True)

    clean = tmp_path / "clean"
    initialize_cache_root(clean, expected, require_fresh=False)
    changed = dict(expected)
    changed["release_hash"] = "release-b"
    with pytest.raises(PermissionError, match="provenance mismatch"):
        initialize_cache_root(clean, changed, require_fresh=False)


def test_release_read_is_shard_order_invariant(built_release, tmp_path: Path) -> None:
    output, manifest_path, split, _ = built_release
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["files"] = list(reversed(payload["files"]))
    payload["release_hash"] = release_hash(payload)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    validated = validate_development_release(output, manifest_path, split)
    first, _ = read_development_panel(
        validated,
        trade_date=pd.Timestamp("2025-04-01"),
        row_group_index=0,
        columns=["feature"],
        read_ledger_path=tmp_path / "ledger.json",
        loader_sha="loader-test-sha",
    )
    assert first.equals(first.sort_values(["code", "trade_time"], kind="mergesort").reset_index(drop=True))


def test_builder_rejects_source_manifest_bound_to_another_root(tmp_path: Path) -> None:
    source = tmp_path / "source"
    split = tmp_path / "split.csv"
    _split(split)
    panel = source / "shard_00" / PANEL_RELATIVE_PATH
    _write_groups(panel, [_frame(["2025-04-01"])])
    files = tmp_path / "files.csv"
    pd.DataFrame({"output_panel": [str(panel.resolve())]}).to_csv(files, index=False)
    summary = tmp_path / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "output_root": str((tmp_path / "different").resolve()),
                "shard_count": 1,
                "panel_rel": str(PANEL_RELATIVE_PATH),
                "manifest": str(files.resolve()),
                "hard_rules": [
                    "ctx_* sidecars are previous-available only via source_date < exec_date",
                    "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute",
                    "original shard root is not modified",
                ],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(PermissionError, match="escapes its approved source root"):
        build_release(
            source,
            tmp_path / "out",
            split,
            summary,
            release_id="bad",
            expected_source_release_manifest_sha256=sha256_file(summary),
            expected_source_file_manifest_sha256=sha256_file(files),
            expected_shards=1,
        )


def test_checkpoint_is_invalidated_when_split_changes(built_release) -> None:
    output, _, split, first = built_release
    source = Path(first["source_root"])
    source_summary = Path(first["source_release_manifest"])
    changed = pd.read_csv(split)
    changed.loc[changed["trade_date"].eq("2025-04-02"), "split"] = "validation"
    changed.to_csv(split, index=False)
    second = build_release(
        source,
        output,
        split,
        source_summary,
        release_id="test_development_release_changed_split",
        expected_source_release_manifest_sha256=sha256_file(source_summary),
        expected_source_file_manifest_sha256=sha256_file(Path(first["source_file_manifest"])),
        expected_shards=2,
    )
    assert second["totals"]["rows"] == 12
    assert second["files"][0]["build_provenance"]["split_manifest_sha256"] == sha256_file(split)


def test_parallel_shard_builder_matches_single_worker_output(built_release, tmp_path: Path) -> None:
    _, _, split, single = built_release
    source = Path(single["source_root"])
    source_summary = Path(single["source_release_manifest"])
    source_files = Path(single["source_file_manifest"])
    parallel = build_release(
        source,
        tmp_path / "parallel",
        split,
        source_summary,
        release_id="parallel_test",
        expected_source_release_manifest_sha256=sha256_file(source_summary),
        expected_source_file_manifest_sha256=sha256_file(source_files),
        expected_shards=2,
        workers=2,
    )
    assert parallel["totals"] == single["totals"]
    assert [row["sha256"] for row in parallel["files"]] == [row["sha256"] for row in single["files"]]


def test_builder_refuses_2026_split_before_release_output(tmp_path: Path) -> None:
    source = tmp_path / "source"
    panel = source / "shard_00" / PANEL_RELATIVE_PATH
    _write_groups(panel, [_frame(["2025-04-01"])])
    split = tmp_path / "split.csv"
    pd.DataFrame({"trade_date": ["2025-04-01", "2026-01-02"], "split": ["train", "train"]}).to_csv(split, index=False)
    files = tmp_path / "files.csv"
    pd.DataFrame({"output_panel": [str(panel.resolve())]}).to_csv(files, index=False)
    summary = tmp_path / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "output_root": str(source.resolve()),
                "shard_count": 1,
                "panel_rel": str(PANEL_RELATIVE_PATH),
                "manifest": str(files.resolve()),
                "hard_rules": [
                    "ctx_* sidecars are previous-available only via source_date < exec_date",
                    "evt_uplimit_* sidecars are same-day but hidden until trade_time >= cutoff minute",
                    "original shard root is not modified",
                ],
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "out"
    with pytest.raises(PermissionError, match="outside 2024-2025"):
        build_release(
            source,
            output,
            split,
            summary,
            release_id="no_2026",
            expected_source_release_manifest_sha256=sha256_file(summary),
            expected_source_file_manifest_sha256=sha256_file(files),
            expected_shards=1,
        )
    assert not (output / "development_only_release_manifest.json").exists()
