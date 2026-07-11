from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import pandas as pd
import pytest

from our_system_phase2.services.pit_group_release import (
    PITGroupReleaseSpec,
    build_interval_release,
    build_snapshot_release,
    load_verified_release,
    write_release,
)


def _spec(*, group_type: str = "industry", membership_policy: str = "single") -> PITGroupReleaseSpec:
    return PITGroupReleaseSpec(
        source_name="licensed_vendor",
        source_version="2025-12-31",
        source_uri="baidu-share:user-supplied",
        source_artifact_sha256="a" * 64,
        retrieved_at="2026-07-11T12:00:00+08:00",
        group_type=group_type,
        membership_policy=membership_policy,
        required_start="2024-01-01 09:30:00",
        required_end="2025-12-31 23:59:59",
        maximum_observable_time="2025-12-31 23:59:59",
    )


def test_interval_release_is_order_invariant_and_normalizes_cn_codes() -> None:
    source = pd.DataFrame(
        {
            "code": ["SZ000001", "600000.SH", "SZ000001"],
            "group_id": ["I1", "I1", "I2"],
            "effective_from": ["2023-01-01", "2023-01-01", "2025-03-01"],
            "effective_to": ["2025-03-01", None, None],
            "source_observed_at": ["2023-01-01", "2023-01-01", "2025-03-01"],
        }
    )

    first, first_manifest = build_interval_release(source, _spec())
    second, second_manifest = build_interval_release(source.iloc[::-1], _spec())

    assert first["code"].tolist() == ["000001", "000001", "600000"]
    assert first_manifest["membership_sha256"] == second_manifest["membership_sha256"]
    assert first_manifest["coverage_gate"] == "PASS"
    assert first_manifest["reward_or_performance_used"] is False


def test_snapshot_release_derives_observable_intervals_without_lookahead() -> None:
    snapshots = pd.DataFrame(
        {
            "code": ["000001", "600000", "000001", "600000", "000001", "600000"],
            "group_id": ["I1", "I1", "I1", "I1", "I2", "I1"],
            "snapshot_at": [
                "2024-01-01", "2024-01-01", "2025-01-01", "2025-01-01", "2025-12-31 23:59:59", "2025-12-31 23:59:59",
            ],
            "source_observed_at": [
                "2024-01-01 08:00", "2024-01-01 08:00", "2025-01-02", "2025-01-02", "2025-12-31 23:59:59", "2025-12-31 23:59:59",
            ],
        }
    )

    release, manifest = build_snapshot_release(snapshots, _spec())

    changed = release[(release["code"] == "000001") & (release["group_id"] == "I2")].iloc[0]
    assert changed["effective_from"] == pd.Timestamp("2025-12-31 23:59:59")
    assert changed["source_observed_at"] == pd.Timestamp("2025-12-31 23:59:59")
    assert manifest["input_mode"] == "historical_snapshots"
    assert manifest["snapshot_count"] == 3


def test_single_current_snapshot_cannot_masquerade_as_historical_pit() -> None:
    current = pd.DataFrame(
        {
            "code": ["000001", "600000"],
            "group_id": ["I1", "I1"],
            "snapshot_at": ["2025-12-31", "2025-12-31"],
            "source_observed_at": ["2025-12-31", "2025-12-31"],
        }
    )

    with pytest.raises(ValueError, match="historical snapshots"):
        build_snapshot_release(current, _spec())


def test_backfilled_current_interval_snapshot_cannot_masquerade_as_history() -> None:
    backfilled = pd.DataFrame(
        {
            "code": ["000001", "600000"],
            "group_id": ["I1", "I1"],
            "effective_from": ["2020-01-01", "2020-01-01"],
            "effective_to": [None, None],
            "source_observed_at": ["2020-01-01", "2020-01-01"],
        }
    )

    with pytest.raises(ValueError, match="historical churn"):
        build_interval_release(backfilled, _spec())


@pytest.mark.parametrize("forbidden", ["reward", "validation_rank", "future_return", "label"])
def test_release_rejects_performance_or_evaluation_columns(forbidden: str) -> None:
    source = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "group_id": ["I1", "I2"],
            "effective_from": ["2024-01-01", "2025-01-01"],
            "effective_to": ["2025-01-01", None],
            "source_observed_at": ["2024-01-01", "2025-01-01"],
            forbidden: [0.0, 1.0],
        }
    )

    with pytest.raises(ValueError, match="performance/evaluation"):
        build_interval_release(source, _spec())


def test_release_rejects_observations_beyond_sealed_boundary() -> None:
    source = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "group_id": ["I1", "I2"],
            "effective_from": ["2024-01-01", "2025-01-01"],
            "effective_to": ["2025-01-01", None],
            "source_observed_at": ["2024-01-01", "2026-01-01"],
        }
    )

    with pytest.raises(ValueError, match="sealed boundary"):
        build_interval_release(source, _spec())


def test_caller_cannot_move_the_sealed_boundary_into_2026() -> None:
    source = pd.DataFrame(
        {
            "code": ["000001"],
            "group_id": ["I1"],
            "effective_from": ["2024-01-01"],
            "effective_to": [None],
            "source_observed_at": ["2024-01-01"],
        }
    )

    with pytest.raises(ValueError, match="2026 forward boundary is sealed"):
        build_interval_release(
            source,
            replace(_spec(), maximum_observable_time="2026-12-31"),
        )


def test_atomic_release_round_trip_verifies_canonical_hash(tmp_path: Path) -> None:
    source = pd.DataFrame(
        {
            "code": ["000001", "000001", "600000"],
            "group_id": ["I1", "I2", "I1"],
            "effective_from": ["2024-01-01", "2025-01-01", "2024-01-01"],
            "effective_to": ["2025-01-01", None, None],
            "source_observed_at": ["2024-01-01", "2025-01-01", "2024-01-01"],
        }
    )
    raw_source = tmp_path / "raw.csv"
    source.to_csv(raw_source, index=False)
    source_hash = hashlib.sha256(raw_source.read_bytes()).hexdigest()
    release, manifest = build_interval_release(
        source,
        replace(_spec(), source_artifact_sha256=source_hash),
    )

    paths = write_release(release, manifest, tmp_path / "release", source_path=raw_source)
    loaded, loaded_manifest = load_verified_release(
        paths["membership"], paths["manifest"], source_path=raw_source
    )

    assert loaded_manifest["membership_sha256"] == manifest["membership_sha256"]
    assert loaded_manifest["source_artifact_verified_at_write"] is True
    assert len(loaded_manifest["manifest_sha256"]) == 64
    pd.testing.assert_frame_equal(loaded, release)

    tampered = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    tampered["membership_sha256"] = "0" * 64
    paths["manifest"].write_text(json.dumps(tampered), encoding="utf-8")
    with pytest.raises(ValueError, match="manifest hash mismatch"):
        load_verified_release(paths["membership"], paths["manifest"])


def test_write_rejects_a_different_source_artifact(tmp_path: Path) -> None:
    source = pd.DataFrame(
        {
            "code": ["000001", "000001"],
            "group_id": ["I1", "I2"],
            "effective_from": ["2024-01-01", "2025-01-01"],
            "effective_to": ["2025-01-01", None],
            "source_observed_at": ["2024-01-01", "2025-01-01"],
        }
    )
    expected_source = tmp_path / "expected.csv"
    expected_source.write_text("expected", encoding="utf-8")
    different_source = tmp_path / "different.csv"
    different_source.write_text("different", encoding="utf-8")
    source_hash = hashlib.sha256(expected_source.read_bytes()).hexdigest()
    release, manifest = build_interval_release(
        source,
        replace(_spec(), source_artifact_sha256=source_hash),
    )

    with pytest.raises(ValueError, match="source artifact hash mismatch"):
        write_release(release, manifest, tmp_path / "release", source_path=different_source)


def test_cli_builds_release_from_mapped_chinese_csv(tmp_path: Path) -> None:
    source = tmp_path / "industry_history.csv"
    pd.DataFrame(
        {
            "证券代码": ["000001.SZ", "000001.SZ", "600000.SH"],
            "行业代码": ["I1", "I2", "I1"],
            "生效日期": ["2023-01-01", "2025-01-01", "2023-01-01"],
            "结束日期": ["2025-01-01", None, None],
            "发布时间": ["2023-01-01", "2025-01-01", "2023-01-01"],
        }
    ).to_csv(source, index=False, encoding="utf-8-sig")
    output = tmp_path / "release"
    mapping = json.dumps(
        {
            "证券代码": "code",
            "行业代码": "group_id",
            "生效日期": "effective_from",
            "结束日期": "effective_to",
            "发布时间": "source_observed_at",
        },
        ensure_ascii=False,
    )
    root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(root / "src")

    completed = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "build_pit_group_release.py"),
            "--input", str(source),
            "--input-mode", "intervals",
            "--output-root", str(output),
            "--column-map", mapping,
            "--source-name", "licensed_vendor",
            "--source-version", "2025-12-31",
            "--source-uri", "baidu-share:user-supplied",
            "--retrieved-at", "2026-07-11T12:00:00+08:00",
            "--group-type", "industry",
            "--membership-policy", "single",
            "--required-start", "2024-01-01 09:30:00",
            "--required-end", "2025-12-31 23:59:59",
            "--maximum-observable-time", "2025-12-31 23:59:59",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["status"] == "PIT_GROUP_RELEASE_BUILT"
    assert len(payload["source_artifact_sha256"]) == 64
    assert (output / "pit_group_membership.parquet").exists()
    assert (output / "pit_group_membership.manifest.json").exists()
