from __future__ import annotations

import json

import numpy as np
import pytest

from our_system_phase2.services.phase3cm_streaming_checkpoint import (
    CheckpointDriftError,
    StreamingCheckpointPayload,
    load_checkpoint,
    write_checkpoint,
)


def _payload(total: float = 3.0) -> StreamingCheckpointPayload:
    return StreamingCheckpointPayload(
        execution_plan_hash="a" * 64,
        input_binding_hash="b" * 64,
        backend_identity="minute_active",
        route_cohort="active_bar",
        execution_position={"global_time_block_ordinal": 2, "pair_batch_ordinal": 1},
        completed_blocks=[0, 1],
        completed_pair_batches=["batch.0"],
        temporal_continuation_payload={"close_tail": np.asarray([[1.0, 2.0]])},
        state_event_continuation_payload={"duration": np.asarray([2], dtype=np.int64)},
        portfolio_continuation_payload={"selected": np.asarray([[True, False]])},
        streaming_reducer_payload={"net_sum": np.asarray([total])},
    )


def test_checkpoint_roundtrip_persists_actual_continuation_payload(tmp_path) -> None:
    record = write_checkpoint(tmp_path, _payload(), checkpoint_ordinal=2)
    restored = load_checkpoint(
        record["checkpoint_path"],
        expected_execution_plan_hash="a" * 64,
        expected_input_binding_hash="b" * 64,
    )

    assert restored.completed_blocks == [0, 1]
    np.testing.assert_array_equal(
        restored.temporal_continuation_payload["close_tail"],
        np.asarray([[1.0, 2.0]]),
    )
    np.testing.assert_array_equal(
        restored.portfolio_continuation_payload["selected"],
        np.asarray([[True, False]]),
    )
    assert record["payload_bytes"] > 0
    assert len(record["payload_sha256"]) == 64


def test_checkpoint_rejects_execution_plan_drift(tmp_path) -> None:
    record = write_checkpoint(tmp_path, _payload(), checkpoint_ordinal=2)
    with pytest.raises(CheckpointDriftError, match="execution-plan hash drift"):
        load_checkpoint(
            record["checkpoint_path"],
            expected_execution_plan_hash="c" * 64,
            expected_input_binding_hash="b" * 64,
        )


def test_resume_state_matches_uninterrupted_reducer_result(tmp_path) -> None:
    values = [1.0, 2.0, 3.0, 4.0]
    uninterrupted = sum(values)
    record = write_checkpoint(tmp_path, _payload(sum(values[:2])), checkpoint_ordinal=2)
    restored = load_checkpoint(
        record["checkpoint_path"],
        expected_execution_plan_hash="a" * 64,
        expected_input_binding_hash="b" * 64,
    )
    resumed = float(restored.streaming_reducer_payload["net_sum"][0]) + sum(values[2:])
    assert resumed == uninterrupted
    manifest = json.loads((tmp_path / "CN_STREAMING_CHECKPOINT_MANIFEST.json").read_text())
    assert manifest["latest_complete_checkpoint"] == record["checkpoint_path"].name

