from __future__ import annotations

import numpy as np

from scripts.compare_cn_phase3cm_scaling_checkpoints import (
    _compare,
    normalize_portfolio_continuation,
)


def test_portfolio_continuation_normalizes_different_batch_partitions() -> None:
    payload = {
        "schema_version": "cn_phase3cm_batched_portfolio_continuation_v1",
        "batches": [
            {
                "candidate_indices": [2, 0],
                "payload": {
                    "selection_epoch": np.asarray([[[2]], [[0]]], dtype=np.int32),
                    "epoch_counter": np.asarray([[2], [0]], dtype=np.int32),
                },
            },
            {
                "candidate_indices": [1],
                "payload": {
                    "selection_epoch": np.asarray([[[1]]], dtype=np.int32),
                    "epoch_counter": np.asarray([[1]], dtype=np.int32),
                },
            },
        ],
    }

    normalized = normalize_portfolio_continuation(payload)

    assert sorted(normalized) == [0, 1, 2]
    assert normalized[2]["epoch_counter"].tolist() == [2]


def test_recursive_compare_accepts_equal_nan_arrays_and_reports_drift() -> None:
    assert _compare(np.asarray([1.0, np.nan]), np.asarray([1.0, np.nan])) == []
    assert _compare(np.asarray([1.0]), np.asarray([2.0])) == ["root:array_values"]
