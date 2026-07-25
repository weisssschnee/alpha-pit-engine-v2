from __future__ import annotations

import numpy as np

from our_system_phase2.services.phase3cm_streaming_support import (
    PairSupportAccumulator,
    common_support_masks,
)


def _reference_common_support_masks(signals: np.ndarray) -> np.ndarray:
    pair_count = signals.shape[0] // 2
    masks = np.empty((pair_count, signals.shape[1]), dtype=np.bool_)
    for pair_index in range(pair_count):
        common = np.isfinite(signals[2 * pair_index]) & np.isfinite(
            signals[2 * pair_index + 1]
        )
        masks[pair_index] = common
        signals[2 * pair_index, ~common] = np.nan
        signals[2 * pair_index + 1, ~common] = np.nan
    return masks


def test_common_support_masks_match_reference_exactly() -> None:
    source = np.array(
        [
            [1.0, np.nan, 3.0, np.inf, 5.0, -np.inf],
            [1.5, 2.0, np.nan, 4.0, np.inf, 6.0],
            [np.nan, 8.0, 9.0, 10.0, 11.0, 12.0],
            [7.0, 8.5, np.inf, 10.5, 11.5, 12.5],
        ],
        dtype=np.float64,
    )
    expected_signals = source.copy()
    expected_masks = _reference_common_support_masks(expected_signals)
    actual_signals = source.copy()
    actual_masks = common_support_masks(actual_signals)

    np.testing.assert_array_equal(actual_masks, expected_masks)
    np.testing.assert_array_equal(
        np.isnan(actual_signals),
        np.isnan(expected_signals),
    )
    np.testing.assert_allclose(
        actual_signals,
        expected_signals,
        rtol=0.0,
        atol=0.0,
        equal_nan=True,
    )


def test_common_support_masks_preserve_float32_dtype() -> None:
    signals = np.array(
        [[1.0, np.nan, 3.0], [1.5, 2.0, np.inf]],
        dtype=np.float32,
    )
    masks = common_support_masks(signals)
    assert masks.dtype == np.bool_
    assert signals.dtype == np.float32
    np.testing.assert_array_equal(
        masks,
        np.array([[True, False, False]], dtype=np.bool_),
    )
    assert np.isnan(signals[:, 1:]).all()


def test_common_support_masks_reject_invalid_layouts() -> None:
    with np.testing.assert_raises(ValueError):
        common_support_masks(np.ones((3, 4), dtype=np.float64))
    with np.testing.assert_raises(TypeError):
        common_support_masks(np.ones((2, 4), dtype=np.int64))


def test_common_support_masks_preserve_non_contiguous_semantics() -> None:
    signals = np.array(
        [[1.0, np.nan, 3.0, 4.0], [1.5, 2.0, np.inf, 4.5]],
        dtype=np.float64,
    )[:, ::2]
    masks = common_support_masks(signals)
    np.testing.assert_array_equal(
        masks,
        np.array([[True, False]], dtype=np.bool_),
    )
    assert np.isnan(signals[:, 1]).all()


def test_pair_support_digest_is_block_composable_and_checkpointable() -> None:
    time = np.repeat(np.arange(4, dtype=np.int64), 3)
    code = np.tile(np.arange(3, dtype=np.int32), 4)
    shard = (code % 2).astype(np.uint16)
    row = np.arange(len(time), dtype=np.uint64)
    duplicate = np.zeros(len(time), dtype=np.uint32)
    mask = np.vstack((np.arange(len(time)) % 2 == 0, np.arange(len(time)) % 3 != 0))
    whole = PairSupportAccumulator(pair_ids=("p0", "p1"))
    whole.update(
        common_masks=mask,
        trade_times_ns=time,
        code_ids=code,
        source_shards=shard,
        source_row_identity=row,
        duplicate_ordinal=duplicate,
    )
    streamed = PairSupportAccumulator(pair_ids=("p0", "p1"))
    for part in (slice(0, 6), slice(6, None)):
        streamed.update(
            common_masks=mask[:, part],
            trade_times_ns=time[part],
            code_ids=code[part],
            source_shards=shard[part],
            source_row_identity=row[part],
            duplicate_ordinal=duplicate[part],
        )
    assert streamed.identities() == whole.identities()
    restored = PairSupportAccumulator(pair_ids=("p0", "p1"))
    restored.restore_continuation_payload(streamed.continuation_payload())
    assert restored.identities() == streamed.identities()


def test_pair_support_batches_match_single_full_pair_update() -> None:
    time = np.repeat(np.arange(4, dtype=np.int64), 3)
    code = np.tile(np.arange(3, dtype=np.int32), 4)
    shard = (code % 2).astype(np.uint16)
    row = np.arange(len(time), dtype=np.uint64)
    duplicate = np.zeros(len(time), dtype=np.uint32)
    mask = np.vstack((np.arange(len(time)) % 2 == 0, np.arange(len(time)) % 3 != 0))
    coordinates = dict(
        trade_times_ns=time,
        code_ids=code,
        source_shards=shard,
        source_row_identity=row,
        duplicate_ordinal=duplicate,
    )
    whole = PairSupportAccumulator(pair_ids=("p0", "p1"))
    whole.update(common_masks=mask, **coordinates)

    batched = PairSupportAccumulator(pair_ids=("p0", "p1"))
    batched.update(common_masks=mask[:1], pair_indices=(0,), **coordinates)
    batched.update(common_masks=mask[1:], pair_indices=(1,), **coordinates)

    assert batched.identities() == whole.identities()


def test_prepared_support_tokens_preserve_exact_identities() -> None:
    time = np.repeat(np.arange(4, dtype=np.int64), 3)
    code = np.tile(np.arange(3, dtype=np.int32), 4)
    coordinates = dict(
        trade_times_ns=time,
        code_ids=code,
        source_shards=(code % 2).astype(np.uint16),
        source_row_identity=np.arange(len(time), dtype=np.uint64),
        duplicate_ordinal=np.zeros(len(time), dtype=np.uint32),
    )
    mask = np.vstack((np.arange(len(time)) % 2 == 0, np.arange(len(time)) % 3 != 0))
    direct = PairSupportAccumulator(pair_ids=("p0", "p1"))
    direct.update(common_masks=mask, **coordinates)
    reused = PairSupportAccumulator(pair_ids=("p0", "p1"))
    tokens = reused.prepare_block_tokens(**coordinates)
    reused.update(common_masks=mask[:1], pair_indices=(0,), block_tokens=tokens)
    reused.update(common_masks=mask[1:], pair_indices=(1,), block_tokens=tokens)
    assert reused.identities() == direct.identities()
