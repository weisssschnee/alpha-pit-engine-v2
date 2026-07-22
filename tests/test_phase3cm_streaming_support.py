from __future__ import annotations

import numpy as np

from our_system_phase2.services.phase3cm_streaming_support import PairSupportAccumulator


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
