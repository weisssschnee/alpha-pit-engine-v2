from __future__ import annotations

from our_system_phase2.runtime.phase3cp_real_cm_small_loop import (
    _canonical_expression_key,
    _load_memory_hashes,
    _skeleton_key,
    _write_semantic_block_memory,
)


def test_sampled_signal_equivalence_memory_blocks_exact_expression_not_skeleton(tmp_path) -> None:
    expression = "Add(CSRank($x),CSRank($y))"
    path = _write_semantic_block_memory(
        tmp_path,
        [
            {
                "candidate_id": "duplicate-b",
                "expression": expression,
                "memory_block_policy": "sampled_signal_observation",
                "memory_block_reason": "exact_quantized_rank_and_mask_match",
                "pre_cm_semantic_decision": "REJECT_SIGNAL_EQUIVALENT",
                "pre_cm_semantic_reasons": "exact_quantized_rank_and_mask_match",
            }
        ],
        run_label="unit-test",
    )

    assert path is not None
    hashes, _ = _load_memory_hashes([tmp_path], ["*.csv"])
    assert _canonical_expression_key(expression) in hashes
    assert _skeleton_key(expression) not in hashes
