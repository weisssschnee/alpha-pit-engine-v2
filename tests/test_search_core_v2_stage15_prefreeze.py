from pathlib import Path
from scripts import build_cn_search_core_v2_stage15_prefreeze as builder

REPO = Path(__file__).resolve().parents[1]

def test_stage15_prefreeze_is_disjoint_mature_continuation():
    p = builder.build(REPO, source_repo_sha="a" * 40)
    assert p["status"] == builder.STATUS
    assert p["stage15"]["total_financial_evaluations"] == 336
    assert p["arm_a_primitive"]["selected_count"] == 168
    assert len(set(p["arm_a_primitive"]["selected_exact_identities"])) == 168
    for rows in p["arm_a_primitive"]["selected_by_template"].values():
        assert len(rows) == 24
    b = p["arm_b_state_jump"]
    assert b["source_history_count"] == 14
    assert b["source_generated_exact_count"] == 336
    assert b["source_memory_observations"] == 336
    assert b["sealed_feedback_allowed"] is False
    assert p["financial_labels_read_by_builder"] is False
    assert not any(p["restricted_reads"].values())
    assert p["automatic_stage2_authorized"] is False


def test_stage15_primitive_is_exact_next_slice_after_stage1():
    p = builder.build(REPO, source_repo_sha="b" * 40)
    s1 = builder._read(REPO / builder.STAGE1_PREFREEZE)
    for template in builder.TEMPLATES:
        order = list(s1["arm_a_primitive"]["eligible_orders_by_template"][template])
        assert p["arm_a_primitive"]["selected_by_template"][template] == order[48:72]
        assert not set(order[:48]).intersection(p["arm_a_primitive"]["selected_by_template"][template])
