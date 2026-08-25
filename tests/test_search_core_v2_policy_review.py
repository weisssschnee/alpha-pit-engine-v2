from pathlib import Path
from scripts import build_cn_search_core_v2_policy_review as builder
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2

REPO=Path(__file__).resolve().parents[1]

def test_policy_review_freezes_mature_generator_primary_without_financial_read():
    p=builder.build(REPO,source_repo_sha='a'*40)
    assert p['status']==builder.STATUS
    assert p['policy']['current_primary_arm']==SEMANTIC_STATE_JUMP_GENERATOR_V2
    assert p['policy']['mature_switch_threshold_development_observations']==168
    assert p['evidence']['stage1']['round0_productive']=={'primitive':67,'generator':34}
    assert p['evidence']['stage1']['round1_productive']=={'primitive':59,'generator':71}
    assert p['evidence']['stage2']['template_wins_b']==7
    assert p['financial_evaluation_executed'] is False
    assert p['authority_boundaries']['alpha_promotion_authorized'] is False
    assert p['project_control_decision']['search_core_policy_change_authorized'] is True
    assert p['project_control_decision']['automatic_financial_campaign_launch_authorized'] is False

def test_policy_review_accepts_relative_repo_root(monkeypatch):
    monkeypatch.chdir(REPO)
    p=builder.build(Path('.'),source_repo_sha='b'*40)
    assert p['status']==builder.STATUS
