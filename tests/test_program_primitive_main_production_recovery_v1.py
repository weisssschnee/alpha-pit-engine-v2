from __future__ import annotations
import json
from pathlib import Path
import app
from scripts import run_cn_program_primitive_main_production_recovery_v1 as recovery
from our_system_phase2.services.candidate_program_proposal_v0 import GENERATION_ARMS, ProgramProposalReceiptV0

ROOT=Path(__file__).resolve().parents[1]
PREFIX=ROOT/'runtime/run_plans/cn_program_primitive_main_production_recovery_prefix_be111a2_20260821.json'

def test_recovery_route_registered_high_cost():
    assert app.ROUTES['cn-program-primitive-main-production-recovery-v1']=='our_system_phase2.runtime.cn_program_primitive_main_production_recovery_v1'
    assert 'cn-program-primitive-main-production-recovery-v1' in app.HIGH_COST_ROUTE_ACTIONS

def test_primitive_arm_is_legal_program_generation_arm():
    assert 'PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1' in GENERATION_ARMS

def test_recovery_prefix_is_frozen_24_plus_816_equals_840():
    p=recovery.verify_recovery_prefix(PREFIX)
    assert p['completed_logical_records']==24
    assert p['remaining_new_evaluations']==816
    assert p['final_total_logical_records']==840
    assert p['resume_checkpoint_ordinal']==1
    assert p['resume_template_index']==1
    assert p['financial_evaluator_reexecution_authorized'] is False
    assert p['financial_evaluator_reexecution_performed'] is False
    assert p['source_checkpoint1_generation_arm']=='PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1'
    assert p['source_checkpoint1_candidate_records']==0
