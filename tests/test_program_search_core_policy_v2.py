from copy import deepcopy
import json
from pathlib import Path
import pytest
from our_system_phase2.services.program_search_core_policy_v2 import (
    MIN_MATURE_DEVELOPMENT_OBSERVATIONS,
    resolve_search_core_v2_primary_arm,
    validate_search_core_v2_mature_continuation_snapshot,
)
from our_system_phase2.services.program_search_primitive_credit_v1 import PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
from our_system_phase2.services.program_search_state_jump_generator_v2 import SEMANTIC_STATE_JUMP_GENERATOR_V2
from our_system_phase2.services.unified_capability_registry import stable_hash

REPO=Path(__file__).resolve().parents[1]
FINAL=REPO/'runtime/run_plans/cn_search_core_v2_stage2_final_optimizer_state_888a276_20260825.json'
WAVE1_FINAL=REPO/'runtime/run_plans/cn_search_core_v2_production_wave1_final_optimizer_state_412d51c_20260825.json'

def _resign(payload):
    body=dict(payload); body.pop('snapshot_hash',None); payload['snapshot_hash']=stable_hash(body); return payload

def test_no_state_bootstraps_primitive():
    row=resolve_search_core_v2_primary_arm(None)
    assert row['primary_arm']==PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
    assert row['development_observations']==0

def test_final_mature_state_selects_generator():
    state=json.loads(FINAL.read_text(encoding='utf-8-sig'))
    row=resolve_search_core_v2_primary_arm(state)
    assert row['primary_arm']==SEMANTIC_STATE_JUMP_GENERATOR_V2
    assert row['development_observations']==1008
    assert row['minimum_mature_development_observations']==168

def test_wave1_mature_state_keeps_initial_adoption_gate_strict():
    state=json.loads(WAVE1_FINAL.read_text(encoding='utf-8-sig'))
    with pytest.raises(ValueError, match='DEAD_REGION_NOT_CLEAN'):
        resolve_search_core_v2_primary_arm(state)


def test_wave1_mature_state_is_valid_continuation_with_learned_dead_region():
    state=json.loads(WAVE1_FINAL.read_text(encoding='utf-8-sig'))
    row=validate_search_core_v2_mature_continuation_snapshot(state)
    assert row['primary_arm']==SEMANTIC_STATE_JUMP_GENERATOR_V2
    assert row['development_observations']==1344
    assert row['history_count']==56
    assert row['dead_region_count']==1
    assert row['reason']=='MATURE_CONTINUATION_STATE_CONFIRMED'


def test_below_threshold_stays_primitive():
    state=json.loads(FINAL.read_text(encoding='utf-8-sig'))
    state=deepcopy(state); state['history']=state['history'][:6]
    exacts=sorted(state['generated_exact_identities'])[:144]
    state['generated_exact_identities']=exacts
    state['generator']['memory']={}  # generator internals are not inspected by the policy resolver
    state['history'][-1]['generator_diagnostics']['memory_observations']=144
    _resign(state)
    row=resolve_search_core_v2_primary_arm(state)
    assert row['development_observations']==144
    assert row['primary_arm']==PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1

def test_rejects_non_development_feedback():
    state=json.loads(FINAL.read_text(encoding='utf-8-sig'))
    state=deepcopy(state); state['history'][-1]['feedback_domain']='VALIDATION'; _resign(state)
    with pytest.raises(ValueError, match='NON_DEVELOPMENT'):
        resolve_search_core_v2_primary_arm(state)

def test_rejects_sealed_feedback():
    state=json.loads(FINAL.read_text(encoding='utf-8-sig'))
    state=deepcopy(state); state['history'][-1]['sealed_feedback_used']=True; _resign(state)
    with pytest.raises(ValueError, match='SEALED_FEEDBACK'):
        resolve_search_core_v2_primary_arm(state)
