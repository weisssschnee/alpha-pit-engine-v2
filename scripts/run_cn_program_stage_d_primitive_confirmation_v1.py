"""Prospective Stage-D confirmation of the unchanged primitive-local ranking on a disjoint fresh pool."""
from __future__ import annotations
import argparse, json, math, statistics, time
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_program_stage_c_system_search_v1 as stagec
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from scripts import run_cn_joint_program_phase_c_v0 as engine
from our_system_phase2.services.unified_capability_registry import stable_hash

TEMPLATES=stagec.TEMPLATES
CHECKPOINT_SIZE=48
PRIMARY_EXECUTOR_WORKERS=24
RESOURCE_CANARY_PROBE_SECONDS=30.0
PRIMARY_POLICY_ID='PRIMITIVE_LOCAL_HIERARCHICAL_V1'
UNIFORM_POLICY_ID='UNIFORM_HASH_V1'


def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding='utf-8-sig'))

def verify_prefreeze(path:Path)->dict[str,Any]:
    p=_read(path); body=dict(p); claimed=str(body.pop('prefreeze_payload_sha256',''))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError('STAGE_D_PREFREEZE_HASH_DRIFT')
    if (
        p.get('status')!='STAGE_D_PRIMITIVE_CONFIRMATION_PREFROZEN_BEFORE_FINANCIAL_READ'
        or p.get('no_stage_c_label_retraining_or_tuning') is not True
        or p.get('stage_d_financial_labels_read_during_freeze') is not False
        or p.get('candidate_evaluation_executed') is not False
    ): raise RuntimeError('STAGE_D_PREFREEZE_STATUS_DRIFT')
    source=dict(p['primitive_score_source'])
    if int(source.get('stage_c_results_in_stats', -1))!=0 or int(source.get('stats_unique_program_exact_count') or 0)!=1392:
        raise RuntimeError('STAGE_D_PRIMITIVE_SOURCE_DRIFT')
    cohort=dict(p['cohort']); candidates=list(cohort['candidates']); exacts=[str(r['exact_identity']) for r in candidates]; physical=list(map(str,cohort['physical_order']))
    if len(candidates)!=2016 or len(set(exacts))!=2016 or len(physical)!=2016 or set(physical)!=set(exacts) or stable_hash(physical)!=str(cohort['physical_order_sha256']):
        raise RuntimeError('STAGE_D_COHORT_DRIFT')
    from collections import Counter
    if Counter(str(r['template_id']) for r in candidates)!={t:288 for t in TEMPLATES}: raise RuntimeError('STAGE_D_TEMPLATE_DRIFT')
    policy=dict(p['policy']); prim=dict(policy['primary_policy_order_by_template'])
    if str(policy['primary_policy_id'])!=PRIMARY_POLICY_ID: raise RuntimeError('STAGE_D_PRIMARY_POLICY_DRIFT')
    for t in TEMPLATES:
        order=list(map(str,prim[t])); expected={str(r['exact_identity']) for r in candidates if str(r['template_id'])==t}
        if len(order)!=288 or len(set(order))!=288 or set(order)!=expected: raise RuntimeError(f'STAGE_D_PRIMITIVE_ORDER_DRIFT:{t}')
    uniforms=dict(policy['uniform_orders_by_seed_and_template'])
    if len(uniforms)!=8: raise RuntimeError('STAGE_D_UNIFORM_SEED_COUNT_DRIFT')
    for seed,by in uniforms.items():
        for t in TEMPLATES:
            order=list(map(str,dict(by)[t])); expected={str(r['exact_identity']) for r in candidates if str(r['template_id'])==t}
            if len(order)!=288 or set(order)!=expected: raise RuntimeError(f'STAGE_D_UNIFORM_ORDER_DRIFT:{seed}:{t}')
    return p


def _load_authority(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
    prior=dict(authorization['source_prior_exact'])
    return successor._load_authority(
        args,authorization=authorization,repo_sha=repo_sha,
        campaign_id=str(authorization['campaign_id']),campaign_profile=str(authorization['campaign_profile']),
        prior_freeze_payload_sha256=str(prior['payload_sha256']),prior_exact_count=int(prior['count']),
        prior_exact_identities_sha256=str(prior['exact_identities_sha256']),prior_identity_field=str(prior['identity_field']),
        input_binding_schema_version='cn_program_stage_d_primitive_confirmation_input_binding_v1',
        resource_profile_id='SEARCH_DUAL_24',resource_profile_role='SEARCH',maximum_executor_workers=PRIMARY_EXECUTOR_WORKERS,
    )


def reconstruct_schedules(prefreeze:Mapping[str,Any],*,authority:Mapping[str,Any])->list[dict[str,Any]]:
    cohort=dict(prefreeze['cohort']); by_exact={str(r['exact_identity']):dict(r) for r in cohort['candidates']}; from collections import Counter
    counters=Counter(); schedules=[]
    for idx,exact in enumerate(map(str,cohort['physical_order'])):
        c=by_exact[exact]; t=str(c['template_id'])
        schedules.append(stagec._schedule(c,authority=authority,global_ordinal=idx,checkpoint_ordinal=idx//CHECKPOINT_SIZE,template_ordinal=counters[t])); counters[t]+=1
    if len(schedules)!=2016 or len({str(s['stage_c_exact_identity']) for s in schedules})!=2016: raise RuntimeError('STAGE_D_SCHEDULE_COVERAGE_DRIFT')
    return schedules


def prefinancial_rehearsal(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
    pre=verify_prefreeze(args.stage_d_prefreeze); authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); schedules=reconstruct_schedules(pre,authority=authority); fields=tuple(sorted(engine._checkpoint_field_columns(schedules)))
    return {'status':'ZERO_FINANCIAL_STAGE_D_PREFLIGHT_READY','schedule_count':2016,'checkpoint_count':math.ceil(2016/CHECKPOINT_SIZE),'field_column_count':len(fields),'field_columns':list(fields),'field_columns_sha256':stable_hash(list(fields)),'candidate_evaluation_executed':False,'stage_d_financial_labels_read':False,'validation_reads':0,'holdout_reads':0,'forward_2026_reads':0}


def _select(by_template:Mapping[str,Sequence[str]],k:int,result_by_exact:Mapping[str,Mapping[str,Any]])->tuple[list[dict[str,Any]],dict[str,dict[str,Any]]]:
    all_rows=[]; per={}
    for t in TEMPLATES:
        rows=[dict(result_by_exact[str(x)]) for x in list(by_template[t])[:k]]; all_rows.extend(rows); per[t]=stagec._metric(rows)
    return all_rows,per


def _curve(prefreeze:Mapping[str,Any], result_by_exact:Mapping[str,Mapping[str,Any]])->dict[str,Any]:
    policy=dict(prefreeze['policy']); prim_orders=dict(policy['primary_policy_order_by_template']); uniform_orders=dict(policy['uniform_orders_by_seed_and_template']); ev_orders=stagec._evolution_orders(prefreeze,result_by_exact)
    budgets=list(map(int,dict(prefreeze['evaluation'])['budgets_per_template'])); out={'budgets':{},'typed_evolution_orders_by_template':ev_orders}
    for k in budgets:
        prim_rows,prim_per=_select(prim_orders,k,result_by_exact); prim=stagec._metric(prim_rows)
        ev_rows,ev_per=_select(ev_orders,k,result_by_exact); ev=stagec._metric(ev_rows)
        uniform_seed={}; uniform_per={}
        for seed,by in sorted(uniform_orders.items()):
            rows,per=_select(dict(by),k,result_by_exact); uniform_seed[str(seed)]=stagec._metric(rows); uniform_per[str(seed)]=per
        mean_prod=statistics.mean(float(x['productive']) for x in uniform_seed.values()); mean_beh=statistics.mean(float(x['distinct_behavior_pair_count']) for x in uniform_seed.values())
        wins_u=wins_e=0; gains=[]; uniform_per_mean={}
        for t in TEMPLATES:
            p=float(prim_per[t]['productive']); u=statistics.mean(float(uniform_per[s][t]['productive']) for s in uniform_per); e=float(ev_per[t]['productive']); uniform_per_mean[t]=u
            wins_u+=int(p>u); wins_e+=int(p>e); gains.append(max(0.0,p-u))
        gain=sum(gains); concentration=max(gains)/gain if gain>0 else 1.0
        out['budgets'][str(7*k)]={
            'per_template_budget':k,'primitive_local':prim,'primitive_per_template':prim_per,'typed_evolution':ev,'typed_evolution_per_template':ev_per,
            'uniform_seed_metrics':uniform_seed,'uniform_per_template_mean_productive':uniform_per_mean,
            'uniform_seed_mean_productive_count':mean_prod,'uniform_seed_mean_distinct_behavior_pair_count':mean_beh,
            'primitive_vs_uniform_productive_ratio':float(prim['productive'])/mean_prod if mean_prod else None,
            'primitive_vs_uniform_productive_delta':float(prim['productive'])-mean_prod,
            'primitive_vs_evolution_productive_ratio':float(prim['productive'])/float(ev['productive']) if ev['productive'] else None,
            'primitive_vs_evolution_productive_delta':float(prim['productive'])-float(ev['productive']),
            'primitive_vs_uniform_behavior_ratio':float(prim['distinct_behavior_pair_count'])/mean_beh if mean_beh else None,
            'primitive_distinct_behavior_pair_rate':float(prim['distinct_behavior_pair_count'])/float(prim['evaluated']) if prim['evaluated'] else 0.0,
            'primitive_templates_won_vs_uniform_mean':wins_u,'primitive_templates_won_vs_evolution':wins_e,
            'primitive_positive_gain_max_template_fraction':concentration,
        }
    gate=dict(dict(prefreeze['evaluation'])['confirmatory_victory_gate']); checks={}
    for budget,minimum in dict(gate['primitive_vs_uniform_mean_min_ratio']).items():
        checks[f'primitive_vs_uniform_ratio_{budget}']=float(out['budgets'][str(budget)]['primitive_vs_uniform_productive_ratio'] or 0)>=float(minimum)
    primary=out['budgets']['1008']
    checks.update({
        'primitive_vs_evolution_ratio_1008':float(primary['primitive_vs_evolution_productive_ratio'] or 0)>=float(gate['primitive_vs_typed_evolution_min_ratio_at_1008']),
        'primitive_vs_evolution_delta_1008':float(primary['primitive_vs_evolution_productive_delta'])>=float(gate['primitive_vs_typed_evolution_min_absolute_delta_at_1008']),
        'template_wins_uniform_1008':int(primary['primitive_templates_won_vs_uniform_mean'])>=int(gate['minimum_templates_won_vs_uniform_mean_at_1008']),
        'template_wins_evolution_1008':int(primary['primitive_templates_won_vs_evolution'])>=int(gate['minimum_templates_won_vs_evolution_at_1008']),
        'gain_concentration_1008':float(primary['primitive_positive_gain_max_template_fraction'])<=float(gate['maximum_single_template_fraction_of_positive_productive_gain']),
        'behavior_absolute_floor_1008':float(primary['primitive_distinct_behavior_pair_rate'])>=float(gate['minimum_distinct_behavior_pair_rate_at_1008']),
        'behavior_relative_floor_1008':float(primary['primitive_vs_uniform_behavior_ratio'] or 0)>=float(gate['minimum_distinct_behavior_pair_count_vs_uniform_mean_ratio_at_1008']),
    })
    out['victory_checks']=checks
    out['status']=dict(dict(prefreeze['evaluation'])['classification'])['pass' if all(checks.values()) else 'fail']
    return out


def _close_checkpoint(inflight:Path,closed:Path,previous_sha:str,checkpoint:int)->str:
    artifacts=[successor._artifact(p,inflight) for p in sorted(inflight.rglob('*')) if p.is_file() and p.name!='checkpoint_manifest.json']
    manifest=engine._self_hashed({'schema_version':'cn_program_stage_d_primitive_confirmation_checkpoint_manifest_v1','status':'STAGE_D_CHECKPOINT_CLOSED_IMMUTABLE','checkpoint_ordinal':checkpoint,'previous_checkpoint_manifest_file_sha256':previous_sha,'artifacts':artifacts},'manifest_payload_sha256')
    engine._write_json(inflight/'checkpoint_manifest.json',manifest); inflight.replace(closed); return engine._sha256(closed/'checkpoint_manifest.json')


def run(args:argparse.Namespace,*,admission:Mapping[str,Any],authorization:Mapping[str,Any])->dict[str,Any]:
    root=args.output_root.resolve()
    if not root.is_dir() or not (root/'.project_control_execution').is_dir(): raise RuntimeError('STAGE_D_ADMITTED_ROOT_MISSING')
    if {p.name for p in root.iterdir()}!={'.project_control_execution'}: raise RuntimeError('STAGE_D_ADMITTED_ROOT_NOT_CLEAN')
    pre=verify_prefreeze(args.stage_d_prefreeze); authority=_load_authority(args,authorization=authorization,repo_sha=str(admission['repo_sha'])); schedules=reconstruct_schedules(pre,authority=authority)
    engine._write_json(root/'input_binding.json',authority['input_binding'])
    engine._write_json(root/'policy_binding.json',{'schema_version':'cn_program_stage_d_policy_binding_v1','prefreeze_payload_sha256':str(pre['prefreeze_payload_sha256']),'primary_policy_id':PRIMARY_POLICY_ID,'primitive_stats_payload_sha256':str(pre['primitive_score_source']['stats_payload_sha256']),'stage_c_results_in_stats':0,'physical_order_policy_independent':True,'stage_d_financial_labels_read_during_freeze':False})
    input_hash=str(authority['input_binding']['input_binding_sha256']); fields=tuple(sorted(engine._checkpoint_field_columns(schedules)))
    official=_read(args.official_resource_canary)
    if official.get('status')!='PASS' or int(official.get('field_column_count') or 0)!=len(fields) or tuple(map(str,official.get('field_columns') or ()))!=fields or str(official.get('prefreeze_payload_sha256') or '')!=str(pre['prefreeze_payload_sha256']): raise RuntimeError('STAGE_D_OFFICIAL_CANARY_DRIFT')
    old=large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try:
        large_fresh.RESOURCE_CANARY_PROBE_SECONDS=RESOURCE_CANARY_PROBE_SECONDS; canary=large_fresh._resource_canary(authority,input_hash,PRIMARY_EXECUTOR_WORKERS,fields)
    finally: large_fresh.RESOURCE_CANARY_PROBE_SECONDS=old
    if canary.get('status')!='PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY' or int(canary.get('requested_workers') or 0)!=24 or int(canary.get('field_column_count') or 0)!=len(fields) or int(canary.get('pagefile_pages_in_delta_bytes',-1))!=0 or int(canary.get('pagefile_pages_out_delta_bytes',-1))!=0 or canary.get('candidate_evaluation_executed') is not False: raise RuntimeError('STAGE_D_RUNTIME_CANARY_FAIL')
    engine._write_json(root/'resource_canary.json',canary)

    started=time.perf_counter(); previous='GENESIS'; all_results=[]; checkpoint=0
    for offset in range(0,len(schedules),CHECKPOINT_SIZE):
        batch=schedules[offset:offset+CHECKPOINT_SIZE]; inflight=root/f'checkpoint_{checkpoint:04d}.inflight'; closed=root/f'checkpoint_{checkpoint:04d}'; inflight.mkdir(parents=False,exist_ok=False); engine._write_jsonl(inflight/'selected_schedule.jsonl',batch)
        records=successor._evaluate_schedules(batch,record_root=inflight/'records',authority=authority,input_hash=input_hash,executor_workers=PRIMARY_EXECUTOR_WORKERS)
        by={int(s['main_record_ordinal']):s for s in batch}; results=[stagec._result_record(r,by[int(r['main_record_ordinal'])]) for r in records]; engine._write_jsonl(inflight/'candidate_results.jsonl',results); previous=_close_checkpoint(inflight,closed,previous,checkpoint); all_results.extend(results); checkpoint+=1
    result_by_exact={str(r['exact_identity']):r for r in all_results}
    if len(all_results)!=2016 or len(result_by_exact)!=2016: raise RuntimeError('STAGE_D_RESULT_COVERAGE_DRIFT')
    benchmark=_curve(pre,result_by_exact); engine._write_json(root/'primitive_confirmation_benchmark.json',benchmark)
    closure=engine._self_hashed({
        'schema_version':'cn_program_stage_d_primitive_confirmation_complete_v1','status':'STAGE_D_PRIMITIVE_CONFIRMATION_COMPLETE','repo_sha':str(admission['repo_sha']),'authorization_payload_sha256':str(authorization['authorization_payload_sha256']),'prefreeze_payload_sha256':str(pre['prefreeze_payload_sha256']),'stage_d_evaluated':2016,'checkpoint_count':checkpoint,'last_checkpoint_manifest_file_sha256':previous,'confirmation_status':str(benchmark['status']),'primitive_confirmation_benchmark':benchmark,'full_stage_d_metric':stagec._metric(all_results),'wall_seconds':time.perf_counter()-started,'evaluation_data_role':'DEVELOPMENT_ONLY','restricted_reads':{'validation':0,'holdout':0,'historical_2023':0,'forward_b':0,'forward_2026':0},'validation_feedback_used':False,'promotion_authorized':False,'oos_authority':'NONE','automatic_successor_authorized':False,
    },'closure_payload_sha256')
    engine._write_json(root/'CN_PROGRAM_STAGE_D_PRIMITIVE_CONFIRMATION_COMPLETE.json',closure); return closure

__all__=['verify_prefreeze','reconstruct_schedules','prefinancial_rehearsal','_curve','run']
