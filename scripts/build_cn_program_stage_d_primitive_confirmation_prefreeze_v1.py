"""Freeze the Stage-D primitive-local confirmatory cohort and all comparison orders before any Stage-D label exists."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping

from scripts.build_cn_program_stage_c_system_search_prefreeze_v1 import (
    TEMPLATES, BUDGETS_PER_TEMPLATE, UNIFORM_SEEDS, primitive_score,
)
from our_system_phase2.services.program_search_optimizer_v1 import program_availability_entries_v1
from our_system_phase2.services.unified_capability_registry import stable_hash

COHORT_PER_TEMPLATE=288
COHORT_SEED="STAGE_D_PRIMITIVE_CONFIRMATORY_COHORT_V1"
PRIMITIVE_POLICY_ID="PRIMITIVE_LOCAL_HIERARCHICAL_V1"
UNIFORM_POLICY_ID="UNIFORM_HASH_V1"
TYPED_EVOLUTION_POLICY_ID="CATALOG_TYPED_EVOLUTION_PROGRAM_V2"


def read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def sha(path:Path)->str:
    h=hashlib.sha256();
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1<<20),b''): h.update(block)
    return h.hexdigest()
def verify_self(payload:Mapping[str,Any], field:str, label:str)->str:
    body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError(f"{label} self-hash drift")
    return claimed


def main(argv=None)->int:
    p=argparse.ArgumentParser(); root=Path(__file__).resolve().parents[1]
    p.add_argument('--repo-root',type=Path,default=root)
    p.add_argument('--output',type=Path,default=Path('runtime/run_plans/cn_program_stage_d_primitive_confirmation_prefreeze_v1.json'))
    a=p.parse_args(argv); root=a.repo_root.resolve()

    supply_path=root/'runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json'
    supply=read(supply_path); supply_hash=verify_self(supply,'audit_payload_sha256','Stage-D expanded supply')
    if (
        supply.get('status')!='ZERO_FINANCIAL_STAGE_D_EXPANDED_SUPPLY_AUDIT_COMPLETE'
        or int(supply.get('fresh_unique_count') or 0)!=3584
        or int(supply.get('effective_spent_exact_count') or 0)!=4718
        or bool(supply.get('candidate_evaluation_executed'))
        or bool(supply.get('stage_c_financial_labels_read_by_builder'))
    ):
        raise RuntimeError('Stage-D expanded supply contract drift')

    stats_path=root/'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json'
    stats=read(stats_path); stats_hash=verify_self(stats,'stats_payload_sha256','pre-Stage-C primitive stats')
    if (
        stats.get('status')!='SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN'
        or int(stats.get('unique_program_exact_count') or 0)!=1392
        or dict(stats.get('source_counts') or {})!={'large':840,'stage_a':288,'stage_b':264}
    ):
        raise RuntimeError('primitive stats provenance drift')

    stagec_builder=root/'scripts/build_cn_program_stage_c_system_search_prefreeze_v1.py'
    stagec_builder_sha=sha(stagec_builder)
    if stagec_builder_sha!='a1f0d08d38de1edd94c0c76dc5db6a5aab78ff878add99eef582c506258c1655':
        raise RuntimeError('Stage-C primitive score implementation drift')

    by_template={t:[] for t in TEMPLATES}
    for row in supply['fresh_entries']:
        t=str(row['template_id'])
        if t not in by_template: raise RuntimeError(f'unexpected template {t}')
        by_template[t].append(dict(row))

    cohort=[]
    for t in TEMPLATES:
        ordered=sorted(by_template[t],key=lambda r:(stable_hash({'seed':COHORT_SEED,'template':t,'exact':r['exact_identity']}),str(r['exact_identity'])))
        selected=ordered[:COHORT_PER_TEMPLATE]
        if len(selected)!=COHORT_PER_TEMPLATE: raise RuntimeError(f'underfilled Stage-D cohort {t}')
        cohort.extend(selected)
    exacts=[str(r['exact_identity']) for r in cohort]
    if len(cohort)!=2016 or len(set(exacts))!=2016: raise RuntimeError('Stage-D cohort coverage drift')

    # Freeze one-to-one normalized identities for causal Typed Evolution replay.
    evolution_entries=program_availability_entries_v1([{'genes':dict(r['program_genes'])} for r in cohort])
    normalized_by_physical={str(row['exact_identity']):entry.exact_identity for row,entry in zip(cohort,evolution_entries,strict=True)}
    if len(normalized_by_physical)!=2016 or len(set(normalized_by_physical.values()))!=2016:
        raise RuntimeError('Stage-D normalized Evolution mapping is not one-to-one')

    for row in cohort:
        primitive,novelty,_detail=primitive_score(row,stats)
        row['primitive_score']=float(primitive)
        row['novelty_score']=float(novelty)

    primitive_orders={}
    for t in TEMPLATES:
        rows=[r for r in cohort if str(r['template_id'])==t]
        primitive_orders[t]=[str(r['exact_identity']) for r in sorted(rows,key=lambda r:(-float(r['primitive_score']),str(r['exact_identity'])))]
    uniform_orders={}
    for seed in UNIFORM_SEEDS:
        uniform_orders[str(seed)]={
            t:[str(r['exact_identity']) for r in sorted((x for x in cohort if str(x['template_id'])==t),key=lambda r:(stable_hash({'seed':int(seed),'exact':str(r['exact_identity'])}),str(r['exact_identity'])))]
            for t in TEMPLATES
        }

    # Physical evaluation order is independent of every search policy.
    by_t={t:[str(r['exact_identity']) for r in cohort if str(r['template_id'])==t] for t in TEMPLATES}
    physical_order=[by_t[t][i] for i in range(COHORT_PER_TEMPLATE) for t in TEMPLATES]

    payload={
        'schema_version':'cn_program_stage_d_primitive_confirmation_prefreeze_v1',
        'status':'STAGE_D_PRIMITIVE_CONFIRMATION_PREFROZEN_BEFORE_FINANCIAL_READ',
        'source_repo_sha':'cbaaaee17a456fedf0a6a4144e51da7f328c1994',
        'hypothesis':'UNCHANGED_PRE_STAGE_C_PRIMITIVE_CREDIT_RANKING_REPEATS_LARGE_SCALE_SEARCH_ADVANTAGE_ON_DISJOINT_FRESH_POOL',
        'no_stage_c_label_retraining_or_tuning':True,
        'primitive_score_source':{
            'implementation_relative_path':'scripts/build_cn_program_stage_c_system_search_prefreeze_v1.py',
            'implementation_file_sha256':stagec_builder_sha,
            'stats_relative_path':'runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json',
            'stats_file_sha256':sha(stats_path),
            'stats_payload_sha256':stats_hash,
            'stats_unique_program_exact_count':1392,
            'stage_c_results_in_stats':0,
        },
        'expanded_supply':{
            'relative_path':'runtime/run_plans/cn_stage_d_expanded_supply_audit_cbaaaee_20260820.json',
            'file_sha256':sha(supply_path),
            'payload_sha256':supply_hash,
            'raw_ordinal_offset':1024,
            'fresh_unique_count':3584,
            'effective_spent_exact_count':4718,
        },
        'cohort':{
            'selection_seed':COHORT_SEED,
            'records_per_template':COHORT_PER_TEMPLATE,
            'total_records':2016,
            'templates':list(TEMPLATES),
            'exact_identities_sha256':stable_hash(sorted(exacts)),
            'physical_order':physical_order,
            'physical_order_sha256':stable_hash(physical_order),
            'evolution_normalized_identity_by_physical_exact':dict(sorted(normalized_by_physical.items())),
            'evolution_identity_mapping_sha256':stable_hash(dict(sorted(normalized_by_physical.items()))),
            'candidates':cohort,
        },
        'policy':{
            'primary_policy_id':PRIMITIVE_POLICY_ID,
            'primary_policy_order_by_template':primitive_orders,
            'uniform_policy_id':UNIFORM_POLICY_ID,
            'uniform_seeds':list(UNIFORM_SEEDS),
            'uniform_orders_by_seed_and_template':uniform_orders,
            'typed_evolution':{
                'policy_id':TYPED_EVOLUTION_POLICY_ID,
                'causal_replay_only':True,
                'warmup':32,'tournament_size':4,'population_limit':256,'template_cell_limit':64,'seed':2718281,
            },
        },
        'evaluation':{
            'budgets_per_template':list(BUDGETS_PER_TEMPLATE),
            'total_budgets':[7*x for x in BUDGETS_PER_TEMPLATE],
            'primary_total_budget':1008,
            'confirmatory_victory_gate':{
                'primitive_vs_uniform_mean_min_ratio':{'168':1.15,'336':1.12,'504':1.10,'1008':1.08},
                'primitive_vs_typed_evolution_min_ratio_at_1008':1.05,
                'primitive_vs_typed_evolution_min_absolute_delta_at_1008':5,
                'minimum_templates_won_vs_uniform_mean_at_1008':5,
                'minimum_templates_won_vs_evolution_at_1008':4,
                'maximum_single_template_fraction_of_positive_productive_gain':0.50,
                # New confirmatory collapse guard: diversity need not beat random, but must remain broad in absolute and relative terms.
                'minimum_distinct_behavior_pair_rate_at_1008':2/3,
                'minimum_distinct_behavior_pair_count_vs_uniform_mean_ratio_at_1008':0.80,
                'all_restricted_reads_zero':True,
            },
            'classification':{
                'pass':'PRIMITIVE_LOCAL_LARGE_SCALE_CONFIRMATION_PASS_STAGE_D',
                'fail':'PRIMITIVE_LOCAL_LARGE_SCALE_CONFIRMATION_FAIL_STAGE_D',
            },
        },
        'stage_d_financial_labels_read_during_freeze':False,
        'candidate_evaluation_executed':False,
        'validation_reads':0,'holdout_reads':0,'forward_2026_reads':0,
        'promotion_authorized':False,'oos_authority':'NONE',
    }
    body=dict(payload); payload['prefreeze_payload_sha256']=stable_hash(body)
    out=a.output if a.output.is_absolute() else root/a.output; out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'status':payload['status'],'records':2016,'per_template':COHORT_PER_TEMPLATE,'payload':payload['prefreeze_payload_sha256'],'cohort_sha':payload['cohort']['exact_identities_sha256'],'primitive_stats_payload':stats_hash,'primitive_score_source_sha':stagec_builder_sha},sort_keys=True))
    return 0

if __name__=='__main__': raise SystemExit(main())
