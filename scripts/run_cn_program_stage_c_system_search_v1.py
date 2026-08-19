"""Large-scale 2016-exact prospective Stage-C search benchmark across seven Program templates."""
from __future__ import annotations
import argparse, json, math, statistics, time
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_joint_program_phase_c_v0 as engine
from scripts import run_cn_program_optimizer_large_fresh_v1 as large_fresh
from scripts import run_cn_program_optimizer_successor_benchmark_v1 as successor
from our_system_phase2.services.program_search_optimizer_historical_v2 import CatalogTypedEvolutionProgramV2
from our_system_phase2.services.program_search_optimizer_v1 import ProgramOptimizerObservationV1, program_availability_entries_v1
from our_system_phase2.services.search_v2_admission import AbsoluteEconomicAdmission
from our_system_phase2.services.search_v2_conditional_uplift import MATCHED_CONTROL_CONTRACT_ID, ProgramUpliftCredit
from our_system_phase2.services.unified_capability_registry import stable_hash

CHECKPOINT_SIZE=48
PRIMARY_EXECUTOR_WORKERS=24
RESOURCE_CANARY_PROBE_SECONDS=30.0
TEMPLATES=("BASE_EVENT","BASE_MARKET","BASE_MARKET_EVENT","BASE_TEMPORAL","BASE_TEMPORAL_EVENT","BASE_TEMPORAL_MARKET","BASE_TEMPORAL_MARKET_EVENT")
STATIC_POLICIES=("HYBRID_HIERARCHICAL_V1","GLOBAL_MULTIHEAD_V2","PRIMITIVE_LOCAL_HIERARCHICAL_V1","NOVELTY_ONLY_V1")

def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def _read_jsonl(path:Path)->list[dict[str,Any]]: return [json.loads(x) for x in path.read_text(encoding="utf-8-sig").splitlines() if x.strip()]

def verify_prefreeze(path:Path)->dict[str,Any]:
    p=_read(path); body=dict(p); claimed=str(body.pop("prefreeze_payload_sha256",""))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError("STAGE_C_PREFREEZE_HASH_DRIFT")
    if p.get("status")!="STAGE_C_SYSTEM_SEARCH_PREFROZEN_BEFORE_FINANCIAL_READ" or p.get("stage_c_financial_labels_read_during_freeze") is not False or p.get("candidate_evaluation_executed") is not False: raise RuntimeError("STAGE_C_PREFREEZE_STATUS_DRIFT")
    cohort=dict(p["cohort"]); candidates=list(cohort["candidates"]); exacts=[str(r["exact_identity"]) for r in candidates]; physical=list(map(str,cohort["physical_order"]))
    if len(candidates)!=2016 or len(set(exacts))!=2016 or len(physical)!=2016 or set(physical)!=set(exacts) or stable_hash(physical)!=str(cohort["physical_order_sha256"]): raise RuntimeError("STAGE_C_PREFREEZE_COHORT_DRIFT")
    counts=Counter(str(r["template_id"]) for r in candidates)
    if counts!={t:288 for t in TEMPLATES}: raise RuntimeError("STAGE_C_PREFREEZE_TEMPLATE_DRIFT")
    normalized=dict(cohort.get("evolution_normalized_identity_by_physical_exact") or {})
    if set(normalized)!=set(exacts) or len(set(map(str,normalized.values())))!=2016 or stable_hash(dict(sorted(normalized.items())))!=str(cohort.get("evolution_identity_mapping_sha256") or ""):
        raise RuntimeError("STAGE_C_EVOLUTION_IDENTITY_MAPPING_DRIFT")
    orders=dict(dict(p["policy"])["orders"])
    for policy in STATIC_POLICIES:
        by=dict(orders[policy])
        if set(by)!=set(TEMPLATES): raise RuntimeError("STAGE_C_STATIC_POLICY_TEMPLATE_DRIFT")
        for t in TEMPLATES:
            order=list(map(str,by[t])); expected={str(r["exact_identity"]) for r in candidates if str(r["template_id"])==t}
            if len(order)!=288 or len(set(order))!=288 or set(order)!=expected: raise RuntimeError("STAGE_C_STATIC_POLICY_ORDER_DRIFT")
    uniforms=dict(orders["UNIFORM_HASH_V1"])
    if len(uniforms)!=8: raise RuntimeError("STAGE_C_UNIFORM_SEED_COUNT_DRIFT")
    for seed,by in uniforms.items():
        for t in TEMPLATES:
            order=list(map(str,dict(by)[t])); expected={str(r["exact_identity"]) for r in candidates if str(r["template_id"])==t}
            if len(order)!=288 or set(order)!=expected: raise RuntimeError(f"STAGE_C_UNIFORM_ORDER_DRIFT:{seed}:{t}")
    return p

def _load_authority(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
    prior=dict(authorization["source_prior_exact"])
    return successor._load_authority(args,authorization=authorization,repo_sha=repo_sha,campaign_id=str(authorization["campaign_id"]),campaign_profile=str(authorization["campaign_profile"]),prior_freeze_payload_sha256=str(prior["payload_sha256"]),prior_exact_count=int(prior["count"]),prior_exact_identities_sha256=str(prior["exact_identities_sha256"]),prior_identity_field=str(prior["identity_field"]),input_binding_schema_version="cn_program_stage_c_system_search_input_binding_v1",resource_profile_id="SEARCH_DUAL_24",resource_profile_role="SEARCH",maximum_executor_workers=PRIMARY_EXECUTOR_WORKERS)

def _reservoir(candidate:Mapping[str,Any])->dict[str,Any]:
    return {"schema_version":"cn_joint_program_phase_c_reservoir_record_v0","template_id":str(candidate["template_id"]),"components":dict(candidate["components"]),"combination_policy":dict(candidate["combination_policy"]),"raw_combination_sha256":str(candidate["raw_combination_sha256"]),"reservoir_record_sha256":str(candidate["reservoir_record_sha256"]),"semantic_compile_required_at_selection":True,"semantic_noop_does_not_count_toward_quota":True,"financial_evaluation_executed":False}

def _schedule(candidate:Mapping[str,Any],*,authority:Mapping[str,Any],global_ordinal:int,checkpoint_ordinal:int,template_ordinal:int)->dict[str,Any]:
    reservoir=_reservoir(candidate)
    entry=engine._catalog_entry(reservoir,components_by_id=authority["components_by_id"],adapter=authority["adapter"],compiler=authority["compiler"])
    if entry.get("status")!="EXECUTABLE": raise RuntimeError("STAGE_C_CANDIDATE_NOT_EXECUTABLE")
    exact=stable_hash(dict(entry["program_genes"])); template=str(candidate["template_id"])
    if exact!=str(candidate["exact_identity"]) or str(entry["program_id"])!=str(candidate["program_id"]): raise RuntimeError("STAGE_C_CANDIDATE_IDENTITY_DRIFT")
    ask={"schema_version":"cn_program_stage_c_fixed_ask_v1","main_record_ordinal":int(global_ordinal),"checkpoint_ordinal":int(checkpoint_ordinal),"template_id":template,"template_record_ordinal":int(template_ordinal),"generation_arm":"SUCCESSOR_PHYSICAL_DEDUP","matched_control_contract_id":MATCHED_CONTROL_CONTRACT_ID,"absolute_admission_head_eligible":True,"conditional_uplift_head_eligible":True,"stage_c_exact_identity":exact}
    ask["ask_record_sha256"]=stable_hash(ask)
    decision=engine._self_hashed({"schema_version":"cn_program_stage_c_fixed_selection_v1","selection_mode":"PREFROZEN_PHYSICAL_COHORT","stage_c_exact_identity":exact,"adaptive_template_credit_used":False,"optimizer_selection_used":False},"selection_decision_sha256")
    schedule=engine._schedule_record(ask,entry,decision,components_by_id=authority["components_by_id"],adapter=authority["adapter"],compiler=authority["compiler"])
    schedule.update({"stage_c_exact_identity":exact,"successor_exact_identity":exact,"stage_c_program_genes":dict(candidate["program_genes"]),"stage_c_physical_order_ordinal":int(global_ordinal),"optimizer_selection_used":False})
    schedule["schedule_record_sha256"]=stable_hash({k:v for k,v in schedule.items() if k!="schedule_record_sha256"})
    return schedule

def reconstruct_schedules(prefreeze:Mapping[str,Any],*,authority:Mapping[str,Any])->list[dict[str,Any]]:
    cohort=dict(prefreeze["cohort"]); by_exact={str(r["exact_identity"]):dict(r) for r in cohort["candidates"]}; counters=Counter(); schedules=[]
    for idx,exact in enumerate(map(str,cohort["physical_order"])):
        c=by_exact[exact]; t=str(c["template_id"]); schedules.append(_schedule(c,authority=authority,global_ordinal=idx,checkpoint_ordinal=idx//CHECKPOINT_SIZE,template_ordinal=counters[t])); counters[t]+=1
    if len(schedules)!=2016 or len({s["stage_c_exact_identity"] for s in schedules})!=2016: raise RuntimeError("STAGE_C_SCHEDULE_COVERAGE_DRIFT")
    return schedules

def prefinancial_rehearsal(args:argparse.Namespace,*,authorization:Mapping[str,Any],repo_sha:str)->dict[str,Any]:
    pre=verify_prefreeze(args.stage_c_prefreeze); authority=_load_authority(args,authorization=authorization,repo_sha=repo_sha); schedules=reconstruct_schedules(pre,authority=authority); fields=tuple(sorted(engine._checkpoint_field_columns(schedules)))
    return {"status":"ZERO_FINANCIAL_STAGE_C_PREFLIGHT_READY","schedule_count":2016,"checkpoint_count":math.ceil(2016/CHECKPOINT_SIZE),"field_column_count":len(fields),"field_columns":list(fields),"field_columns_sha256":stable_hash(list(fields)),"candidate_evaluation_executed":False,"stage_c_financial_labels_read":False,"validation_reads":0,"holdout_reads":0,"forward_2026_reads":0}

def _behavior_pair(record:Mapping[str,Any])->str|None:
    p=dict(record.get("primary") or {}); c=dict(record.get("base_control") or {}); left=str(p.get("behavior_identity") or ""); right=str(c.get("behavior_identity") or "")
    if not left or not right or left==right: return None
    return stable_hash({"primary":left,"base_control":right})

def _result_record(record:Mapping[str,Any],schedule:Mapping[str,Any])->dict[str,Any]:
    physical=successor._physical_result(record,schedule); admission=physical.admission.to_record(); uplift=None if physical.uplift is None else physical.uplift.to_record()
    payload={"schema_version":"cn_program_stage_c_result_v1","exact_identity":str(schedule["stage_c_exact_identity"]),"template_id":str(schedule["template_id"]),"base_component_id":str(dict(schedule["components"])["base"]["component_id"]),"component_ids":{role:str(binding["component_id"]) for role,binding in dict(schedule["components"]).items()},"admission":admission,"uplift":uplift,"behavior_pair_identity":_behavior_pair(record),"source_record_payload_sha256":str(record["record_payload_sha256"]),"optimizer_selection_used":False,"validation_feedback_used":False}
    payload["result_payload_sha256"]=stable_hash(payload); return payload

def _productive(row:Mapping[str,Any])->bool:
    a=dict(row.get("admission") or {}); u=dict(row.get("uplift") or {}); c=dict(u.get("program_credit") or {})
    return bool(a.get("admitted")) and float(c.get("matched_cumulative_net_return_increment") or 0)>0 and float(c.get("matched_net_reward_increment") or 0)>0

def _metric(rows:Sequence[Mapping[str,Any]])->dict[str,Any]:
    rows=list(rows); n=len(rows); prod=sum(_productive(r) for r in rows); adm=sum(bool(dict(r.get("admission") or {}).get("admitted")) for r in rows); behaviors={str(r["behavior_pair_identity"]) for r in rows if r.get("behavior_pair_identity")}
    return {"evaluated":n,"productive":prod,"productive_rate":prod/n if n else 0.0,"admitted":adm,"admission_rate":adm/n if n else 0.0,"distinct_behavior_pair_count":len(behaviors)}

def _select(by_template:Mapping[str,Sequence[str]],k:int,result_by_exact:Mapping[str,Mapping[str,Any]])->tuple[list[dict[str,Any]],dict[str,dict[str,Any]]]:
    all_rows=[]; per={}
    for t in TEMPLATES:
        rows=[dict(result_by_exact[str(x)]) for x in list(by_template[t])[:k]]; all_rows.extend(rows); per[t]=_metric(rows)
    return all_rows,per

def _admission_obj(row:Mapping[str,Any])->AbsoluteEconomicAdmission:
    a=dict(row["admission"])
    return AbsoluteEconomicAdmission(record_payload_sha256=str(a["record_payload_sha256"]),pair_id=str(a["pair_id"]),program_id=str(a["program_id"]),control_program_id=str(a["control_program_id"]),admitted=bool(a["admitted"]),failure_reasons=tuple(map(str,a.get("failure_reasons") or ())),metrics=dict(a.get("metrics") or {}),policy_id=str(a.get("policy_id") or "CN_SEARCH_V2_ABSOLUTE_ECONOMIC_ADMISSION_V1"))
def _uplift_obj(row:Mapping[str,Any])->ProgramUpliftCredit|None:
    if row.get("uplift") is None: return None
    u=dict(row["uplift"])
    return ProgramUpliftCredit(record_payload_sha256=str(u["record_payload_sha256"]),pair_id=str(u["pair_id"]),program_id=str(u["program_id"]),control_program_id=str(u["control_program_id"]),program_credit=dict(u["program_credit"]),component_attribution_status=str(u.get("component_attribution_status") or "COMPONENT_ATTRIBUTION_UNIDENTIFIED"),component_credits=u.get("component_credits"),policy_id=str(u.get("policy_id") or "CN_SEARCH_V2_CONDITIONAL_FULL_VS_BASE_UPLIFT_V1"))
def _evolution_orders(prefreeze:Mapping[str,Any],result_by_exact:Mapping[str,Mapping[str,Any]])->dict[str,list[str]]:
    cohort=dict(prefreeze["cohort"]); candidates=list(cohort["candidates"])
    entries=program_availability_entries_v1([{"genes":dict(r["program_genes"])} for r in candidates])
    frozen_map={str(k):str(v) for k,v in dict(cohort["evolution_normalized_identity_by_physical_exact"]).items()}
    observed_map={str(row["exact_identity"]):entry.exact_identity for row,entry in zip(candidates,entries,strict=True)}
    if observed_map!=frozen_map or len(set(observed_map.values()))!=2016:
        raise RuntimeError("STAGE_C_EVOLUTION_ENTRY_IDENTITY_MAPPING_DRIFT")
    physical_by_normalized={normalized:physical for physical,normalized in observed_map.items()}
    cfg=dict(dict(prefreeze["policy"])["typed_evolution"]); opt=CatalogTypedEvolutionProgramV2(entries=entries,seen_exact_identities=(),seed=int(cfg["seed"]),warmup=int(cfg["warmup"]),tournament_size=int(cfg["tournament_size"]),population_limit=int(cfg["population_limit"]),template_cell_limit=int(cfg["template_cell_limit"]))
    orders={t:[] for t in TEMPLATES}
    for round_id in range(6):
        for t in TEMPLATES:
            asks=opt.ask(checkpoint_id=f"STAGE_C_EVOLUTION_R{round_id}_{t}",count=24,required_program_template_id=t)
            if len(asks)!=24: raise RuntimeError(f"STAGE_C_EVOLUTION_ASK_COUNT_DRIFT:{round_id}:{t}:{len(asks)}")
            observations=[]
            for ask in asks:
                normalized=str(ask["exact_identity"]); physical=physical_by_normalized[normalized]; result=dict(result_by_exact[physical]); orders[t].append(physical); observations.append(ProgramOptimizerObservationV1(proposal_id=str(ask["proposal_id"]),exact_identity=normalized,admission=_admission_obj(result),uplift=_uplift_obj(result)))
            opt.tell(observations)
    if any(len(v)!=144 or len(set(v))!=144 for v in orders.values()): raise RuntimeError("STAGE_C_EVOLUTION_ORDER_COVERAGE_DRIFT")
    return orders

def _curve(prefreeze:Mapping[str,Any],result_by_exact:Mapping[str,Mapping[str,Any]])->dict[str,Any]:
    ev_orders=_evolution_orders(prefreeze,result_by_exact); orders=dict(dict(prefreeze["policy"])["orders"]); budgets=list(map(int,dict(prefreeze["evaluation"])["budgets_per_template"])); out={"budgets":{},"typed_evolution_orders_by_template":ev_orders}
    for k in budgets:
        static={}; per_static={}
        for policy in STATIC_POLICIES:
            rows,per=_select(dict(orders[policy]),k,result_by_exact); static[policy]=_metric(rows); per_static[policy]=per
        ev_rows,ev_per=_select(ev_orders,k,result_by_exact); ev_metric=_metric(ev_rows)
        uniform_seed={}; uniform_per={}
        for seed,by in sorted(dict(orders["UNIFORM_HASH_V1"]).items()):
            rows,per=_select(dict(by),k,result_by_exact); uniform_seed[str(seed)]=_metric(rows); uniform_per[str(seed)]=per
        mean_prod=statistics.mean(float(x["productive"]) for x in uniform_seed.values()); mean_beh=statistics.mean(float(x["distinct_behavior_pair_count"]) for x in uniform_seed.values())
        hybrid=static["HYBRID_HIERARCHICAL_V1"]
        template_wins_uniform=0; template_wins_evolution=0; positive_gains=[]
        for t in TEMPLATES:
            h=float(per_static["HYBRID_HIERARCHICAL_V1"][t]["productive"]); u=statistics.mean(float(uniform_per[s][t]["productive"]) for s in uniform_per); e=float(ev_per[t]["productive"])
            template_wins_uniform+=int(h>u); template_wins_evolution+=int(h>e); positive_gains.append(max(0.0,h-u))
        total_gain=sum(positive_gains); max_gain_fraction=max(positive_gains)/total_gain if total_gain>0 else 1.0
        out["budgets"][str(7*k)]={"per_template_budget":k,"static":static,"static_per_template":per_static,"typed_evolution":ev_metric,"typed_evolution_per_template":ev_per,"uniform_seed_metrics":uniform_seed,"uniform_seed_mean_productive_count":mean_prod,"uniform_seed_mean_distinct_behavior_pair_count":mean_beh,"hybrid_vs_uniform_productive_ratio":float(hybrid["productive"])/mean_prod if mean_prod else None,"hybrid_vs_uniform_productive_delta":float(hybrid["productive"])-mean_prod,"hybrid_vs_evolution_productive_ratio":float(hybrid["productive"])/float(ev_metric["productive"]) if ev_metric["productive"] else None,"hybrid_vs_evolution_productive_delta":float(hybrid["productive"])-float(ev_metric["productive"]),"hybrid_vs_uniform_behavior_ratio":float(hybrid["distinct_behavior_pair_count"])/mean_beh if mean_beh else None,"hybrid_templates_won_vs_uniform_mean":template_wins_uniform,"hybrid_templates_won_vs_evolution":template_wins_evolution,"hybrid_positive_gain_max_template_fraction":max_gain_fraction}
    gate=dict(dict(prefreeze["evaluation"])["system_victory_gate"]); checks={}
    for budget,minimum in dict(gate["hybrid_vs_uniform_mean_min_ratio"]).items(): checks[f"hybrid_vs_uniform_ratio_{budget}"]=float(out["budgets"][str(budget)]["hybrid_vs_uniform_productive_ratio"] or 0)>=float(minimum)
    primary=out["budgets"]["1008"]; checks.update({"hybrid_vs_evolution_ratio_1008":float(primary["hybrid_vs_evolution_productive_ratio"] or 0)>=float(gate["hybrid_vs_typed_evolution_min_ratio_at_1008"]),"hybrid_vs_evolution_delta_1008":float(primary["hybrid_vs_evolution_productive_delta"])>=float(gate["hybrid_vs_typed_evolution_min_absolute_delta_at_1008"]),"template_wins_uniform_1008":int(primary["hybrid_templates_won_vs_uniform_mean"])>=int(gate["minimum_templates_won_vs_uniform_mean_at_1008"]),"template_wins_evolution_1008":int(primary["hybrid_templates_won_vs_evolution"])>=int(gate["minimum_templates_won_vs_evolution_at_1008"]),"behavior_ratio_1008":float(primary["hybrid_vs_uniform_behavior_ratio"] or 0)>=float(gate["hybrid_distinct_behavior_pair_count_vs_uniform_mean_min_ratio_at_1008"]),"gain_concentration_1008":float(primary["hybrid_positive_gain_max_template_fraction"])<=float(gate["no_single_template_more_than_fraction_of_productive_gain"])})
    # Global-only fallback is intentionally stricter than a narrative rescue: same Uniform ratios and Evolution gate, but no primitive-additivity claim.
    global_checks={}
    for budget,minimum in dict(gate["hybrid_vs_uniform_mean_min_ratio"]).items():
        block=out["budgets"][str(budget)]; g=float(block["static"]["GLOBAL_MULTIHEAD_V2"]["productive"]); u=float(block["uniform_seed_mean_productive_count"]); global_checks[f"global_vs_uniform_ratio_{budget}"]=g/u>=float(minimum) if u else False
    g1008=float(primary["static"]["GLOBAL_MULTIHEAD_V2"]["productive"]); e1008=float(primary["typed_evolution"]["productive"]); global_checks["global_vs_evolution_ratio_1008"]=(g1008/e1008>=float(gate["hybrid_vs_typed_evolution_min_ratio_at_1008"])) if e1008 else False; global_checks["global_vs_evolution_delta_1008"]=(g1008-e1008)>=float(gate["hybrid_vs_typed_evolution_min_absolute_delta_at_1008"])
    global_per=primary["static_per_template"]["GLOBAL_MULTIHEAD_V2"]; evolution_per=primary["typed_evolution_per_template"]
    uniform_per_seed={}
    for seed,by in dict(orders["UNIFORM_HASH_V1"]).items():
        _rows,per=_select(dict(by),144,result_by_exact); uniform_per_seed[str(seed)]=per
    global_wins_uniform=0; global_wins_evolution=0; global_positive_gains=[]
    for t in TEMPLATES:
        g=float(global_per[t]["productive"]); u=statistics.mean(float(uniform_per_seed[s][t]["productive"]) for s in uniform_per_seed); e=float(evolution_per[t]["productive"])
        global_wins_uniform+=int(g>u); global_wins_evolution+=int(g>e); global_positive_gains.append(max(0.0,g-u))
    global_gain=sum(global_positive_gains); global_concentration=max(global_positive_gains)/global_gain if global_gain>0 else 1.0
    global_behavior_ratio=float(primary["static"]["GLOBAL_MULTIHEAD_V2"]["distinct_behavior_pair_count"])/float(primary["uniform_seed_mean_distinct_behavior_pair_count"]) if float(primary["uniform_seed_mean_distinct_behavior_pair_count"])>0 else 0.0
    global_checks.update({"global_templates_won_uniform_1008":global_wins_uniform>=int(gate["minimum_templates_won_vs_uniform_mean_at_1008"]),"global_templates_won_evolution_1008":global_wins_evolution>=int(gate["minimum_templates_won_vs_evolution_at_1008"]),"global_behavior_ratio_1008":global_behavior_ratio>=float(gate["hybrid_distinct_behavior_pair_count_vs_uniform_mean_min_ratio_at_1008"]),"global_gain_concentration_1008":global_concentration<=float(gate["no_single_template_more_than_fraction_of_productive_gain"])})
    if all(checks.values()): status=dict(dict(prefreeze["evaluation"])["classification"])["pass"]
    elif all(global_checks.values()): status=dict(dict(prefreeze["evaluation"])["classification"])["global_only"]
    else: status=dict(dict(prefreeze["evaluation"])["classification"])["fail"]
    out["victory_checks"]=checks; out["global_only_checks"]=global_checks; out["status"]=status; return out

def _close_checkpoint(inflight:Path,closed:Path,previous_sha:str,checkpoint:int)->str:
    artifacts=[successor._artifact(p,inflight) for p in sorted(inflight.rglob("*")) if p.is_file() and p.name!="checkpoint_manifest.json"]
    manifest=engine._self_hashed({"schema_version":"cn_program_stage_c_checkpoint_manifest_v1","status":"STAGE_C_CHECKPOINT_CLOSED_IMMUTABLE","checkpoint_ordinal":checkpoint,"previous_checkpoint_manifest_file_sha256":previous_sha,"artifacts":artifacts},"manifest_payload_sha256")
    engine._write_json(inflight/"checkpoint_manifest.json",manifest); inflight.replace(closed); return engine._sha256(closed/"checkpoint_manifest.json")

def run(args:argparse.Namespace,*,admission:Mapping[str,Any],authorization:Mapping[str,Any])->dict[str,Any]:
    root=args.output_root.resolve()
    if not root.is_dir() or not (root/".project_control_execution").is_dir() or {p.name for p in root.iterdir()}!={".project_control_execution"}: raise RuntimeError("STAGE_C_ADMITTED_ROOT_NOT_CLEAN")
    pre=verify_prefreeze(args.stage_c_prefreeze); authority=_load_authority(args,authorization=authorization,repo_sha=str(admission["repo_sha"])); schedules=reconstruct_schedules(pre,authority=authority)
    engine._write_json(root/"input_binding.json",authority["input_binding"]); engine._write_json(root/"policy_binding.json",{"schema_version":"cn_program_stage_c_policy_binding_v1","prefreeze_payload_sha256":pre["prefreeze_payload_sha256"],"cohort_exact_identities_sha256":pre["cohort"]["exact_identities_sha256"],"stage_c_financial_labels_read_during_policy_freeze":False,"physical_evaluation_order_independent_of_policy":True})
    input_hash=str(authority["input_binding"]["input_binding_sha256"]); fields=tuple(sorted(engine._checkpoint_field_columns(schedules))); old=large_fresh.RESOURCE_CANARY_PROBE_SECONDS
    try: large_fresh.RESOURCE_CANARY_PROBE_SECONDS=RESOURCE_CANARY_PROBE_SECONDS; canary=large_fresh._resource_canary(authority,input_hash,PRIMARY_EXECUTOR_WORKERS,fields)
    finally: large_fresh.RESOURCE_CANARY_PROBE_SECONDS=old
    if canary.get("status")!="PASS_ZERO_CANDIDATE_EVALUATION_RESOURCE_CANARY" or int(canary.get("requested_workers") or 0)!=24 or int(canary.get("pagefile_pages_in_delta_bytes",-1))!=0 or int(canary.get("pagefile_pages_out_delta_bytes",-1))!=0: raise RuntimeError("STAGE_C_RESOURCE_CANARY_FAIL")
    engine._write_json(root/"resource_canary.json",canary); started=time.perf_counter(); previous="GENESIS"; results=[]
    for cp in range(math.ceil(len(schedules)/CHECKPOINT_SIZE)):
        batch=schedules[cp*CHECKPOINT_SIZE:(cp+1)*CHECKPOINT_SIZE]; inflight=root/f"checkpoint_{cp:04d}.inflight"; closed=root/f"checkpoint_{cp:04d}"; inflight.mkdir(); engine._write_jsonl(inflight/"selected_schedule.jsonl",batch)
        records=successor._evaluate_schedules(batch,record_root=inflight/"records",authority=authority,input_hash=input_hash,executor_workers=PRIMARY_EXECUTOR_WORKERS); by={int(s["main_record_ordinal"]):s for s in batch}; rows=[_result_record(r,by[int(r["main_record_ordinal"])]) for r in records]; engine._write_jsonl(inflight/"candidate_results.jsonl",rows); previous=_close_checkpoint(inflight,closed,previous,cp); results.extend(rows)
    by_exact={str(r["exact_identity"]):r for r in results}
    if len(results)!=2016 or len(by_exact)!=2016: raise RuntimeError("STAGE_C_RESULT_COVERAGE_DRIFT")
    benchmark=_curve(pre,by_exact); engine._write_json(root/"system_search_benchmark.json",benchmark)
    closure=engine._self_hashed({"schema_version":"cn_program_stage_c_system_search_complete_v1","status":"STAGE_C_SYSTEM_SEARCH_COMPLETE","repo_sha":str(admission["repo_sha"]),"authorization_payload_sha256":str(authorization["authorization_payload_sha256"]),"prefreeze_payload_sha256":str(pre["prefreeze_payload_sha256"]),"stage_c_evaluated":2016,"checkpoint_count":math.ceil(2016/CHECKPOINT_SIZE),"last_checkpoint_manifest_file_sha256":previous,"system_search_status":benchmark["status"],"system_search_benchmark":benchmark,"full_cohort_metric":_metric(results),"wall_seconds":time.perf_counter()-started,"evaluation_data_role":"DEVELOPMENT_ONLY","restricted_reads":{"validation":0,"holdout":0,"historical_2023":0,"forward_b":0,"forward_2026":0},"validation_feedback_used":False,"promotion_authorized":False,"oos_authority":"NONE","automatic_successor_authorized":False},"closure_payload_sha256"); engine._write_json(root/"CN_PROGRAM_STAGE_C_SYSTEM_SEARCH_COMPLETE.json",closure); return closure

__all__=["verify_prefreeze","reconstruct_schedules","prefinancial_rehearsal","_curve","run"]
