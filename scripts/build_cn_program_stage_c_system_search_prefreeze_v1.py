"""Freeze the cross-template Stage-C system-search cohort and static policies before any Stage-C label exists."""
from __future__ import annotations
import argparse, json, math
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.program_search_optimizer_v1 import program_availability_entries_v1
from our_system_phase2.services.program_search_trained_optimizer_v2 import StructuredMultiHeadSearchV2
from our_system_phase2.services.unified_capability_registry import stable_hash

TEMPLATES=("BASE_EVENT","BASE_MARKET","BASE_MARKET_EVENT","BASE_TEMPORAL","BASE_TEMPORAL_EVENT","BASE_TEMPORAL_MARKET","BASE_TEMPORAL_MARKET_EVENT")
COHORT_PER_TEMPLATE=288
BUDGETS_PER_TEMPLATE=(24,48,72,144)
UNIFORM_SEEDS=(1729,2718,31415,65537,104729,130363,155921,196613)
COHORT_SEED="STAGE_C_SYSTEM_SEARCH_COHORT_V1"
NOVELTY_CYCLE=5
GLOBAL_WEIGHT=0.55
PRIMITIVE_WEIGHT=0.45

def read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))
def verify_self(payload:Mapping[str,Any],field:str,label:str)->str:
    body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed: raise RuntimeError(f"{label} self-hash drift")
    return claimed

def pct_ranks(rows:Sequence[Mapping[str,Any]], key:str)->dict[str,float]:
    ordered=sorted(rows,key=lambda r:(float(r[key]),str(r["exact_identity"])))
    n=len(ordered); return {str(r["exact_identity"]):(i+1)/n for i,r in enumerate(ordered)}

def shrink(stat:Mapping[str,Any]|None,parent:float,strength:float)->float:
    row=dict(stat or {}); n=float(row.get("n") or 0); prod=float(row.get("productive") or 0)
    return (prod+strength*parent)/(n+strength)

def primitive_score(row:Mapping[str,Any],stats:Mapping[str,Any])->tuple[float,float,dict[str,Any]]:
    global_row=dict(stats["global"]); global_p=(float(global_row["productive"])+1)/(float(global_row["n"])+2)
    template=str(row["template_id"]); tstat=dict(dict(stats["template"]).get(template) or {}); template_p=shrink(tstat,global_p,32.0)
    genes=dict(row["program_genes"]); comps=dict(row["components"]); role_ps=[]; exposures=[]; detail={}
    for role,binding in sorted(comps.items()):
        cid=str(dict(binding)["component_id"]); route=str(dict(binding)["route_id"]); skeleton=str(genes.get(f"{role}__skeleton_id") or "")
        role_p=shrink(dict(stats["role"]).get(role),global_p,32.0)
        route_p=shrink(dict(stats["component_route"]).get(f"{role}|{route}"),role_p,24.0)
        skeleton_p=shrink(dict(stats["component_skeleton"]).get(f"{role}|{skeleton}"),route_p,12.0)
        exact_stat=dict(stats["component_exact"]).get(f"{role}|{cid}")
        exact_p=shrink(exact_stat,skeleton_p,6.0)
        n=int(dict(exact_stat or {}).get("n") or 0); exposures.append(n); role_ps.append(exact_p)
        detail[role]={"component_id":cid,"route_id":route,"skeleton_id":skeleton,"exposure":n,"posterior":exact_p,"skeleton_backoff":skeleton_p,"route_backoff":route_p}
    if not role_ps: raise RuntimeError("candidate has no active components")
    geom=math.exp(sum(math.log(max(1e-12,p)) for p in role_ps)/len(role_ps))
    primitive=geom*(0.75+0.25*template_p)
    novelty=sum(1/math.sqrt(n+1.0) for n in exposures)/len(exposures)
    return primitive,novelty,{"global_prior":global_p,"template_backoff":template_p,"roles":detail}

def interleave(exploit:Sequence[str], novelty:Sequence[str])->list[str]:
    output=[]; used=set(); ie=inov=0
    while len(output)<len(exploit):
        take_novel=(len(output)+1)%NOVELTY_CYCLE==0
        source=novelty if take_novel else exploit
        idx=inov if take_novel else ie
        while idx<len(source) and source[idx] in used: idx+=1
        if idx>=len(source):
            source=exploit if take_novel else novelty; idx=ie if take_novel else inov
            while idx<len(source) and source[idx] in used: idx+=1
        if idx>=len(source): break
        exact=str(source[idx]); output.append(exact); used.add(exact)
        if source is novelty: inov=idx+1
        else: ie=idx+1
    if len(output)!=len(exploit) or len(set(output))!=len(output): raise RuntimeError("hybrid interleave coverage drift")
    return output

def main(argv=None)->int:
    p=argparse.ArgumentParser(); root=Path(__file__).resolve().parents[1]
    p.add_argument("--repo-root",type=Path,default=root); p.add_argument("--output",type=Path,default=Path("runtime/run_plans/cn_program_stage_c_system_search_prefreeze_v1.json")); a=p.parse_args(argv); root=a.repo_root.resolve()
    supply_path=root/"runtime/run_plans/cn_stage_c_expanded_supply_audit_d0160c6_20260820.json"; supply=read(supply_path); supply_hash=verify_self(supply,"audit_payload_sha256","expanded supply")
    if supply.get("status")!="ZERO_FINANCIAL_STAGE_C_EXPANDED_SUPPLY_AUDIT_COMPLETE" or int(supply.get("fresh_unique_count") or 0)!=3584 or int(supply.get("spent_exact_count") or 0)!=2702: raise RuntimeError("expanded supply contract drift")
    stats_path=root/"runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"; stats=read(stats_path); stats_hash=verify_self(stats,"stats_payload_sha256","primitive stats")
    if stats.get("status")!="SPENT_DEVELOPMENT_PRIMITIVE_CREDIT_STATS_FROZEN" or int(stats.get("unique_program_exact_count") or 0)!=1392: raise RuntimeError("primitive stats contract drift")
    dataset_path=root/"runtime/run_plans/cn_program_spent_development_dataset_v2_1310_20260820.json"; dataset=read(dataset_path); dataset_hash=verify_self(dataset,"dataset_payload_sha256","trained dataset")
    if len(dataset.get("rows") or [])!=1310: raise RuntimeError("trained dataset count drift")
    replay_path=root/"runtime/run_plans/cn_program_search_core_replay_v2_stagec_evidence_20260820.json"; replay=read(replay_path); replay_hash=verify_self(replay,"replay_payload_sha256","trained replay")
    if replay.get("status")!="OFFLINE_WALK_FORWARD_SEARCH_CORE_REPLAY_COMPLETE" or replay.get("qualified_trained_policy_leader")!="TRAINED_MULTIHEAD_V2": raise RuntimeError("trained replay qualification drift")
    stageb_path=root/"runtime/run_plans/cn_program_primitive_local_stage_b_d0160c6_independent_audit_20260820.json"; stageb=read(stageb_path); stageb_hash=verify_self(stageb,"audit_payload_sha256","stage B audit")
    if stageb.get("status")!="PASS_INDEPENDENT_TERMINAL_AUDIT" or stageb.get("search_transfer_status")!="PRIMITIVE_LOCAL_SEARCH_TRANSFER_PASS_STAGE_C_LARGE_SCALE_AUTHORIZATION_ELIGIBLE": raise RuntimeError("stage B transfer authority drift")

    by_template={t:[] for t in TEMPLATES}
    for row in supply["fresh_entries"]: by_template[str(row["template_id"])].append(dict(row))
    cohort=[]
    for t in TEMPLATES:
        candidates=sorted(by_template[t],key=lambda r:(stable_hash({"seed":COHORT_SEED,"template":t,"exact":r["exact_identity"]}),r["exact_identity"]))
        selected=candidates[:COHORT_PER_TEMPLATE]
        if len(selected)!=COHORT_PER_TEMPLATE: raise RuntimeError(f"underfilled cohort {t}")
        cohort.extend(selected)
    exacts=[str(r["exact_identity"]) for r in cohort]
    if len(cohort)!=2016 or len(set(exacts))!=2016: raise RuntimeError("Stage C cohort coverage drift")
    evolution_entries=program_availability_entries_v1([{"genes":dict(r["program_genes"])} for r in cohort])
    normalized_by_physical={str(row["exact_identity"]):entry.exact_identity for row,entry in zip(cohort,evolution_entries,strict=True)}
    if len(normalized_by_physical)!=2016 or len(set(normalized_by_physical.values()))!=2016:
        raise RuntimeError("Stage C normalized Evolution identity mapping is not one-to-one")

    model=StructuredMultiHeadSearchV2(seed=8261701,beta=0.75).fit(list(dataset["rows"]))
    model_rows=[{"structural_genes":dict(r["program_genes"]),"base_group_id":str(r["base_component_id"])} for r in cohort]
    global_scores=model.score_rows(model_rows)
    for row,score in zip(cohort,global_scores,strict=True):
        row["global_score"]=float(score["score"]); row["global_p_productive"]=float(score["p_productive"])
        primitive,novelty,_detail=primitive_score(row,stats); row["primitive_score"]=primitive; row["novelty_score"]=novelty

    orders={"HYBRID_HIERARCHICAL_V1":{},"GLOBAL_MULTIHEAD_V2":{},"PRIMITIVE_LOCAL_HIERARCHICAL_V1":{},"NOVELTY_ONLY_V1":{},"UNIFORM_HASH_V1":{}}
    for t in TEMPLATES:
        rows=[r for r in cohort if r["template_id"]==t]
        gp=pct_ranks([{**r,"_s":r["global_score"]} for r in rows],"_s")
        pp=pct_ranks([{**r,"_s":r["primitive_score"]} for r in rows],"_s")
        for r in rows: r["hybrid_score"]=GLOBAL_WEIGHT*gp[r["exact_identity"]]+PRIMITIVE_WEIGHT*pp[r["exact_identity"]]
        global_order=[r["exact_identity"] for r in sorted(rows,key=lambda r:(-r["global_score"],r["exact_identity"]))]
        prim_order=[r["exact_identity"] for r in sorted(rows,key=lambda r:(-r["primitive_score"],r["exact_identity"]))]
        novelty_order=[r["exact_identity"] for r in sorted(rows,key=lambda r:(-r["novelty_score"],stable_hash({"novelty":r["exact_identity"]})))]
        exploit=[r["exact_identity"] for r in sorted(rows,key=lambda r:(-r["hybrid_score"],r["exact_identity"]))]
        orders["GLOBAL_MULTIHEAD_V2"][t]=global_order; orders["PRIMITIVE_LOCAL_HIERARCHICAL_V1"][t]=prim_order; orders["NOVELTY_ONLY_V1"][t]=novelty_order; orders["HYBRID_HIERARCHICAL_V1"][t]=interleave(exploit,novelty_order)
    uniform={}
    for seed in UNIFORM_SEEDS:
        uniform[str(seed)]={t:[r["exact_identity"] for r in sorted((x for x in cohort if x["template_id"]==t),key=lambda r:(stable_hash({"seed":seed,"exact":r["exact_identity"]}),r["exact_identity"]))] for t in TEMPLATES}
    orders["UNIFORM_HASH_V1"]=uniform

    by_t_order={t:[str(r["exact_identity"]) for r in cohort if r["template_id"]==t] for t in TEMPLATES}
    physical_order=[by_t_order[t][i] for i in range(COHORT_PER_TEMPLATE) for t in TEMPLATES]
    payload={"schema_version":"cn_program_stage_c_system_search_prefreeze_v1","status":"STAGE_C_SYSTEM_SEARCH_PREFROZEN_BEFORE_FINANCIAL_READ","source_repo_sha":"d0160c6347a8fdb005db9796bd68484c8e1d86d9","cohort":{"selection_seed":COHORT_SEED,"records_per_template":COHORT_PER_TEMPLATE,"total_records":2016,"templates":list(TEMPLATES),"exact_identities_sha256":stable_hash(sorted(exacts)),"physical_order":physical_order,"physical_order_sha256":stable_hash(physical_order),"evolution_normalized_identity_by_physical_exact":dict(sorted(normalized_by_physical.items())),"evolution_identity_mapping_sha256":stable_hash(dict(sorted(normalized_by_physical.items()))),"candidates":cohort},"training_evidence":{"dataset_relative_path":"runtime/run_plans/cn_program_spent_development_dataset_v2_1310_20260820.json","dataset_payload_sha256":dataset_hash,"search_core_replay_relative_path":"runtime/run_plans/cn_program_search_core_replay_v2_stagec_evidence_20260820.json","search_core_replay_payload_sha256":replay_hash,"primitive_stats_relative_path":"runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json","primitive_stats_payload_sha256":stats_hash,"stage_b_audit_relative_path":"runtime/run_plans/cn_program_primitive_local_stage_b_d0160c6_independent_audit_20260820.json","stage_b_audit_payload_sha256":stageb_hash,"expanded_supply_payload_sha256":supply_hash},"policy":{"hybrid_global_weight":GLOBAL_WEIGHT,"hybrid_primitive_weight":PRIMITIVE_WEIGHT,"novelty_reserve_fraction":1/NOVELTY_CYCLE,"orders":orders,"uniform_seeds":list(UNIFORM_SEEDS),"typed_evolution":{"policy_id":"CATALOG_TYPED_EVOLUTION_PROGRAM_V2","causal_replay_only":True,"warmup":32,"tournament_size":4,"population_limit":256,"template_cell_limit":64,"seed":2718281}},"evaluation":{"budgets_per_template":list(BUDGETS_PER_TEMPLATE),"total_budgets":[7*x for x in BUDGETS_PER_TEMPLATE],"primary_total_budget":1008,"system_victory_gate":{"hybrid_vs_uniform_mean_min_ratio":{"168":1.15,"336":1.12,"504":1.10,"1008":1.08},"hybrid_vs_typed_evolution_min_ratio_at_1008":1.05,"hybrid_vs_typed_evolution_min_absolute_delta_at_1008":5,"minimum_templates_won_vs_uniform_mean_at_1008":5,"minimum_templates_won_vs_evolution_at_1008":4,"hybrid_distinct_behavior_pair_count_vs_uniform_mean_min_ratio_at_1008":1.03,"no_single_template_more_than_fraction_of_productive_gain":0.50,"all_restricted_reads_zero":True},"classification":{"pass":"SYSTEM_LARGE_SCALE_SEARCH_VICTORY_STAGE_C","global_only":"GLOBAL_MODEL_LARGE_SCALE_VICTORY_PRIMITIVE_HYBRID_NOT_ADDITIVE","fail":"NO_SYSTEM_LARGE_SCALE_SEARCH_VICTORY"}},"stage_c_financial_labels_read_during_freeze":False,"candidate_evaluation_executed":False,"validation_reads":0,"holdout_reads":0,"forward_2026_reads":0,"promotion_authorized":False,"oos_authority":"NONE"}
    body=dict(payload); payload["prefreeze_payload_sha256"]=stable_hash(body)
    out=a.output if a.output.is_absolute() else root/a.output; out.parent.mkdir(parents=True,exist_ok=True); out.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"status":payload["status"],"records":2016,"per_template":COHORT_PER_TEMPLATE,"payload":payload["prefreeze_payload_sha256"],"cohort_sha":payload["cohort"]["exact_identities_sha256"],"global_model_return_scale":model.return_scale,"global_model_reward_scale":model.reward_scale},sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
