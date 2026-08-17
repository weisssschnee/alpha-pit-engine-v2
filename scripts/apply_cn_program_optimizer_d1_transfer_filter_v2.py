"""Apply frozen D1 transfer Filter V2 to a completed fresh development cohort.

The filter uses only development economics and the chronological matched-return
increments from development_1/2/3. It never reads validation or holdout data.
"""
from __future__ import annotations

import argparse, hashlib, json, math, sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT/"src") not in sys.path: sys.path.insert(0,str(PROJECT_ROOT/"src"))
from our_system_phase2.services.unified_capability_registry import stable_hash

FILTER_RELATIVE_PATH=Path("runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v2_20260817.json")
FILTER_ID="CN_D1_TRANSFER_FILTER_TEMPORAL_ECON_LOGIT_V2"
EXPECTED_FEATURES=("dev_matched_return","dev_matched_reward","dev_window_1_increment","dev_window_2_increment","dev_window_3_increment")
EXPECTED_WINDOWS=("development_1","development_2","development_3")

def _read_json(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding="utf-8-sig"))
def _read_jsonl(p:Path)->list[dict[str,Any]]: return [json.loads(x) for x in p.read_text(encoding="utf-8-sig").splitlines() if x.strip()]
def _sha256(p:Path)->str:
 h=hashlib.sha256()
 with p.open("rb") as f:
  for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
 return h.hexdigest()
def _verify(payload:Mapping[str,Any],field:str)->None:
 body=dict(payload); c=str(body.pop(field,""))
 if not c or stable_hash(body)!=c: raise ValueError(f"self-hash drift: {field}")
def _score(features:Mapping[str,float],flt:Mapping[str,Any])->float:
 if tuple(flt.get("features") or ())!=EXPECTED_FEATURES: raise ValueError("V2 feature contract drift")
 formula=dict(flt["score_formula_raw"]); coeff=dict(formula["coefficients"])
 if set(coeff)!=set(EXPECTED_FEATURES): raise ValueError("V2 coefficient contract drift")
 s=float(formula["intercept"])
 for k in EXPECTED_FEATURES:
  v=float(features[k])
  if not math.isfinite(v): raise ValueError(f"non-finite V2 feature: {k}")
  s+=float(coeff[k])*v
 return s

def _features_from_result(row:Mapping[str,Any])->dict[str,float]|None:
 admission=dict(row.get("admission") or {}); uplift=row.get("uplift")
 if not bool(admission.get("admitted")) or not isinstance(uplift,Mapping): return None
 credit=dict(uplift.get("program_credit") or {}); ret=float(credit.get("matched_cumulative_net_return_increment")); reward=float(credit.get("matched_net_reward_increment"))
 if not (ret>0.0 and reward>0.0): return None
 ids=tuple(map(str,dict(admission.get("metrics") or {}).get("development_window_ids") or ()))
 if ids!=EXPECTED_WINDOWS: raise RuntimeError(f"development window identity drift: {ids}")
 wins=tuple(map(float,credit.get("window_return_increments") or ()))
 if len(wins)!=3 or not all(math.isfinite(v) for v in wins): raise RuntimeError("development window increment drift")
 return {"dev_matched_return":ret,"dev_matched_reward":reward,"dev_window_1_increment":wins[0],"dev_window_2_increment":wins[1],"dev_window_3_increment":wins[2]}

def _development_rows(run_root:Path)->list[dict[str,Any]]:
 rows=[]
 for wave in range(20):
  w=run_root/f"wave_{wave:03d}"
  if not w.is_dir(): raise FileNotFoundError(w)
  logical={str(x["exact_identity"]):x for x in _read_jsonl(w/"logical_asks.jsonl")}
  for result in _read_jsonl(w/"physical_results.jsonl"):
   feats=_features_from_result(result)
   if feats is None: continue
   exact=str(result["exact_identity"]); ask=logical.get(exact)
   if ask is None: raise RuntimeError(f"missing logical ask for {exact}")
   rows.append({"exact_identity":exact,"wave_index":wave,"template_id":str(ask["template_id"]),"selection_kind":str(ask["selection_kind"]),**feats})
 if len({r["exact_identity"] for r in rows})!=len(rows): raise RuntimeError("duplicate development-positive exact")
 return rows

def _rank_and_mark(rows:list[dict[str,Any]],flt:Mapping[str,Any])->tuple[list[dict[str,Any]],int]:
 for row in rows:
  features={k:float(row[k]) for k in EXPECTED_FEATURES}; score=_score(features,flt); row["transfer_v2_linear_score"]=score; row["transfer_v2_probability_score"]=1/(1+math.exp(-score))
 ordered=sorted(rows,key=lambda r:(-float(r["transfer_v2_linear_score"]),str(r["exact_identity"])))
 frac=float(flt["selection_contract"]["selected_fraction"]); k=int(math.ceil(frac*len(ordered))); selected={r["exact_identity"] for r in ordered[:k]}
 for r in rows: r["transfer_filter_v2_selected"]=r["exact_identity"] in selected
 return ordered,k

def apply_filter(*,run_root:Path,filter_path:Path,output_path:Path)->dict[str,Any]:
 flt=_read_json(filter_path); _verify(flt,"filter_payload_sha256")
 if flt.get("filter_id")!=FILTER_ID or flt.get("status")!="FROZEN_BEFORE_C_DEVELOPMENT_AND_VALIDATION": raise ValueError("V2 filter not frozen")
 if tuple(flt.get("development_window_ids") or ())!=EXPECTED_WINDOWS: raise ValueError("V2 window contract drift")
 rows=_development_rows(run_root); minimum=int(flt["prospective_C_validation_acceptance"]["minimum_development_productive_population"])
 if len(rows)<minimum: raise RuntimeError("insufficient development-positive population for V2 prospective test")
 ordered,k=_rank_and_mark(rows,flt)
 out={"schema_version":"cn_program_optimizer_d1_transfer_filter_application_v2","status":"FROZEN_BEFORE_VALIDATION_ACCESS","filter_id":FILTER_ID,"filter_file_sha256":_sha256(filter_path),"filter_payload_sha256":flt["filter_payload_sha256"],"source_run_root":str(run_root.resolve()),"source_closure_file_sha256":_sha256(run_root/"CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE.json"),"development_productive_count":len(rows),"selected_count":k,"selected_fraction_contract":float(flt["selection_contract"]["selected_fraction"]),"selected_exact_identities":[r["exact_identity"] for r in ordered[:k]],"all_candidates":sorted(rows,key=lambda r:str(r["exact_identity"])),"development_window_ids":list(EXPECTED_WINDOWS),"validation_reads":0,"holdout_reads":0,"forward_2026_reads":0,"selection_frozen_before_validation":True,"optimizer_feedback_write":"FORBIDDEN","promotion_authorized":False}
 out["application_payload_sha256"]=stable_hash(out); output_path.parent.mkdir(parents=True,exist_ok=True); output_path.write_text(json.dumps(out,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8"); return out

def main(argv:Sequence[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument("--run-root",type=Path,required=True); p.add_argument("--filter",type=Path,default=PROJECT_ROOT/FILTER_RELATIVE_PATH); p.add_argument("--output",type=Path,required=True); a=p.parse_args(argv); r=apply_filter(run_root=a.run_root,filter_path=a.filter,output_path=a.output); print(json.dumps({k:r[k] for k in ("status","development_productive_count","selected_count","application_payload_sha256")},sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
