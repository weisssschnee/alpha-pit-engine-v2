"""Freeze fresh D1 continuation C for prospective Transfer Filter V2 validation.

All development-productive Programs are frozen before validation access. Filter
V2 top-40% membership is frozen as a label, but every development-positive
Program remains in the report-only validation population so precision and
recall can be measured prospectively.
"""
from __future__ import annotations

import argparse, hashlib, json, sys
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT=Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT/'src') not in sys.path: sys.path.insert(0,str(PROJECT_ROOT/'src'))
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0,str(PROJECT_ROOT))

from scripts.apply_cn_program_optimizer_d1_transfer_filter_v2 import apply_filter
from our_system_phase2.services.unified_capability_registry import stable_hash

D1_CLOSURE='CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE.json'

def _sha256(path:Path)->str:
 h=hashlib.sha256()
 with path.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''): h.update(b)
 return h.hexdigest()
def _read_json(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding='utf-8-sig'))
def _read_jsonl(path:Path)->list[dict[str,Any]]: return [json.loads(x) for x in path.read_text(encoding='utf-8-sig').splitlines() if x.strip()]
def _write_json(path:Path,payload:Mapping[str,Any])->Path:
 path.parent.mkdir(parents=True,exist_ok=True); path.write_text(json.dumps(dict(payload),ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8'); return path
def _write_jsonl(path:Path,rows:Sequence[Mapping[str,Any]])->Path:
 path.parent.mkdir(parents=True,exist_ok=True); path.write_text(''.join(json.dumps(dict(x),ensure_ascii=False,sort_keys=True)+'\n' for x in rows),encoding='utf-8'); return path

def freeze(*,run_root:Path,filter_path:Path,output_root:Path,source_cohort:str='D1_CONTINUATION_C')->dict[str,Any]:
 run_root=run_root.resolve(); output_root=output_root.resolve()
 if output_root.exists(): raise FileExistsError(output_root)
 closure_path=run_root/D1_CLOSURE; closure=_read_json(closure_path); body=dict(closure); claimed=str(body.pop('closure_payload_sha256',''))
 if not claimed or stable_hash(body)!=claimed or closure.get('status')!='CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE': raise RuntimeError('C closure drift')
 if int(closure.get('closed_waves') or 0)!=20 or int(closure.get('logical_records') or 0)!=140 or int(closure.get('physical_evaluation_calls') or 0)!=140: raise RuntimeError('C closure cardinality drift')
 if any(int(v)!=0 for v in (closure.get('restricted_reads') or {}).values()): raise RuntimeError('C restricted read drift')
 app_path=output_root/'transfer_filter_application.json'
 application=apply_filter(run_root=run_root,filter_path=filter_path.resolve(),output_path=app_path)
 by_exact={str(x['exact_identity']):dict(x) for x in application['all_candidates']}
 members=[]
 for wave in range(20):
  wr=run_root/f'wave_{wave:03d}'; manifest_path=wr/'wave_manifest.json'; manifest_sha=_sha256(manifest_path)
  asks={str(x['exact_identity']):x for x in _read_jsonl(wr/'logical_asks.jsonl')}
  schedules={str(x.get('d1_exact_identity') or x.get('successor_exact_identity')):x for x in _read_jsonl(wr/'physical_schedules.jsonl')}
  results={str(x['exact_identity']):x for x in _read_jsonl(wr/'physical_results.jsonl')}
  for exact,filt in by_exact.items():
   if int(filt['wave_index'])!=wave: continue
   ask=asks.get(exact); schedule=schedules.get(exact); result=results.get(exact)
   if ask is None or schedule is None or result is None: raise RuntimeError(f'C freeze lineage missing: {exact}')
   credit=dict((result.get('uplift') or {}).get('program_credit') or {})
   if not bool(result['admission']['admitted']) or float(credit.get('matched_cumulative_net_return_increment') or 0)<=0 or float(credit.get('matched_net_reward_increment') or 0)<=0: raise RuntimeError('V2 application includes non-productive Program')
   members.append({
    'candidate_id':exact,'exact_identity':exact,'source_cohort':source_cohort,'source_root':str(run_root),'source_wave':wave,'source_wave_manifest_sha256':manifest_sha,
    'logical_proposal_id':str(ask['logical_proposal_id']),'template_id':str(ask['template_id']),'selection_kind':str(ask['selection_kind']),
    'schedule_record_sha256':str(schedule['schedule_record_sha256']),'program_id':str(schedule['primary_program']['program_id']),'control_program_id':str(schedule['control_program']['program_id']),'pair_id':str(schedule['pair_id']),
    'physical_result_hash':str(result['physical_result_hash']),'source_record_sha256':str(result['source_record_sha256']),
    'development_admitted':True,'development_matched_cumulative_net_return_increment':float(filt['dev_matched_return']),'development_matched_net_reward_increment':float(filt['dev_matched_reward']),
    'development_window_return_increments':[float(filt['dev_window_1_increment']),float(filt['dev_window_2_increment']),float(filt['dev_window_3_increment'])],
    'transfer_linear_score':float(filt['transfer_v2_linear_score']),'transfer_probability_score':float(filt['transfer_v2_probability_score']),'transfer_filter_selected':bool(filt['transfer_filter_v2_selected']),
   })
 members.sort(key=lambda r:str(r['exact_identity']))
 if len(members)!=int(application['development_productive_count']) or len({m['exact_identity'] for m in members})!=len(members): raise RuntimeError('C prospective freeze cardinality drift')
 selected=sorted(m['exact_identity'] for m in members if m['transfer_filter_selected'])
 if len(selected)!=int(application['selected_count']): raise RuntimeError('C V2 selected count drift')
 output_root.mkdir(parents=True,exist_ok=True); members_path=_write_jsonl(output_root/'validation_candidate_members.jsonl',members)
 freeze_payload={
  'schema_version':'cn_program_optimizer_d1_transfer_prospective_validation_freeze_v1','status':'FROZEN_BEFORE_VALIDATION_ACCESS','source_cohort':source_cohort,
  'source_development_root':str(run_root),'source_development_closure_file_sha256':_sha256(closure_path),'source_development_closure_payload_sha256':claimed,
  'candidate_count':len(members),'candidate_exact_identities_sha256':stable_hash(sorted(m['exact_identity'] for m in members)),'candidate_members_payload_sha256':stable_hash(members),
  'transfer_filter_application_file_sha256':_sha256(app_path),'transfer_filter_application_payload_sha256':application['application_payload_sha256'],'transfer_filter_id':application['filter_id'],
  'transfer_filter_selected_count':len(selected),'transfer_filter_selected_exact_identities':selected,'transfer_filter_selected_exact_identities_sha256':stable_hash(selected),
  'selection_rule':{'validation_population':'ALL_DEVELOPMENT_PRODUCTIVE_PROGRAMS','filter_membership':'FROZEN_TOP_40_PERCENT_BEFORE_VALIDATION','validation_result_visibility_at_freeze':'FORBIDDEN','post_hoc_ranking':'FORBIDDEN'},
  'validation_contract':{'evaluation_role':'validation','usage':'REPORT_ONLY_PROSPECTIVE_TRANSFER_TEST','optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion':'FORBIDDEN','holdout_reads':0,'forward_2026_reads':0},
 }
 freeze_payload['freeze_payload_sha256']=stable_hash(freeze_payload); freeze_path=_write_json(output_root/'validation_candidate_freeze.json',freeze_payload)
 return {**freeze_payload,'freeze_file_sha256':_sha256(freeze_path),'members_path':str(members_path),'freeze_path':str(freeze_path)}

def main(argv:Sequence[str]|None=None)->int:
 p=argparse.ArgumentParser(description=__doc__); p.add_argument('--run-root',type=Path,required=True); p.add_argument('--filter',type=Path,required=True); p.add_argument('--output-root',type=Path,required=True); p.add_argument('--source-cohort',default='D1_CONTINUATION_C'); a=p.parse_args(argv)
 result=freeze(run_root=a.run_root,filter_path=a.filter,output_root=a.output_root,source_cohort=a.source_cohort); print(json.dumps({k:result[k] for k in ('status','candidate_count','transfer_filter_selected_count','candidate_exact_identities_sha256','freeze_payload_sha256','freeze_file_sha256')},sort_keys=True)); return 0
if __name__=='__main__': raise SystemExit(main())
