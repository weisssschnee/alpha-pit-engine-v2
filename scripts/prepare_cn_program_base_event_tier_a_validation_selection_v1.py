"""Freeze Tier-A BASE_EVENT schedules before any validation access."""
from __future__ import annotations
import argparse, hashlib, json
from pathlib import Path
from typing import Any, Mapping, Sequence

from our_system_phase2.services.unified_capability_registry import stable_hash

FREEZE_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_validation_candidate_freeze_20260823.json')
MEMBERS_RELATIVE_PATH=Path('runtime/run_plans/cn_program_base_event_tier_a_validation_candidate_members_20260823.jsonl')
STATUS='BASE_EVENT_TIER_A_VALIDATION_SELECTION_PREFLIGHT_READY'

def _read(path:Path)->dict[str,Any]:return json.loads(path.read_text(encoding='utf-8-sig'))
def _readjl(path:Path)->list[dict[str,Any]]:return [json.loads(x) for x in path.read_text(encoding='utf-8-sig').splitlines() if x.strip()]
def _write(path:Path,payload:Mapping[str,Any])->Path:path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(payload,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');return path
def _writejl(path:Path,rows:Sequence[Mapping[str,Any]])->Path:path.parent.mkdir(parents=True,exist_ok=True);path.write_text(''.join(json.dumps(dict(x),ensure_ascii=False,sort_keys=True)+'\n' for x in rows),encoding='utf-8');return path
def _sha(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def _self(payload:Mapping[str,Any],field:str,label:str)->str:
 b=dict(payload);c=str(b.pop(field,''));
 if not c or stable_hash(b)!=c:raise RuntimeError(f'{label} self-hash drift')
 return c

def _load(repo:Path)->tuple[dict[str,Any],list[dict[str,Any]]]:
 freeze=_read(repo/FREEZE_RELATIVE_PATH);fh=_self(freeze,'freeze_payload_sha256','candidate freeze');members=_readjl(repo/MEMBERS_RELATIVE_PATH)
 if freeze.get('status')!='FROZEN_BEFORE_VALIDATION_ACCESS' or int(freeze.get('candidate_count') or 0)!=5 or len(members)!=5:raise RuntimeError('candidate freeze cardinality drift')
 if stable_hash(members)!=freeze.get('candidate_members_payload_sha256') or _sha(repo/MEMBERS_RELATIVE_PATH)!=freeze.get('candidate_members_file_sha256'):raise RuntimeError('candidate members drift')
 ids=sorted(str(x['exact_identity']) for x in members)
 if len(set(ids))!=5 or stable_hash(ids)!=freeze.get('candidate_exact_identities_sha256'):raise RuntimeError('candidate exact identity drift')
 return freeze,members

def _resolve(members:Sequence[Mapping[str,Any]])->list[dict[str,Any]]:
 out=[]
 for m in members:
  root=Path(str(m['source_root'])).resolve();cp=root/str(m['source_checkpoint']);manifest=cp/'checkpoint_manifest.json'
  if _sha(manifest)!=str(m['source_checkpoint_manifest_file_sha256']):raise RuntimeError('source checkpoint manifest drift')
  exact=str(m['exact_identity']);sched_sha=str(m['schedule_record_sha256']);matches=[x for x in _readjl(cp/'selected_schedule.jsonl') if str(x.get('fresh_physical_exact_identity') or '')==exact and str(x.get('schedule_record_sha256') or '')==sched_sha]
  if len(matches)!=1:raise RuntimeError(f'frozen schedule cardinality drift:{exact}')
  s=dict(matches[0]);body={k:v for k,v in s.items() if k!='schedule_record_sha256'}
  if stable_hash(body)!=sched_sha:raise RuntimeError('source schedule self-hash drift')
  if str(s['pair_id'])!=str(m['pair_id']) or str(s['primary_program']['program_id'])!=str(m['program_id']) or str(s['control_program']['program_id'])!=str(m['control_program_id']):raise RuntimeError('source schedule economic identity drift')
  recp=cp/'records'/f"record_{int(m['source_main_record_ordinal']):04d}.json"
  if not recp.is_file():
   candidates=list((cp/'records').glob('record_*.json')); recp=next((p for p in candidates if int(_read(p).get('main_record_ordinal') or -1)==int(m['source_main_record_ordinal'])),None)
  if recp is None or str(_read(recp).get('record_payload_sha256') or '')!=str(m['source_record_payload_sha256']):raise RuntimeError('source record binding drift')
  out.append(s)
 return out

def _required(schedules:Sequence[Mapping[str,Any]])->tuple[str,...]:
 fields=set()
 for s in schedules:
  for key in ('primary_compiled','control_compiled'):fields.update(map(str,dict(s[key]).get('physical_leaf_ids') or ()))
 return tuple(sorted(fields))

def prepare(repo:Path,out:Path)->dict[str,Any]:
 repo=repo.resolve();out=out.resolve()
 if out.exists():raise FileExistsError(out)
 out.mkdir(parents=True)
 freeze,members=_load(repo);schedules=_resolve(members);fields=_required(schedules)
 sp=_writejl(out/'resolved_program_schedules.jsonl',schedules)
 payload={'schema_version':'cn_program_base_event_tier_a_validation_selection_preflight_v1','status':STATUS,'candidate_freeze_payload_sha256':freeze['freeze_payload_sha256'],'candidate_exact_identities_sha256':freeze['candidate_exact_identities_sha256'],'candidate_count':5,'resolved_schedule_count':len(schedules),'resolved_schedule_file_sha256':_sha(sp),'source_schedule_record_sha256s':sorted(str(x['schedule_record_sha256']) for x in schedules),'required_physical_leaf_count':len(fields),'required_physical_leaf_ids':list(fields),'required_physical_leaf_ids_sha256':stable_hash(list(fields)),'candidate_evaluation_executed':False,'validation_reads':0,'holdout_reads':0,'historical_2023_reads':0,'forward_b_reads':0,'forward_2026_reads':0,'validation_access_authorized':False,'promotion_authorized':False}
 payload['preflight_payload_sha256']=stable_hash(payload);p=_write(out/'BASE_EVENT_TIER_A_VALIDATION_SELECTION_PREFLIGHT_READY.json',payload);return {**payload,'preflight_file_sha256':_sha(p)}
def main(argv:Sequence[str]|None=None)->int:
 p=argparse.ArgumentParser();p.add_argument('--repo-root',type=Path,required=True);p.add_argument('--output-root',type=Path,required=True);a=p.parse_args(argv);print(json.dumps(prepare(a.repo_root,a.output_root),ensure_ascii=False,sort_keys=True));return 0
if __name__=='__main__':raise SystemExit(main())
