from __future__ import annotations
import argparse, json
from pathlib import Path
from typing import Any, Mapping
from our_system_phase2.services.unified_capability_registry import stable_hash

def read(p:Path)->dict[str,Any]: return json.loads(p.read_text(encoding='utf-8-sig'))
def read_jsonl(p:Path)->list[dict[str,Any]]: return [json.loads(x) for x in p.read_text(encoding='utf-8-sig').splitlines() if x.strip()]
def verify(x:Mapping[str,Any],field:str,label:str)->str:
    b=dict(x); c=str(b.pop(field,''))
    if not c or stable_hash(b)!=c: raise RuntimeError(f'{label} self-hash drift')
    return c

def extract(root:Path, target_path:Path|None)->dict[str,Any]:
    root=root.resolve()
    result_by_ordinal={}
    for cp in range(14):
        p=root/f'checkpoint_{cp:04d}'/'candidate_results.jsonl'
        for r in read_jsonl(p): result_by_ordinal[int(r['main_record_ordinal'])]=str(r['exact_identity'])
    if len(result_by_ordinal)!=336: raise RuntimeError('result ordinal coverage drift')
    target=None
    if target_path is not None:
        members=read_jsonl(target_path.resolve()); target={str(r['exact_identity']) for r in members}
        if not members or len(target)!=len(members): raise RuntimeError('target exact coverage/duplicate drift')
    rows=[]; skipped_no_pair=0
    for cp in range(14):
        rr=root/f'checkpoint_{cp:04d}'/'records'; files=sorted(rr.glob('record_*.json'))
        if len(files)!=24: raise RuntimeError(f'checkpoint {cp} record count drift: {len(files)}')
        for p in files:
            x=read(p); h=verify(x,'record_payload_sha256',p.name)
            exact=result_by_ordinal[int(x['main_record_ordinal'])]
            if target is not None and exact not in target: continue
            if x.get('primary') is None or x.get('base_control') is None:
                skipped_no_pair += 1
                continue
            prim={w['window_id']:dict(w) for w in x['primary']['development_subwindows']}
            ctrl={w['window_id']:dict(w) for w in x['base_control']['development_subwindows']}
            ids=('development_1','development_2','development_3')
            if set(prim)!=set(ids) or set(ctrl)!=set(ids): raise RuntimeError('window ids drift')
            pw=[float(prim[i]['cumulative_net_return']) for i in ids]; cw=[float(ctrl[i]['cumulative_net_return']) for i in ids]
            sessions=[int(prim[i]['session_count']) for i in ids]
            if sessions != [int(ctrl[i]['session_count']) for i in ids]: raise RuntimeError('session count drift')
            rows.append({'exact_identity':exact,'program_id':str(x['program_id']),'control_program_id':str(x['control_program_id']),'template_id':str(x['template_id']),'checkpoint_ordinal':cp,'main_record_ordinal':int(x['main_record_ordinal']),'primary_cumulative_net_return':float(x['primary']['cumulative_net_return']),'control_cumulative_net_return':float(x['base_control']['cumulative_net_return']),'matched_cumulative_return_increment':float(x['matched_cumulative_return_increment']),'window_ids':list(ids),'session_counts':sessions,'primary_window_returns':pw,'control_window_returns':cw,'matched_window_return_increments':[a-b for a,b in zip(pw,cw,strict=True)],'source_record_payload_sha256':h})
    expected=len(target) if target is not None else 336-skipped_no_pair
    if len(rows)!=expected or len({r['exact_identity'] for r in rows})!=expected: raise RuntimeError(f'exact coverage drift rows={len(rows)} expected={expected} skipped={skipped_no_pair}')
    if target is not None and {r['exact_identity'] for r in rows}!=target: raise RuntimeError('target exact set not fully recovered')
    rows.sort(key=lambda r:r['main_record_ordinal'])
    payload={'schema_version':'cn_search_core_v2_wave1_development_pair_windows_v1','status':'RECOVERED_FROM_IMMUTABLE_DEVELOPMENT_RESULTS_NO_FINANCIAL_READ','source_output_root':str(root),'candidate_count':len(rows),'targeted_shortlist':target is not None,'skipped_no_pair_count':skipped_no_pair,'rows':rows,'research_boundaries':{'financial_evaluation_performed':False,'validation_read':False,'holdout_read':False,'forward_read':False,'promotion_authorized':False}}
    payload['evidence_payload_sha256']=stable_hash(payload); return payload

def main(argv=None):
    ap=argparse.ArgumentParser();ap.add_argument('--root',type=Path,required=True);ap.add_argument('--target-exacts-jsonl',type=Path);ap.add_argument('--output',type=Path,required=True);a=ap.parse_args(argv)
    x=extract(a.root,a.target_exacts_jsonl);a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(x,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8');print(json.dumps({'status':x['status'],'count':x['candidate_count'],'targeted':x['targeted_shortlist'],'payload':x['evidence_payload_sha256'],'output':str(a.output)},sort_keys=True));return 0
if __name__=='__main__': raise SystemExit(main())
