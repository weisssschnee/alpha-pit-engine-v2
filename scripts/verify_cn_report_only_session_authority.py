"""Independently verify a role-bound report-only session authority."""
from __future__ import annotations
import argparse,json
from datetime import datetime,timezone
from pathlib import Path
from typing import Any
import pandas as pd
from scripts import build_cn_finalist_session_authority as base
from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import build_cn_validation_session_authority as build

def verify_report_only_session_authority(*,authority_root:Path,output_root:Path,evaluation_role:str,date_min:str,date_max:str,expected_schema_version:str,expected_status:str)->dict[str,Any]:
    root=authority_root.resolve(); out=output_root.resolve(); out.mkdir(parents=True,exist_ok=False); prefix=evaluation_role
    manifest_path=root/f'{prefix}_session_authority_manifest.json'; m=v1._read_json(manifest_path); body=dict(m); claim=str(body.pop('manifest_payload_sha256',''))
    if not claim or v1._stable_hash(body)!=claim: raise RuntimeError('report-only session authority self-hash mismatch')
    role_reads='forward_2026_reads' if evaluation_role=='forward_2026' else f'{evaluation_role}_reads'
    required={'schema_version':expected_schema_version,'status':expected_status,'evaluation_role':evaluation_role,'data_role':f'{evaluation_role}_report_only','date_min':date_min,'date_max':date_max,'allowed_exchanges':list(build.ALLOWED_EXCHANGES),'optimizer_feedback_write':'FORBIDDEN','scheduler_write':'FORBIDDEN','archive_write':'FORBIDDEN','promotion':'FORBIDDEN'}
    drift=[k for k,v in required.items() if m.get(k)!=v]
    if drift: raise RuntimeError('report-only session authority drift:'+','.join(drift))
    if int(m.get(role_reads) or 0)<=0: raise RuntimeError(f'{evaluation_role} session authority has no role reads')
    for k in {'validation_reads','holdout_reads','historical_challenge_reads','forward_2026_reads'}-{role_reads}:
        if int(m.get(k) or 0)!=0: raise RuntimeError(f'{evaluation_role} session authority crossed {k}')
    if int(m.get('missing_exact_st_fail_closed_session_count',-1))!=0: raise RuntimeError('report-only session authority has missing ST states')
    if int(m.get('exact_st_session_count',-1))!=int(m.get('observed_session_row_count',-2)): raise RuntimeError('report-only ST coverage drift')
    if int(m.get('non_st_authority_session_count') or 0)<=0: raise RuntimeError('report-only authority has no non-ST sessions')
    if list(m.get('columns') or ())!=list(base.SESSION_AUTHORITY_COLUMNS): raise RuntimeError('report-only authority column drift')
    for a in m.get('artifacts') or ():
        p=root/str(a['path'])
        if not p.is_file() or p.stat().st_size!=int(a['bytes']) or v1._sha256(p)!=str(a['sha256']): raise RuntimeError(f'report-only authority artifact drift:{p}')
    field_path=Path(str(m['field_manifest'])).resolve()
    if v1._sha256(field_path)!=str(m['field_manifest_sha256']): raise RuntimeError('report-only field manifest binding drift')
    field=v1._read_json(field_path)
    if field.get('status')!='TIME_MAJOR_LAYOUT_PARITY_PASS' or field.get('evaluation_role')!=evaluation_role or field.get('data_role')!=f'{evaluation_role}_report_only' or int(field.get(role_reads) or 0)<=0: raise RuntimeError('report-only field evidence drift')
    for k in {'validation_reads','holdout_reads','historical_challenge_reads','forward_2026_reads'}-{role_reads}:
        if int(field.get(k) or 0)!=0: raise RuntimeError(f'report-only field crossed {k}')
    source_root=Path(str(m['public_source_snapshot_manifest'])).parent; source=base.verify_source_snapshot(source_root)
    if source['manifest_file_sha256']!=str(m['public_source_snapshot_manifest_sha256']): raise RuntimeError('public source snapshot binding drift')
    st=Path(str(m['daily_st_source'])).resolve()
    if not st.is_file() or v1._sha256(st)!=str(m['daily_st_source_sha256']): raise RuntimeError('daily ST source binding drift')
    authority=pd.read_parquet(root/f'{prefix}_session_authority.parquet'); observed=pd.read_parquet(root/f'{prefix}_observed_sessions.parquet'); excluded=v1._read_json(root/f'excluded_{prefix}_codes.json'); excluded_st=pd.read_parquet(root/'excluded_missing_st_coordinates.parquet')
    for frame in (authority,observed,excluded_st):
        frame['date']=pd.to_datetime(frame['date'],errors='raise').dt.normalize();frame['code']=frame['code'].map(base.normalize_code)
    for name,frame in (('authority',authority),('observed',observed),('excluded_st',excluded_st)):
        if frame.duplicated(['date','code']).any(): raise RuntimeError(f'report-only {name} duplicate coordinates')
    if set(authority['exchange'].astype(str))-set(build.ALLOWED_EXCHANGES): raise RuntimeError('report-only authority prohibited exchange')
    dates=pd.DatetimeIndex(authority['date'].unique()).sort_values()
    if dates[0]!=pd.Timestamp(date_min) or dates[-1]!=pd.Timestamp(date_max) or len(dates)!=int(m.get(f'{evaluation_role}_date_count') or -1): raise RuntimeError('report-only authority calendar drift')
    if len(authority)!=int(m['authority_session_row_count']) or len(observed)!=int(m['observed_session_row_count']): raise RuntimeError('report-only authority row cardinality drift')
    if observed['is_st'].isna().any(): raise RuntimeError('report-only observed ST incomplete')
    exact,_,_=build._extract_exact_st(source_path=st,expected_source_sha256=str(m['daily_st_source_sha256']),validation_dates=tuple(dates.date),evaluation_role=evaluation_role,date_min=date_min,date_max=date_max)
    joined=observed[['date','code','is_st']].merge(exact,on=['date','code'],how='left',suffixes=('_observed','_source'),validate='one_to_one')
    if joined['is_st_source'].isna().any() or not joined['is_st_observed'].eq(joined['is_st_source']).all(): raise RuntimeError('report-only observed ST source parity failure')
    ex=excluded_st[['date','code']].merge(exact,on=['date','code'],how='left',validate='one_to_one')
    if ex['is_st'].notna().any(): raise RuntimeError('report-only ST exclusion has exact source')
    if len(excluded_st)!=int(m['excluded_missing_exact_st_session_count']): raise RuntimeError('report-only ST exclusion count drift')
    joined2=observed.merge(authority,on=['date','code'],how='left',suffixes=('_observed','_authority'),validate='one_to_one')
    if joined2['exchange'].isna().any() or not joined2['is_st_observed'].eq(joined2['is_st_authority']).all() or joined2['suspended'].any(): raise RuntimeError('report-only observed/authority parity failure')
    if int((~authority['is_st'].astype(bool)).sum())!=int(m['non_st_authority_session_count']): raise RuntimeError('report-only non-ST count drift')
    expected_total=int(excluded['excluded_non_sse_szse_code_count'])+int(excluded['excluded_missing_corporate_action_source_code_count'])+int(excluded['excluded_incomplete_corporate_action_source_code_count'])
    if int(excluded['excluded_code_count'])!=expected_total: raise RuntimeError('report-only total exclusion count drift')
    receipt={'schema_version':'cn_report_only_session_authority_audit_v1','status':'PASS_INDEPENDENT_REPORT_ONLY_SESSION_AUTHORITY_VERIFICATION','evaluation_role':evaluation_role,'verified_at_utc':datetime.now(timezone.utc).isoformat(),'authority_manifest_sha256':v1._sha256(manifest_path),'authority_manifest_payload_sha256':claim,'authority_session_row_count':len(authority),'observed_session_row_count':len(observed),f'{evaluation_role}_date_count':len(dates),'identity_calendar_status':'PASS','observed_st_source_parity_status':'PASS','exact_st_session_count':int(m['exact_st_session_count']),'excluded_missing_exact_st_session_count':int(m['excluded_missing_exact_st_session_count']),'non_st_authority_session_count':int(m['non_st_authority_session_count']),'daily_st_source_sha256':str(m['daily_st_source_sha256']),'source_artifact_verification_status':'PASS','validation_reads':int(m.get('validation_reads') or 0),'holdout_reads':int(m.get('holdout_reads') or 0),'historical_challenge_reads':int(m.get('historical_challenge_reads') or 0),'forward_2026_reads':int(m.get('forward_2026_reads') or 0),'promotion_authorized':False}
    receipt['receipt_payload_sha256']=v1._stable_hash(receipt);p=v1._write_json(out/'audit.json',receipt);return {**receipt,'audit_path':str(p),'audit_file_sha256':v1._sha256(p)}
def main()->int:
    p=argparse.ArgumentParser();p.add_argument('--authority-root',type=Path,required=True);p.add_argument('--output-root',type=Path,required=True);p.add_argument('--evaluation-role',choices=('validation','holdout','forward_2026','historical_challenge'),required=True);p.add_argument('--date-min',required=True);p.add_argument('--date-max',required=True);p.add_argument('--expected-schema-version',required=True);p.add_argument('--expected-status',required=True);a=p.parse_args();r=verify_report_only_session_authority(authority_root=a.authority_root,output_root=a.output_root,evaluation_role=a.evaluation_role,date_min=a.date_min,date_max=a.date_max,expected_schema_version=a.expected_schema_version,expected_status=a.expected_status);print(json.dumps(r,ensure_ascii=False,sort_keys=True));return 0
if __name__=='__main__': raise SystemExit(main())
