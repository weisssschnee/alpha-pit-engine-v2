"""Fuse a verified validation session sidecar with incremental PIT fields.

Both inputs must describe the same 73-session validation coordinates.  Every
shard is joined only after exact stable-key and overlapping-value parity.  The
output is a fresh report-only validation sidecar with new artifact hashes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

STABLE_KEY=("trade_time","code","source_shard","source_row_identity","duplicate_ordinal")
MANIFEST_NAME="CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json"


def _sha256(path:Path)->str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(4*1024*1024),b""): h.update(b)
    return h.hexdigest()

def _read(path:Path)->dict[str,Any]: return json.loads(path.read_text(encoding="utf-8-sig"))

def _validate_manifest(root:Path, label:str)->dict[str,Any]:
    p=root.resolve()/MANIFEST_NAME
    j=_read(p)
    required={"status":"TIME_MAJOR_LAYOUT_PARITY_PASS","evaluation_role":"validation","data_role":"validation_report_only","holdout_reads":0,"forward_2026_reads":0,"feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","promotion":"FORBIDDEN"}
    drift=[k for k,v in required.items() if j.get(k)!=v]
    if drift: raise RuntimeError(f"{label} validation sidecar drift: {drift}")
    if int(j.get("eligible_validation_date_count") or 0)!=73 or int(j.get("source_shard_count") or 0)!=16:
        raise RuntimeError(f"{label} validation shape drift")
    if len(j.get("shards") or ())!=16: raise RuntimeError(f"{label} shard cardinality drift")
    for row in j["shards"]:
        p=Path(str(row["output_path"])).resolve()
        if not p.is_file() or _sha256(p)!=str(row["output_sha256"]): raise RuntimeError(f"{label} shard hash drift: {p}")
    return j

def _frame_equal(left:pd.Series,right:pd.Series)->bool:
    if str(left.dtype).startswith("datetime") or str(right.dtype).startswith("datetime"):
        return pd.to_datetime(left,errors="raise").equals(pd.to_datetime(right,errors="raise"))
    a=left.to_numpy(); b=right.to_numpy()
    if np.issubdtype(a.dtype,np.number) and np.issubdtype(b.dtype,np.number):
        return bool(np.array_equal(a,b,equal_nan=True))
    return left.astype("string").fillna("<NA>").equals(right.astype("string").fillna("<NA>"))

def fuse(*,base_root:Path,incremental_root:Path,output_root:Path,required_fields:Sequence[str]|None=None)->dict[str,Any]:
    started=time.perf_counter(); base=_validate_manifest(base_root,"base"); inc=_validate_manifest(incremental_root,"incremental")
    if str(base.get("split_manifest_hash"))!=str(inc.get("split_manifest_hash")): raise RuntimeError("fusion split manifest drift")
    if output_root.exists(): raise FileExistsError(output_root)
    base_fields=list(map(str,base.get("fields") or ())); inc_fields=list(map(str,inc.get("fields") or ()))
    added=[f for f in inc_fields if f not in base_fields and f not in STABLE_KEY]
    if not added: raise RuntimeError("fusion incremental sidecar adds no fields")
    if required_fields is not None:
        missing=sorted(set(map(str,required_fields))-set(base_fields)-set(added))
        if missing: raise RuntimeError(f"fusion still misses required fields: {missing}")
    output_root.mkdir(parents=True)
    bby={int(r["source_shard"]):r for r in base["shards"]}; iby={int(r["source_shard"]):r for r in inc["shards"]}
    records=[]
    for shard in range(16):
        br=bby[shard]; ir=iby[shard]
        if int(br["rows"])!=int(ir["rows"]) or str(br["source_sha256"])!=str(ir["source_sha256"]): raise RuntimeError(f"fusion source/row drift shard={shard}")
        bf=pd.read_parquet(Path(str(br["output_path"])).resolve()); inf=pd.read_parquet(Path(str(ir["output_path"])).resolve())
        if len(bf)!=len(inf): raise RuntimeError(f"fusion row drift shard={shard}")
        for col in STABLE_KEY:
            if col not in bf or col not in inf or not _frame_equal(bf[col],inf[col]): raise RuntimeError(f"fusion stable-key drift shard={shard} col={col}")
        overlap=sorted((set(bf.columns)&set(inf.columns))-set(STABLE_KEY))
        for col in overlap:
            if not _frame_equal(bf[col],inf[col]): raise RuntimeError(f"fusion overlapping value drift shard={shard} col={col}")
        fused=bf.copy(deep=False)
        for col in added: fused[col]=inf[col].to_numpy()
        out=output_root/f"shard_{shard:02d}.parquet"
        pq.write_table(pa.Table.from_pandas(fused,preserve_index=False),out,compression="zstd")
        pit=dict(br.get("pit_coverage") or {}); pit.update({k:v for k,v in (ir.get("pit_coverage") or {}).items() if k in added})
        direct=dict(br.get("direct_field_intraday_variation") or {}); direct.update({k:v for k,v in (ir.get("direct_field_intraday_variation") or {}).items() if k in added})
        records.append({**{k:br[k] for k in ("source_shard","source_path","source_sha256","rows")},"status":"SESSION_VALIDATION_PIT_MATERIALIZATION_PASS","fields":list(map(str,fused.columns)),"pit_coverage":pit,"direct_field_intraday_variation":direct,"direct_field_materialization_policy":str(br.get("direct_field_materialization_policy") or "LAST_OBSERVED_VALUE_AT_OR_BEFORE_SESSION_CLOSE_PIT"),"output_path":str(out),"output_sha256":_sha256(out),"output_bytes":out.stat().st_size})
    fields=records[0]["fields"]
    if any(r["fields"]!=fields for r in records): raise RuntimeError("fusion shard field drift")
    coverage={f:max(float((r.get("pit_coverage") or {}).get(f,0.0)) for r in records) for f in sorted({k for r in records for k in (r.get("pit_coverage") or {})})}
    direct_max={f:max(int((r.get("direct_field_intraday_variation") or {}).get(f,0)) for r in records) for f in sorted({k for r in records for k in (r.get("direct_field_intraday_variation") or {})})}
    manifest=dict(base)
    manifest.update({"schema_version":"cn_program_optimizer_validation_fused_session_sidecar_v1","status":"TIME_MAJOR_LAYOUT_PARITY_PASS","fields":fields,"canonical_pit_coverage":coverage,"direct_field_intraday_variation_max":direct_max,"source_shard_count":16,"source_rows":sum(int(r["rows"]) for r in records),"sidecar_rows":sum(int(r["rows"]) for r in records),"sidecar_bytes":sum(int(r["output_bytes"]) for r in records),"build_wall_seconds":time.perf_counter()-started,"shards":records,"validation_reads":sum(int(r["rows"]) for r in records),"holdout_reads":0,"forward_2026_reads":0,"feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","promotion":"FORBIDDEN","fusion_provenance":{"base_manifest_path":str((base_root/MANIFEST_NAME).resolve()),"base_manifest_sha256":_sha256((base_root/MANIFEST_NAME).resolve()),"incremental_manifest_path":str((incremental_root/MANIFEST_NAME).resolve()),"incremental_manifest_sha256":_sha256((incremental_root/MANIFEST_NAME).resolve()),"added_fields":added,"stable_key":list(STABLE_KEY),"stable_key_parity":"PASS","overlap_value_parity":"PASS"}})
    p=output_root/MANIFEST_NAME; p.write_text(json.dumps(manifest,ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    return {"status":"TIME_MAJOR_LAYOUT_PARITY_PASS","manifest":str(p),"manifest_sha256":_sha256(p),"sidecar_rows":manifest["sidecar_rows"],"added_fields":added,"field_count":len(fields)}

def parser()->argparse.ArgumentParser:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--base-root",type=Path,required=True); p.add_argument("--incremental-root",type=Path,required=True); p.add_argument("--output-root",type=Path,required=True); p.add_argument("--required-fields-json",type=Path); return p

def main(argv:Sequence[str]|None=None)->int:
    a=parser().parse_args(argv); req=None
    if a.required_fields_json: req=json.loads(a.required_fields_json.read_text(encoding="utf-8-sig"))
    print(json.dumps(fuse(base_root=a.base_root,incremental_root=a.incremental_root,output_root=a.output_root,required_fields=req),ensure_ascii=False,sort_keys=True)); return 0
if __name__=="__main__": raise SystemExit(main())
