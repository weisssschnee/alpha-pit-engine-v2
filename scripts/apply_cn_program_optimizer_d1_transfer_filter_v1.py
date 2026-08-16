"""Apply the frozen D1 development-to-validation transfer filter.

This tool consumes a completed D1 development cohort only. It never reads
validation/holdout/forward data and freezes filter membership before any
validation access.
"""
from __future__ import annotations

import argparse, json, math, hashlib
from pathlib import Path
from typing import Any, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
import sys
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from our_system_phase2.services.unified_capability_registry import stable_hash

FILTER_RELATIVE_PATH = Path("runtime/run_plans/cn_program_optimizer_d1_transfer_filter_v1_20260817.json")
FILTER_ID = "CN_D1_TRANSFER_FILTER_ECON_ONLY_LOGIT_V1"
EXPECTED_FEATURES = (
    "dev_matched_return",
    "dev_matched_reward",
    "dev_robust_median",
    "dev_lower_tail",
)


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(x) for x in path.read_text(encoding="utf-8-sig").splitlines() if x.strip()]


def _sha256(path: Path) -> str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()


def _verify_self_hash(payload: Mapping[str, Any], field: str) -> None:
    body=dict(payload); claimed=str(body.pop(field,""))
    if not claimed or stable_hash(body)!=claimed:
        raise ValueError(f"self-hash drift: {field}")


def _score(features: Mapping[str, float], filter_payload: Mapping[str, Any]) -> float:
    formula=dict(filter_payload["score_formula_raw"])
    coeff=dict(formula["coefficients"])
    if tuple(filter_payload["features"]) != EXPECTED_FEATURES or set(coeff) != set(EXPECTED_FEATURES):
        raise ValueError("transfer-filter feature contract drift")
    score=float(formula["intercept"])
    for key in EXPECTED_FEATURES:
        value=float(features[key])
        if not math.isfinite(value): raise ValueError(f"non-finite transfer feature: {key}")
        score += float(coeff[key])*value
    return score


def _development_rows(run_root: Path) -> list[dict[str, Any]]:
    rows=[]
    for wave in range(20):
        root=run_root/f"wave_{wave:03d}"
        if not root.is_dir(): raise FileNotFoundError(root)
        logical={str(x["exact_identity"]):x for x in _read_jsonl(root/"logical_asks.jsonl")}
        physical=_read_jsonl(root/"physical_results.jsonl")
        for row in physical:
            exact=str(row["exact_identity"]); admission=dict(row["admission"]); uplift=row.get("uplift")
            if not bool(admission.get("admitted")) or not isinstance(uplift,Mapping): continue
            credit=dict(uplift.get("program_credit") or {})
            ret=float(credit.get("matched_cumulative_net_return_increment")); reward=float(credit.get("matched_net_reward_increment"))
            if not (ret>0.0 and reward>0.0): continue
            ask=logical.get(exact)
            if ask is None: raise RuntimeError(f"missing logical ask for {exact}")
            rows.append({
                "exact_identity": exact,
                "wave_index": wave,
                "template_id": str(ask["template_id"]),
                "selection_kind": str(ask["selection_kind"]),
                "dev_matched_return": ret,
                "dev_matched_reward": reward,
                "dev_robust_median": float(credit["robust_median_window_return_increment"]),
                "dev_lower_tail": float(credit["lower_tail_window_return_increment"]),
            })
    if len({r["exact_identity"] for r in rows}) != len(rows): raise RuntimeError("duplicate development-positive exact")
    return rows


def _rank_and_mark(rows: list[dict[str, Any]], flt: Mapping[str, Any]) -> tuple[list[dict[str, Any]], int]:
    for row in rows:
        features={k:float(row[k]) for k in EXPECTED_FEATURES}
        row["transfer_linear_score"]=_score(features,flt)
        row["transfer_probability_score"]=1.0/(1.0+math.exp(-row["transfer_linear_score"]))
    ordered=sorted(rows,key=lambda r:(-float(r["transfer_linear_score"]),str(r["exact_identity"])))
    frac=float(flt["selection_contract"]["selected_fraction"]); k=int(math.ceil(frac*len(ordered)))
    selected={r["exact_identity"] for r in ordered[:k]}
    for row in rows: row["transfer_filter_selected"]=row["exact_identity"] in selected
    return ordered, k


def apply_filter(*, run_root: Path, filter_path: Path, output_path: Path) -> dict[str, Any]:
    flt=_read_json(filter_path); _verify_self_hash(flt,"filter_payload_sha256")
    if flt.get("filter_id") != FILTER_ID or flt.get("status") != "FROZEN_BEFORE_NEXT_DEVELOPMENT_COHORT_VALIDATION":
        raise ValueError("transfer filter not frozen")
    rows=_development_rows(run_root)
    minimum=int(flt["prospective_B_validation_acceptance"]["minimum_development_productive_population"])
    if len(rows)<minimum: raise RuntimeError("insufficient development-positive population for prospective filter test")
    ordered, k = _rank_and_mark(rows, flt)
    frac=float(flt["selection_contract"]["selected_fraction"])
    output={
        "schema_version":"cn_program_optimizer_d1_transfer_filter_application_v1",
        "status":"FROZEN_BEFORE_VALIDATION_ACCESS",
        "filter_id":FILTER_ID,
        "filter_file_sha256":_sha256(filter_path),
        "filter_payload_sha256":flt["filter_payload_sha256"],
        "source_run_root":str(run_root.resolve()),
        "source_closure_file_sha256":_sha256(run_root/"CN_PROGRAM_OPTIMIZER_D1_DEVELOPMENT_COMPLETE.json"),
        "development_productive_count":len(rows),
        "selected_count":k,
        "selected_fraction_contract":frac,
        "selected_exact_identities":[r["exact_identity"] for r in ordered[:k]],
        "all_candidates":sorted(rows,key=lambda r:str(r["exact_identity"])),
        "validation_reads":0,"holdout_reads":0,"forward_2026_reads":0,
        "selection_frozen_before_validation":True,"optimizer_feedback_write":"FORBIDDEN","promotion_authorized":False,
    }
    output["application_payload_sha256"]=stable_hash(output)
    output_path.parent.mkdir(parents=True,exist_ok=True)
    output_path.write_text(json.dumps(output,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    return output


def main(argv: Sequence[str]|None=None) -> int:
    p=argparse.ArgumentParser(description=__doc__); p.add_argument("--run-root",type=Path,required=True); p.add_argument("--filter",type=Path,default=PROJECT_ROOT/FILTER_RELATIVE_PATH); p.add_argument("--output",type=Path,required=True); a=p.parse_args(argv)
    result=apply_filter(run_root=a.run_root,filter_path=a.filter,output_path=a.output)
    print(json.dumps({k:result[k] for k in ("status","development_productive_count","selected_count","application_payload_sha256")},sort_keys=True))
    return 0

if __name__=="__main__": raise SystemExit(main())
