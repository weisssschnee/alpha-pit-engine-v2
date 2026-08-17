from __future__ import annotations

import app
import pytest

from scripts.run_cn_program_optimizer_d1_transfer_prospective_validation_v1 import _acceptance, _filter_acceptance_contract
from our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1 import ROUTE_ID
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
)


def _row(productive: bool, admitted: bool = True) -> dict:
    return {
        "admission": {"admitted": admitted},
        "validation_productive": productive,
    }


def _contract() -> dict:
    return {
        "minimum_development_productive_population": 30,
        "filtered_precision_minimum": 0.35,
        "filtered_minus_unfiltered_precision_minimum": 0.10,
        "filtered_recall_minimum": 0.60,
        "all_conditions_required": True,
        "rule_change_after_B_development_or_validation": "FORBIDDEN",
    }


def test_prospective_transfer_acceptance_passes_only_frozen_joint_gate() -> None:
    all_rows = [_row(i < 8) for i in range(30)]
    selected = [_row(i < 6) for i in range(12)]
    verdict = _acceptance(all_rows=all_rows, selected_rows=selected, contract=_contract())
    assert verdict["status"] == "PASS"
    assert verdict["all_development_positive_precision"] == 8 / 30
    assert verdict["filtered_precision"] == 6 / 12
    assert verdict["filtered_recall"] == 6 / 8
    assert verdict["filtered_minus_unfiltered_precision"] > 0.10
    assert all(verdict["checks"].values())


def test_prospective_transfer_acceptance_fails_when_precision_lift_is_too_small() -> None:
    all_rows = [_row(i < 8) for i in range(30)]
    selected = [_row(i < 4) for i in range(12)]
    verdict = _acceptance(all_rows=all_rows, selected_rows=selected, contract=_contract())
    assert verdict["status"] == "FAIL_PROSPECTIVE_FILTER_TEST"
    assert verdict["checks"]["filtered_precision_minimum"] is False
    assert verdict["checks"]["filtered_minus_unfiltered_precision_minimum"] is False


def test_filter_acceptance_contract_supports_frozen_B_or_C_but_not_both() -> None:
    b={"prospective_B_validation_acceptance":_contract()}
    c={"prospective_C_validation_acceptance":{**_contract(),"rule_change_after_C_development_or_validation":"FORBIDDEN"}}
    assert _filter_acceptance_contract(b)["filtered_precision_minimum"]==0.35
    assert _filter_acceptance_contract(c)["filtered_recall_minimum"]==0.60
    with pytest.raises(RuntimeError,match="cardinality drift"):
        _filter_acceptance_contract({**b,**c})
    with pytest.raises(RuntimeError,match="cardinality drift"):
        _filter_acceptance_contract({})


def test_prospective_transfer_validation_route_is_high_cost_and_campaign_bound() -> None:
    assert app.ROUTES[ROUTE_ID] == (
        "our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[ROUTE_ID] == {ACTION_LAUNCH, ACTION_RETRY}
    assert ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_validation_sidecar_fusion_preserves_keys_and_adds_incremental_fields(tmp_path):
    import hashlib
    import json
    from pathlib import Path

    import pandas as pd

    from scripts.fuse_cn_program_validation_session_sidecar_v1 import MANIFEST_NAME, fuse

    def sha(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    base_root = tmp_path / "base"
    inc_root = tmp_path / "inc"
    out_root = tmp_path / "fused"
    base_root.mkdir(); inc_root.mkdir()
    base_records = []
    inc_records = []
    for shard in range(16):
        common = {
            "trade_time": [pd.Timestamp("2025-07-08 15:00:00")],
            "code": [f"{shard:06d}"],
            "source_shard": [shard],
            "source_row_identity": [0],
            "duplicate_ordinal": [0],
            "open": [10.0 + shard],
            "close": [10.5 + shard],
        }
        bf = pd.DataFrame({**common, "base_field": [float(shard)]})
        inf = pd.DataFrame({**common, "added_field": [float(shard + 100)]})
        bp = base_root / f"shard_{shard:02d}.parquet"
        ip = inc_root / f"shard_{shard:02d}.parquet"
        bf.to_parquet(bp, index=False); inf.to_parquet(ip, index=False)
        source_sha = f"{shard + 1:064x}"
        base_records.append({"source_shard":shard,"source_path":f"source_{shard}","source_sha256":source_sha,"rows":1,"status":"SESSION_VALIDATION_PIT_MATERIALIZATION_PASS","fields":list(bf.columns),"pit_coverage":{"base_field":1.0},"direct_field_intraday_variation":{},"direct_field_materialization_policy":"LAST_OBSERVED_VALUE_AT_OR_BEFORE_SESSION_CLOSE_PIT","output_path":str(bp),"output_sha256":sha(bp),"output_bytes":bp.stat().st_size})
        inc_records.append({"source_shard":shard,"source_path":f"source_{shard}","source_sha256":source_sha,"rows":1,"status":"SESSION_VALIDATION_PIT_MATERIALIZATION_PASS","fields":list(inf.columns),"pit_coverage":{"added_field":1.0},"direct_field_intraday_variation":{},"direct_field_materialization_policy":"LAST_OBSERVED_VALUE_AT_OR_BEFORE_SESSION_CLOSE_PIT","output_path":str(ip),"output_sha256":sha(ip),"output_bytes":ip.stat().st_size})

    def manifest(records, fields, coverage):
        return {"schema_version":"test","status":"TIME_MAJOR_LAYOUT_PARITY_PASS","evaluation_role":"validation","data_role":"validation_report_only","split_manifest_hash":"split","eligible_validation_date_count":73,"source_shard_count":16,"fields":fields,"canonical_pit_coverage":coverage,"direct_field_intraday_variation_max":{},"source_rows":16,"sidecar_rows":16,"sidecar_bytes":sum(r["output_bytes"] for r in records),"build_wall_seconds":0.0,"shards":records,"validation_reads":16,"holdout_reads":0,"forward_2026_reads":0,"feedback_write":"FORBIDDEN","scheduler_write":"FORBIDDEN","archive_write":"FORBIDDEN","promotion":"FORBIDDEN"}

    (base_root / MANIFEST_NAME).write_text(json.dumps(manifest(base_records, base_records[0]["fields"], {"base_field":1.0})), encoding="utf-8")
    (inc_root / MANIFEST_NAME).write_text(json.dumps(manifest(inc_records, inc_records[0]["fields"], {"added_field":1.0})), encoding="utf-8")
    result = fuse(base_root=base_root, incremental_root=inc_root, output_root=out_root, required_fields=("base_field","added_field"))
    fused = json.loads((out_root / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert result["added_fields"] == ["added_field"]
    assert len(fused["shards"]) == 16
    assert fused["sidecar_rows"] == fused["validation_reads"] == 16
    assert "base_field" in fused["fields"] and "added_field" in fused["fields"]
    assert fused["fusion_provenance"]["stable_key_parity"] == "PASS"
    assert fused["fusion_provenance"]["overlap_value_parity"] == "PASS"
    assert pd.read_parquet(out_root / "shard_00.parquet")["added_field"].iloc[0] == 100.0
