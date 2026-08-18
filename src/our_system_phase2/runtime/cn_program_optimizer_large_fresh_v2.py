"""Project-Control entry for Large Fresh V2 (Typed Evolution + Uniform)."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    RESOURCE_CPU_THREADS,
    verify_authorization as verify_large_fresh_authorization,
)
from our_system_phase2.services.node_resource_governor import (
    validate_node_resource_lease_receipt,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.unified_capability_registry import stable_hash
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    ProjectControlDenied,
    consume_active_admission,
    sha256_file,
    verify_campaign_authorization_binding,
    verify_consumed_admission_target,
)

ROUTE_ID = "cn-program-optimizer-large-fresh-development-v2"
CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V2"
CAMPAIGN_PROFILE = "cn_program_optimizer_large_fresh_development_v2"
AUTHORIZATION_SCHEMA = "cn_program_optimizer_large_fresh_development_authorization_v2"
FORMAL_SEARCH_AUTHORITY = "CATALOG_TYPED_EVOLUTION_AVAILABILITY_V2"
AUTHORIZATION_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_large_fresh_development_v2.json"
)
PRIMARY_EXECUTOR_WORKERS = 24
RESOURCE_FALLBACK_EXECUTOR_WORKERS = 16
RESOURCE_CANARY_PROBE_SECONDS = 30.0
RESOURCE_STRESS_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_program_optimizer_large_fresh_resource_stress_v2_20260818.json"
)
RESOURCE_STRESS_FILE_SHA256 = "a1397e8929b21faafcfe95325c4bcb9ce6b42f54faaf18ef72dcfd0317ce8460"
RESOURCE_STRESS_PAYLOAD_SHA256 = "529cd4db455698c854a70242b495cbf54efc4931ff68d1939790450c9779e6e0"
RESOURCE_CANARY_FIELD_COLUMNS_SHA256 = "85c84e8faf2bf6293dd8628641605a8af38686aa878b63b6ac46d0539e361544"
RESOURCE_CANARY_FIELD_COLUMNS = (
    "amount", "chip_cost_p15", "chip_cost_p85", "close", "code",
    "ctx_hfq_float_market_cap_yuan", "ctx_hfq_market_cap_yuan", "ctx_hfq_ps_ttm",
    "ctx_hfq_volume_ratio", "ctx_rzrq_rzjme", "ctx_sent_damian_num",
    "ctx_sent_ditian_num", "ctx_sent_down_num", "ctx_sent_lt5_num",
    "ctx_sent_max_lb_num", "ctx_sent_tiandi_num",
    "fund_accruals_profit_minus_cfo_assets", "fund_ba_accounts_rece_source_yoy",
    "fund_ba_accounts_rece_source_yoy_at_disclosure",
    "fund_ba_fixed_asset_source_yoy_at_disclosure", "fund_ba_inventory_delta",
    "fund_ba_inventory_slope_at_disclosure", "fund_ba_monetaryfunds_slope",
    "fund_ca_construct_long_asset_persistence", "fund_ca_netcash_finance_acceleration",
    "fund_ca_netcash_operate_source_yoy", "fund_capex_construct_assets",
    "fund_cashflow_cfo_profit", "fund_cashflow_cfo_revenue",
    "fund_disclosure_balance_pulse", "fund_disclosure_profit_pulse",
    "fund_efficiency_fixed_asset_intensity", "fund_holder_count_log",
    "fund_holder_top10_share_ratio", "fund_holder_top10_shares_log",
    "fund_leverage_liabilities_assets", "fund_liquidity_current_ratio",
    "fund_pr_deduct_parent_netprofit_slope", "fund_pr_deduct_parent_netprofit_yoy",
    "fund_pr_parent_netprofit_acceleration", "fund_pr_parent_netprofit_source_yoy",
    "fund_profit_gross_margin", "fund_profit_operating_margin", "open", "pct_chg",
    "ret_1m", "trade_time",
)


def verify_authorization(path: Path, *, repo_root: Path | None = None) -> dict[str, Any]:
    root = Path(repo_root or Path(__file__).resolve().parents[3]).resolve()
    payload = verify_large_fresh_authorization(
        path,
        repo_root=repo_root,
        expected_schema_version=AUTHORIZATION_SCHEMA,
        expected_campaign_id=CAMPAIGN_ID,
        expected_campaign_profile=CAMPAIGN_PROFILE,
        expected_route_id=ROUTE_ID,
        expected_formal_search_authority=FORMAL_SEARCH_AUTHORITY,
        expected_formal_optimizer_arm=CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
        expected_primary_executor_workers=PRIMARY_EXECUTOR_WORKERS,
        expected_fallback_executor_workers=RESOURCE_FALLBACK_EXECUTOR_WORKERS,
    )
    design = dict(payload.get("search_design") or {})
    if (
        design.get("tpe_selection_authorized") is not False
        or design.get("tpe_feedback_authorized") is not False
        or design.get("cem_selection_authorized") is not False
        or design.get("cem_feedback_authorized") is not False
    ):
        raise ValueError("large fresh V2 challenger authorization drift")
    evidence = dict(payload.get("optimizer_evidence") or {})
    evidence_path = (root / Path(str(evidence.get("relative_path") or ""))).resolve()
    if (
        not evidence_path.is_relative_to(root)
        or not evidence_path.is_file()
        or sha256_file(evidence_path) != str(evidence.get("file_sha256") or "")
        or evidence.get("decision") != "EVOLUTION_PRIMARY_UNIFORM_RESERVE"
        or evidence.get("implementation_repo_sha") != "c8ff15049d29da00085cc7ce1e7b76b09ad58a37"
        or int(evidence.get("paired_seed_count") or 0) != 4
        or float(evidence.get("evolution_mean_productive_delta_at_168") or 0.0)
        != 3.0
        or int(evidence.get("evolution_positive_seed_count_at_168") or 0) != 4
        or float(evidence.get("evolution_mean_productive_delta_at_840") or 0.0)
        != 23.5
        or int(evidence.get("evolution_positive_seed_count_at_840") or 0) != 4
    ):
        raise ValueError("large fresh V2 optimizer evidence binding drift")
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8-sig"))
    evidence_body = dict(evidence_payload)
    claimed = str(evidence_body.pop("evidence_payload_sha256", ""))
    if (
        not claimed
        or stable_hash(evidence_body) != claimed
        or claimed != str(evidence.get("payload_sha256") or "")
    ):
        raise ValueError("large fresh V2 optimizer evidence payload drift")

    resource_evidence = dict(payload.get("resource_evidence") or {})
    resource_path = (root / RESOURCE_STRESS_RELATIVE_PATH).resolve()
    if (
        resource_evidence.get("relative_path") != str(RESOURCE_STRESS_RELATIVE_PATH).replace("\\", "/")
        or not resource_path.is_file()
        or sha256_file(resource_path) != RESOURCE_STRESS_FILE_SHA256
        or resource_evidence.get("file_sha256") != RESOURCE_STRESS_FILE_SHA256
        or resource_evidence.get("payload_sha256") != RESOURCE_STRESS_PAYLOAD_SHA256
        or int(resource_evidence.get("requested_workers") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(resource_evidence.get("distinct_worker_count") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(resource_evidence.get("field_column_count") or 0) != len(RESOURCE_CANARY_FIELD_COLUMNS)
        or resource_evidence.get("field_columns_sha256") != RESOURCE_CANARY_FIELD_COLUMNS_SHA256
        or resource_evidence.get("usage") != "RESOURCE_ONLY_NO_ECONOMIC_REUSE"
    ):
        raise ValueError("large fresh V2 resource evidence binding drift")
    resource_payload = json.loads(resource_path.read_text(encoding="utf-8-sig"))
    body = dict(resource_payload)
    resource_claimed = str(body.pop("resource_evidence_payload_sha256", ""))
    if (
        resource_claimed != RESOURCE_STRESS_PAYLOAD_SHA256
        or stable_hash(body) != resource_claimed
        or resource_payload.get("status") != "PASS_RESOURCE_ONLY_REALISTIC_24_WORKER_STRESS"
        or int(resource_payload.get("requested_workers") or 0) != PRIMARY_EXECUTOR_WORKERS
        or int(resource_payload.get("distinct_worker_count") or 0) != PRIMARY_EXECUTOR_WORKERS
        or tuple(resource_payload.get("field_columns") or ()) != RESOURCE_CANARY_FIELD_COLUMNS
        or resource_payload.get("field_columns_sha256") != RESOURCE_CANARY_FIELD_COLUMNS_SHA256
        or int(resource_payload.get("minimum_commit_headroom_bytes") or 0) < 24 * 1024**3
        or int(resource_payload.get("pagefile_pages_in_delta_bytes", -1)) != 0
        or int(resource_payload.get("pagefile_pages_out_delta_bytes", -1)) != 0
        or list(resource_payload.get("orphan_worker_pids") or ())
        or resource_payload.get("financial_candidate_evaluation_performed") is not False
    ):
        raise ValueError("large fresh V2 realistic resource stress drift")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    admission = consume_active_admission(
        "cn-program-optimizer-large-fresh-development-v2",
        {ACTION_LAUNCH, ACTION_RETRY},
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign-authorization", type=Path, required=True)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)

    verify_consumed_admission_target(admission, output_root=args.output_root)
    verified = verify_campaign_authorization_binding(
        admission, args.campaign_authorization
    )
    authorization = verify_authorization(verified.path)
    if dict(verified.payload) != authorization:
        raise ProjectControlDenied("large fresh V2 authorization payload drift")
    validate_node_resource_lease_receipt(
        args.node_resource_lease_receipt.resolve(),
        expected_role="SEARCH",
        expected_cpu_threads=RESOURCE_CPU_THREADS,
    )

    from scripts.run_cn_program_optimizer_large_fresh_v2 import run

    result = run(args, admission=admission, authorization=authorization)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
