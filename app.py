from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from importlib import import_module
from pathlib import Path


REPO = Path(__file__).resolve().parent
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.project_control_admission import (
    ACTION_FREEZE,
    ACTION_LAUNCH,
    ACTION_RECOVERY,
    ACTION_RETRY,
    ACTION_SUCCESSOR,
    ALLOWED_ACTIONS,
    ProjectControlDenied,
    activate_admission,
    clear_active_admission,
)

ROUTES: dict[str, str] = {
    "phase3bp-true1min-search-algorithm-smoke": "our_system_phase2.runtime.phase3bp_true1min_search_algorithm_smoke",
    "phase3bs-adaptive-ucb-cem-practice": "our_system_phase2.runtime.phase3bs_adaptive_ucb_cem_practice",
    "phase3bt-ast-algorithm-bakeoff": "our_system_phase2.runtime.phase3bt_ast_algorithm_bakeoff",
    "phase3bu-ast-fresh-winner-variants": "our_system_phase2.runtime.phase3bu_ast_fresh_winner_variants",
    "phase3bx-bv-sortino-mcmc-audit": "our_system_phase2.runtime.phase3bx_bv_sortino_mcmc_audit",
    "phase3bz-fragment-replay-audit": "our_system_phase2.runtime.phase3bz_fragment_replay_audit",
    "phase3ca-build-bz-candidate-audit": "our_system_phase2.runtime.phase3ca_build_bz_candidate_audit",
    "phase3cm-train-portfolio-sortino-reward-audit": "our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit",
    "phase3cn-feedback-memory-smoke": "our_system_phase2.runtime.phase3cn_feedback_memory_smoke",
    "phase3cn-integrated-feedback-smoke": "our_system_phase2.runtime.phase3cn_integrated_feedback_smoke",
    "phase3cn-searcher-feedback-smoke": "our_system_phase2.runtime.phase3cn_searcher_feedback_smoke",
    "phase3co-multi-arm-scheduler-smoke": "our_system_phase2.runtime.phase3co_multi_arm_scheduler_smoke",
    "phase3cp-reward-gated-medium-search-smoke": "our_system_phase2.runtime.phase3cp_reward_gated_medium_search_smoke",
    "phase3cp-real-cm-small-loop": "our_system_phase2.runtime.phase3cp_real_cm_small_loop",
    "phase3cp-low-turnover-event-state-probe": "our_system_phase2.runtime.phase3cp_low_turnover_event_state_probe",
    "phase3db-train-sortino-feedback-controller": "our_system_phase2.runtime.phase3db_train_sortino_feedback_controller",
    "phase3ds-targeted-family-repair-pack": "our_system_phase2.runtime.phase3ds_targeted_family_repair_pack",
    "phase3dt-survivor-expansion-repair-pack": "our_system_phase2.runtime.phase3dt_survivor_expansion_repair_pack",
    "phase3du-adaptive-regime-free-deepen-pack": "our_system_phase2.runtime.phase3du_adaptive_regime_free_deepen_pack",
    "phase3dv-budget-pool-self-deepen-pack": "our_system_phase2.runtime.phase3dv_budget_pool_self_deepen_pack",
    "phase3dx-cn-tradable-followup-preflight": "our_system_phase2.runtime.phase3dx_cn_tradable_followup_preflight",
    "phase3dy-true1min-tplus1-tradable-replay": "our_system_phase2.runtime.phase3dy_true1min_tplus1_tradable_replay",
    "phase3cr-atom-lane-inventory-audit": "our_system_phase2.runtime.phase3cr_atom_lane_inventory_audit",
    "phase3cs-build-true1min-sidecar-pack": "our_system_phase2.runtime.phase3cs_build_true1min_sidecar_pack",
    "phase3cs-augment-true1min-shards-with-sidecars": "our_system_phase2.runtime.phase3cs_augment_true1min_shards_with_sidecars",
    "phase3ct-true1min-lifecycle-field-usage-audit": "our_system_phase2.runtime.phase3ct_true1min_lifecycle_field_usage_audit",
    "phase3ce-unsafe-motif-quarantine-audit": "our_system_phase2.runtime.phase3ce_unsafe_motif_quarantine_audit",
    "phase3ce1-search-memory-blocked-view": "our_system_phase2.runtime.phase3ce1_search_memory_blocked_view",
    "phase3ce1-g2-input-gate-smoke": "our_system_phase2.runtime.phase3ce1_g2_input_gate_smoke",
    "phase3ce2-typed-primitive-candidate-pack-canary": "our_system_phase2.runtime.phase3ce2_typed_primitive_candidate_pack_canary",
    "phase3ce2-typed-primitive-evaluator-smoke": "our_system_phase2.runtime.phase3ce2_typed_primitive_evaluator_smoke",
    "phase3cf-large-search-prelaunch": "our_system_phase2.runtime.phase3cf_large_search_prelaunch",
    "nextgen-build-external-sidecars": "scripts.build_nextgen_external_sidecars",
    "nextgen-true1min-plate-materialization-smoke": "our_system_phase2.runtime.nextgen_true1min_plate_materialization_smoke",
    "nextgen-dark-development-canary": "our_system_phase2.runtime.nextgen_dark_development_canary",
    "cn-b1s-development-canary": "our_system_phase2.runtime.cn_b1s_development_canary",
    "cn-generator-funnel-diagnosis": "our_system_phase2.runtime.cn_generator_funnel_diagnosis",
    "cn-unified-capability-preflight": "our_system_phase2.runtime.cn_unified_capability_preflight",
    "cn-broad-event-frozen-replay": "our_system_phase2.runtime.cn_broad_event_frozen_replay",
    "cn-unified-capability-discovery": "our_system_phase2.runtime.cn_unified_capability_discovery",
    "cn-iterative-search-v1-canary": "our_system_phase2.runtime.cn_iterative_search_v1",
    "cn-targeted-search-medium-campaign": "our_system_phase2.runtime.cn_targeted_search_medium_campaign",
    "cn-search-policy-qualification": "our_system_phase2.runtime.cn_search_policy_qualification",
    "cn-large-tpe-search-campaign": "our_system_phase2.runtime.cn_large_tpe_search_campaign",
    "cn-route-supply-closure": "scripts.run_cn_route_supply_closure",
    "cn-candidate-representation-v0-preflight": "our_system_phase2.runtime.cn_candidate_representation_v0_preflight",
    "cn-fixed-stratified-production-v0": "our_system_phase2.runtime.cn_fixed_stratified_production_v0",
    "cn-joint-program-search-v2-canary": "our_system_phase2.runtime.cn_joint_program_search_v2_canary",
    "cn-program-optimizer-tournament-v1": "our_system_phase2.runtime.cn_program_optimizer_tournament_v1",
    "cn-program-optimizer-successor-benchmark-v1": "our_system_phase2.runtime.cn_program_optimizer_successor_benchmark_v1",
    "cn-program-optimizer-d1-development-v1": "our_system_phase2.runtime.cn_program_optimizer_d1_development_v1",
    "cn-program-optimizer-large-fresh-development-v1": "our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1",
    "cn-program-optimizer-large-fresh-development-v2": "our_system_phase2.runtime.cn_program_optimizer_large_fresh_v2",
    "cn-program-disclosure-timing-mechanism-successor-v1": "our_system_phase2.runtime.cn_program_disclosure_timing_mechanism_successor_v1",
    "cn-program-primitive-local-stage-b-benchmark-v1": "our_system_phase2.runtime.cn_program_primitive_local_stage_b_benchmark_v1",
    "cn-program-stage-c-system-search-v1": "our_system_phase2.runtime.cn_program_stage_c_system_search_v1",
    "cn-program-stage-d-primitive-confirmation-v1": "our_system_phase2.runtime.cn_program_stage_d_primitive_confirmation_v1",
    "cn-program-primitive-main-production-v1": "our_system_phase2.runtime.cn_program_primitive_main_production_v1",
    "cn-program-primitive-main-production-recovery-v1": "our_system_phase2.runtime.cn_program_primitive_main_production_recovery_v1",
    "cn-program-optimizer-d1-report-only-validation-v1": "our_system_phase2.runtime.cn_program_optimizer_d1_report_only_validation_v1",
    "cn-program-optimizer-d1-transfer-prospective-validation-v1": "our_system_phase2.runtime.cn_program_optimizer_d1_transfer_prospective_validation_v1",
    "cn-typed-candidate-program-v1-smoke": "our_system_phase2.runtime.cn_typed_candidate_program_v1_smoke",
    "cn-core-pack-authority-smoke": "scripts.run_cn_core_pack_authority_smoke",
    "build-development-only-true1min-release": "our_system_phase2.runtime.build_development_only_true1min_release",
}

CURRENT_SEARCH_ROUTE = "phase3cp-real-cm-small-loop"
CURRENT_SEARCH_ROUTE_CONTRACT = "LEGACY_PROPOSAL_TO_UNIFIED_RECEIPT_GATE_TO_EVALUATOR"
CURRENT_SEARCH_STATUS = "FORMAL_SEARCH_FROZEN_PENDING_SEPARATE_CAPABILITY_RUN_AUTHORIZATION"

RETIRED_ROUTES: dict[str, str] = {
    "phase3du-adaptive-regime-free-deepen-pack": (
        "superseded by Phase3DV budget-pool self-deepen. Phase3DU used a more "
        "rigid deepen/freeze policy and should be used only for provenance."
    ),
    "phase3dv-budget-pool-self-deepen-pack": (
        "legacy proposal source only. Formal semantic/reward evaluation must "
        "flow through phase3cp-real-cm-small-loop and its unified candidate receipt gate."
    ),
}

# These are the canonical execution entrances whose normal purpose can freeze or
# consume material search budget.  The admission is checked before route import,
# so a denial cannot initialize an evaluator, read market data, or create output.
HIGH_COST_ROUTE_ACTIONS: dict[str, frozenset[str]] = {
    "phase3cp-real-cm-small-loop": frozenset(
        {ACTION_LAUNCH, ACTION_SUCCESSOR, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "phase3cf-large-search-prelaunch": frozenset({ACTION_FREEZE}),
    "nextgen-dark-development-canary": frozenset(
        {ACTION_LAUNCH, ACTION_SUCCESSOR, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-b1s-development-canary": frozenset(
        {ACTION_LAUNCH, ACTION_SUCCESSOR, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-iterative-search-v1-canary": frozenset(
        {ACTION_LAUNCH, ACTION_SUCCESSOR, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-targeted-search-medium-campaign": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-large-tpe-search-campaign": frozenset(
        {ACTION_LAUNCH, ACTION_SUCCESSOR, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-fixed-stratified-production-v0": frozenset(
        {ACTION_LAUNCH, ACTION_SUCCESSOR, ACTION_RETRY}
    ),
    "cn-joint-program-search-v2-canary": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-program-optimizer-tournament-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY, ACTION_RECOVERY}
    ),
    "cn-program-optimizer-successor-benchmark-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-optimizer-d1-development-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-optimizer-large-fresh-development-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-optimizer-large-fresh-development-v2": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-disclosure-timing-mechanism-successor-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-primitive-local-stage-b-benchmark-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-stage-c-system-search-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-stage-d-primitive-confirmation-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-primitive-main-production-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-primitive-main-production-recovery-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-optimizer-d1-report-only-validation-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
    "cn-program-optimizer-d1-transfer-prospective-validation-v1": frozenset(
        {ACTION_LAUNCH, ACTION_RETRY}
    ),
}


def _split_route_args(argv: list[str]) -> tuple[list[str], list[str]]:
    if "--" not in argv:
        return argv, []
    idx = argv.index("--")
    return argv[:idx], argv[idx + 1 :]


def _load_main(route: str) -> Callable[..., int | None]:
    module_path = ROUTES[route]
    module = import_module(module_path)
    main = getattr(module, "main", None)
    if main is None:
        raise RuntimeError(f"route {route!r} has no main() in {module_path}")
    return main


def _explicit_output_root(passthrough: list[str]) -> Path:
    values: list[str] = []
    for index, value in enumerate(passthrough):
        if value == "--output-root":
            if index + 1 >= len(passthrough):
                raise ProjectControlDenied("--output-root value missing")
            values.append(passthrough[index + 1])
        elif value.startswith("--output-root="):
            values.append(value.split("=", 1)[1])
    if len(values) != 1 or not values[0]:
        raise ProjectControlDenied(
            "high-cost route requires exactly one explicit --output-root after --"
        )
    return Path(values[0]).expanduser().resolve()


def _validate_high_cost_route_admission(
    *,
    route: str,
    requested_action: str,
    target_run_id: str,
    passthrough: list[str],
    admission_path: Path | None,
    admission_sha256: str,
) -> dict[str, object] | None:
    expected_actions = HIGH_COST_ROUTE_ACTIONS.get(route)
    if expected_actions is None:
        return None
    if (
        admission_path is None
        or not admission_sha256
        or not requested_action
        or not target_run_id
    ):
        raise ProjectControlDenied(
            "high-cost route requires --requested-action, --target-run-id, "
            "--project-control-admission and --project-control-admission-sha256"
        )
    if requested_action not in expected_actions:
        raise ProjectControlDenied("requested action is not valid for this route")
    output_root = _explicit_output_root(passthrough)
    return activate_admission(
        admission_path,
        expected_admission_file_sha256=admission_sha256,
        expected_actions={requested_action},
        expected_target_campaign_id=route,
        expected_target_run_id=target_run_id,
        expected_target_output_root=output_root,
    )


def main(argv: list[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    route_args, passthrough = _split_route_args(raw_args)

    parser = argparse.ArgumentParser(
        description="True1min alpha research entrypoint. Legacy 1D routes are intentionally absent."
    )
    parser.add_argument("route", choices=sorted(ROUTES))
    parser.add_argument("--allow-diagnostic", action="store_true")
    parser.add_argument("--requested-action", choices=sorted(ALLOWED_ACTIONS), default="")
    parser.add_argument("--target-run-id", default="")
    parser.add_argument("--project-control-admission", type=Path)
    parser.add_argument("--project-control-admission-sha256", default="")
    parsed = parser.parse_args(route_args)

    if parsed.route in RETIRED_ROUTES and not parsed.allow_diagnostic:
        parser.error(
            f"route {parsed.route!r} is retired: {RETIRED_ROUTES[parsed.route]} "
            "Pass --allow-diagnostic only for provenance replay."
        )

    if parsed.route in RETIRED_ROUTES:
        print(
            f"[diagnostic-retired-route] {parsed.route}: {RETIRED_ROUTES[parsed.route]}",
            file=sys.stderr,
        )

    try:
        admission_proof = _validate_high_cost_route_admission(
            route=parsed.route,
            requested_action=parsed.requested_action,
            target_run_id=parsed.target_run_id,
            passthrough=passthrough,
            admission_path=parsed.project_control_admission,
            admission_sha256=parsed.project_control_admission_sha256,
        )
    except (ProjectControlDenied, OSError) as exc:
        parser.error(f"project-control admission denied before route import: {exc}")

    try:
        main_func = _load_main(parsed.route)
        try:
            result = main_func(passthrough)
        except TypeError as exc:
            if passthrough:
                raise
            if "positional" not in str(exc) and "argument" not in str(exc):
                raise
            result = main_func()
    finally:
        clear_active_admission()
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
