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


def main(argv: list[str] | None = None) -> int:
    raw_args = list(sys.argv[1:] if argv is None else argv)
    route_args, passthrough = _split_route_args(raw_args)

    parser = argparse.ArgumentParser(
        description="True1min alpha research entrypoint. Legacy 1D routes are intentionally absent."
    )
    parser.add_argument("route", choices=sorted(ROUTES))
    parser.add_argument("--allow-diagnostic", action="store_true")
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

    main_func = _load_main(parsed.route)
    try:
        result = main_func(passthrough)
    except TypeError as exc:
        if passthrough:
            raise
        if "positional" not in str(exc) and "argument" not in str(exc):
            raise
        result = main_func()
    return int(result or 0)


if __name__ == "__main__":
    raise SystemExit(main())
