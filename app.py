from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from importlib import import_module


ROUTES: dict[str, str] = {
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
    "phase3ce-unsafe-motif-quarantine-audit": "our_system_phase2.runtime.phase3ce_unsafe_motif_quarantine_audit",
    "phase3ce1-search-memory-blocked-view": "our_system_phase2.runtime.phase3ce1_search_memory_blocked_view",
    "phase3ce1-g2-input-gate-smoke": "our_system_phase2.runtime.phase3ce1_g2_input_gate_smoke",
    "phase3ce2-typed-primitive-candidate-pack-canary": "our_system_phase2.runtime.phase3ce2_typed_primitive_candidate_pack_canary",
    "phase3ce2-typed-primitive-evaluator-smoke": "our_system_phase2.runtime.phase3ce2_typed_primitive_evaluator_smoke",
    "phase3cf-large-search-prelaunch": "our_system_phase2.runtime.phase3cf_large_search_prelaunch",
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
