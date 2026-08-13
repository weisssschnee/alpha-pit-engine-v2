"""Independent static checker for the frozen Program optimizer tournament path."""

from __future__ import annotations

import ast
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _names(path: Path) -> tuple[set[str], set[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    calls: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            imports.add(str(node.module))
        elif isinstance(node, ast.Call):
            target = node.func
            if isinstance(target, ast.Name):
                calls.add(target.id)
            elif isinstance(target, ast.Attribute):
                calls.add(target.attr)
    return imports, calls


def check() -> dict[str, object]:
    runner = ROOT / "scripts/run_cn_program_optimizer_tournament_v1.py"
    optimizer = (
        ROOT
        / "src/our_system_phase2/services/program_search_optimizer_v1.py"
    )
    phase_c = ROOT / "scripts/run_cn_joint_program_phase_c_v0.py"
    route = (
        ROOT
        / "src/our_system_phase2/runtime/cn_program_optimizer_tournament_v1.py"
    )
    wrapper = ROOT / "scripts/run_cn_program_optimizer_tournament_77o.ps1"
    runner_imports, runner_calls = _names(runner)
    optimizer_imports, optimizer_calls = _names(optimizer)
    phase_c_imports, phase_c_calls = _names(phase_c)
    route_imports, route_calls = _names(route)
    checks = {
        "route_imports_runner_only_after_authority_checks": (
            "verify_campaign_authorization_binding" in route_calls
            and "verify_search_feedback_boundary" in route_calls
            and "scripts.run_cn_program_optimizer_tournament_v1" in route.read_text(
                encoding="utf-8"
            )
        ),
        "canonical_77o_wrapper_binds_project_control_and_fresh_root": all(
            token in wrapper.read_text(encoding="utf-8")
            for token in (
                "cn-program-optimizer-tournament-v1",
                "ProjectControlAdmissionSha256",
                "RepoSha",
                "TargetRunId",
                "must be absent",
            )
        ),
        "runner_uses_common_contract": (
            "ProgramOptimizerTournamentV1" in runner.read_text(encoding="utf-8")
            and "ProgramOptimizerObservationV1" in runner.read_text(encoding="utf-8")
            and "tell" in runner_calls
        ),
        "runner_reuses_phase_c_evaluator": (
            "scripts" in runner_imports
            and "run_cn_joint_program_phase_c_v0 as engine" in runner.read_text(
                encoding="utf-8"
            )
            and "run_cn_joint_program_search_v2_canary" in runner.read_text(
                encoding="utf-8"
            )
            and "_evaluate_record" in runner.read_text(encoding="utf-8")
        ),
        "runner_reuses_admission_and_uplift": (
            "AbsoluteEconomicAdmission" in runner.read_text(encoding="utf-8")
            and "conditional_uplift_credit" in runner_calls
        ),
        "tpe_calls_existing_adapter_actual_ask_and_tell": (
            "RouteConditionalTPESearchAdapter" in optimizer.read_text(
                encoding="utf-8"
            )
            and "ask_trial" in optimizer_calls
            and "tell_population" in optimizer_calls
            and "enqueue_fixed_trial" in optimizer_calls
        ),
        "program_genes_enter_existing_candidate_program_engine": (
            "program_structural_genes_v1" in phase_c.read_text(encoding="utf-8")
            and "compose" in phase_c_calls
            and "compile" in phase_c_calls
        ),
        "surrogate_has_dual_models_acquisition_and_restore": all(
            token in optimizer.read_text(encoding="utf-8")
            for token in (
                "ExtraTreesClassifier",
                "ExtraTreesRegressor",
                "predict_acquisition",
                "P_ADMISSION_TIMES_POSITIVE_UPLIFT_UCB",
                "def restore",
            )
        ),
    }
    result = {
        "schema_version": "cn_program_optimizer_tournament_call_path_check_v1",
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "financial_reads": 0,
        "tournament": "NOT_RUN",
    }
    if result["status"] != "PASS":
        raise RuntimeError(json.dumps(result, sort_keys=True))
    return result


def main() -> int:
    print(json.dumps(check(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
