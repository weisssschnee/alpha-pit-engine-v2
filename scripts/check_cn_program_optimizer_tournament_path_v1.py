"""Independent static checker for the frozen Program optimizer tournament path."""

from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def stable_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


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
    optimizer_text = optimizer.read_text(encoding="utf-8")
    runner_text = runner.read_text(encoding="utf-8")
    wrapper_text = wrapper.read_text(encoding="utf-8")
    report_path = (
        ROOT
        / "runtime/run_plans/"
        "cn_program_optimizer_projection_fairness_p0_report.json"
    )
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report_body = dict(report)
    report_hash = str(report_body.pop("report_sha256", ""))
    checks = {
        "route_imports_runner_only_after_authority_checks": (
            "verify_campaign_authorization_binding" in route_calls
            and "verify_search_feedback_boundary" in route_calls
            and "scripts.run_cn_program_optimizer_tournament_v1" in route.read_text(
                encoding="utf-8"
            )
        ),
        "canonical_77o_wrapper_binds_project_control_and_fresh_root": all(
            token in wrapper_text
            for token in (
                "cn-program-optimizer-tournament-v1",
                "ProjectControlAdmissionSha256",
                "RepoSha",
                "TargetRunId",
                "must be absent",
            )
        ),
        "runner_uses_common_contract": (
            "ProgramOptimizerTournamentV1" in runner_text
            and "ProgramOptimizerObservationV1" in runner_text
            and "tell" in runner_calls
        ),
        "runner_reuses_phase_c_evaluator": (
            "scripts" in runner_imports
            and "run_cn_joint_program_phase_c_v0 as engine" in runner_text
            and "run_cn_joint_program_search_v2_canary" not in runner_text
            and "engine._evaluate_record =" not in runner_text
            and "def _evaluate_record" in phase_c.read_text(encoding="utf-8")
        ),
        "execution_surface_import_smoke_precedes_project_control_route": (
            "import app" in wrapper_text
            and "import our_system_phase2.runtime.cn_program_optimizer_tournament_v1"
            in wrapper_text
            and "import scripts.run_cn_program_optimizer_tournament_v1"
            in wrapper_text
            and wrapper_text.index(
                "import scripts.run_cn_program_optimizer_tournament_v1"
            )
            < wrapper_text.index("& $python @routeArgs")
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
        "tpe_samples_only_legal_template_conditional_exact_programs": all(
            token in optimizer_text
            for token in (
                "TEMPLATE_CONDITIONAL_EXACT_IDENTITY_CATEGORY_V1",
                "program_exact_identity",
                "NORMALIZED_HAMMING_OVER_FULL_FROZEN_PROGRAM_GENES_V1",
                "FULL_LEGAL_SET_MINIMUM_STRUCTURAL_DISTANCE_V1",
            )
        ),
        "tpe_has_no_first_or_global_fallback_path": (
            "replacement = candidates[0]" not in optimizer_text
            and "replacement = remaining[0]" not in optimizer_text
            and "emit_global_fallback" not in optimizer_text
        ),
        "surrogate_scores_full_eligible_set_in_inference_batches": (
            "remaining[: self.candidate_pool_size]" not in optimizer_text
            and "self._acquisition_rows(remaining)" in optimizer_text
            and "INFERENCE_BATCH_SIZE_ONLY_FULL_ELIGIBLE_SET_ALWAYS_SCORED"
            in optimizer_text
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
        "projection_fairness_stress_report_is_self_hashed_and_zero_financial": (
            bool(report_hash)
            and report_hash == stable_hash(report_body)
            and report.get("status") == "PASS"
            and int(report.get("program_space_entry_count", 0)) == 3616
            and int(report.get("legal_tpe_lane_exact_coverage", 0)) == 3616
            and int(report.get("global_fallback_count", -1)) == 0
            and int(report.get("financial_reads", -1)) == 0
            and report.get("tournament") == "NOT_RUN"
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
