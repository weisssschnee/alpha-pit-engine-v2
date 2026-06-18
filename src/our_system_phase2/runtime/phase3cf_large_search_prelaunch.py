"""Phase3CF reward-gated large-search prelaunch.

This module freezes the post-CE2 launch contract for the next true-1min
large search. It validates typed-primitive plumbing evidence, writes a command
manifest, and does not run heavy search or alter X0/R3.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = REPO / "runtime/phase3cf_reward_gated_large_search_prelaunch_20260618"
DEFAULT_REPORT_ROOT = REPO / "reports/phase3cf_reward_gated_large_search_prelaunch_20260618"
DEFAULT_LOCAL_SHARD_ROOT = Path(
    "G:/Project_V7_Rotation/alpha_pit_data_feature_workspace_20260531/runtime/phase3au_aq_only_true1min_sharded_20260611"
)
DEFAULT_COMPANY_SHARD_ROOT = Path("D:/HermesWorker/workspace/phase3aj_new_data_current/runtime/phase3au_company_full_true1min_sharded_20260611")
DEFAULT_COMPANY_REPO_ROOT = Path("D:/HermesWorker/workspace/alpha_pit_true1min_engine_20260619")

REQUIRED_EVIDENCE = {
    "ce1_summary": REPO / "reports/PHASE3CE1_TYPED_PRIMITIVE_GATE_IMPLEMENTATION_SUMMARY_20260618.md",
    "ce2_fullwidth_eval_summary": REPO
    / "reports/phase3ce2_fullwidth_realdata_eval_20260618/phase3as_true_1min_sidecar_canary_eval_summary.json",
    "ce2_fullwidth_compact_summary": REPO
    / "reports/phase3ce2_fullwidth_realdata_eval_20260618/phase3ce2_fullwidth_eval_compact_summary.csv",
    "ce2_fullwidth_panel_summary": REPO
    / "reports/phase3ce2_fullwidth_validation_panel_20260618/phase3ce2_fullwidth_validation_panel_summary.json",
    "ce2_fullwidth_still_blocked": REPO
    / "runtime/phase3ce2_fullwidth_phase3ar_sidecar_adapter_20260618/phase3ar_still_blocked_formula_rows.json",
    "typed_gate_contract": REPO / "reports/PHASE3CE1_TYPED_PRIMITIVE_GATE_CONTRACT_20260618.md",
    "search_memory_blocked_view": REPO
    / "reports/phase3ce1_search_memory_blocked_view_20260618/phase3ce1_search_memory_blocked_view_summary.json",
}


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _command(parts: list[str]) -> str:
    return " ".join(parts)


def _search_commands(
    *,
    repo_root: str,
    python_exe: str,
    shard_root: str,
    scale: str,
) -> dict[str, str]:
    if scale == "company_heavy":
        max_shards = "16"
        sample_times = "96"
        bs_seed = "512"
        bs_each = "512"
        bt_seed = "512"
        bt_each = "512"
        top = "320"
        bz_limit = "128"
        bz_sample = "180"
    else:
        max_shards = "8"
        sample_times = "64"
        bs_seed = "256"
        bs_each = "256"
        bt_seed = "256"
        bt_each = "256"
        top = "160"
        bz_limit = "64"
        bz_sample = "120"

    app = "app.py"
    bs_out = "runtime/phase3cf_bs_adaptive_ucb_cem_20260618"
    bs_rep = "reports/phase3cf_bs_adaptive_ucb_cem_20260618"
    bt_out = "runtime/phase3cf_bt_ast_fresh_20260618"
    bt_rep = "reports/phase3cf_bt_ast_fresh_20260618"
    ca_out = "runtime/phase3cf_bz_candidate_audit_20260618"
    bz_out = "runtime/phase3cf_bz_fragment_replay_20260618"
    bz_rep = "reports/phase3cf_bz_fragment_replay_20260618"

    return {
        "cd_repo": f"Set-Location '{repo_root}'",
        "set_pythonpath": "$env:PYTHONPATH='src'",
        "bs_adaptive_ucb_cem": _command(
            [
                f"'{python_exe}'",
                app,
                "phase3bs-adaptive-ucb-cem-practice",
                "--allow-diagnostic",
                "--",
                "--shard-root",
                f"'{shard_root}'",
                "--memory-root",
                "runtime/search_memory",
                "--output-root",
                bs_out,
                "--report-root",
                bs_rep,
                "--seed-candidates",
                bs_seed,
                "--adaptive-cem-candidates",
                bs_each,
                "--adaptive-hybrid-candidates",
                bs_each,
                "--cem-dominant-ucb-candidates",
                bs_each,
                "--cem-dominant-rx-candidates",
                bs_each,
                "--max-shards",
                max_shards,
                "--sample-trade-times-per-shard",
                sample_times,
                "--top-decisions",
                top,
                "--horizons",
                "1,5,15,30",
                "--min-obs-per-time",
                "20",
                "--seed-exploration",
                "0.35",
                "--learning-rate",
                "0.20",
                "--entropy-floor",
                "0.18",
                "--min-feedback-eligible",
                "32",
            ]
        ),
        "bt_ast_fresh": _command(
            [
                f"'{python_exe}'",
                app,
                "phase3bt-ast-algorithm-bakeoff",
                "--allow-diagnostic",
                "--",
                "--shard-root",
                f"'{shard_root}'",
                "--memory-root",
                "runtime/search_memory",
                "--output-root",
                bt_out,
                "--report-root",
                bt_rep,
                "--max-shards",
                max_shards,
                "--sample-trade-times-per-shard",
                sample_times,
                "--seed-candidates",
                bt_seed,
                "--cem-candidates",
                bt_each,
                "--hybrid-candidates",
                bt_each,
                "--dominant-candidates",
                bt_each,
                "--fresh-hybrid-candidates",
                bt_each,
                "--top-decisions",
                top,
                "--horizons",
                "1,5,15,30",
                "--min-obs-per-time",
                "20",
                "--seed-exploration",
                "0.45",
                "--learning-rate",
                "0.20",
                "--entropy-floor",
                "0.20",
                "--min-feedback-eligible",
                "32",
            ]
        ),
        "ca_bridge": _command(
            [
                f"'{python_exe}'",
                app,
                "phase3ca-build-bz-candidate-audit",
                "--allow-diagnostic",
                "--",
                "--source-root",
                bs_rep,
                "--source-root",
                bt_rep,
                "--output-root",
                ca_out,
                "--top-n",
                bz_limit,
            ]
        ),
        "bz_fragment_replay": _command(
            [
                f"'{python_exe}'",
                app,
                "phase3bz-fragment-replay-audit",
                "--allow-diagnostic",
                "--",
                "--bx-audit",
                f"{ca_out}/phase3ca_bz_candidate_audit.csv",
                "--shard-root",
                f"'{shard_root}'",
                "--output-root",
                bz_out,
                "--report-root",
                bz_rep,
                "--candidate-limit",
                bz_limit,
                "--max-shards",
                max_shards,
                "--sample-trade-times-per-shard",
                bz_sample,
                "--horizons",
                "1,5,15,30",
                "--min-obs-per-time",
                "20",
                "--cost-bps",
                "5",
                "--numexpr-threads",
                "8",
            ]
        ),
    }


def _validate_evidence() -> tuple[list[dict[str, Any]], list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    for name, path in REQUIRED_EVIDENCE.items():
        exists = path.exists()
        rows.append({"name": name, "path": str(path), "exists": exists})
        if not exists:
            blockers.append(f"missing:{name}")

    if not blockers:
        eval_summary = _read_json(REQUIRED_EVIDENCE["ce2_fullwidth_eval_summary"])
        if int(eval_summary.get("input_candidate_count", -1)) != 104:
            blockers.append("ce2_fullwidth_input_candidate_count_not_104")
        if int(eval_summary.get("evaluated_candidate_count", -1)) != 104:
            blockers.append("ce2_fullwidth_evaluated_candidate_count_not_104")
        if int(eval_summary.get("error_count", -1)) != 0:
            blockers.append("ce2_fullwidth_eval_errors")
        if int(eval_summary.get("panel_codes", 0)) < 5000:
            blockers.append("ce2_fullwidth_panel_too_narrow")
        if int(eval_summary.get("evaluated_trade_time_count", 0)) < 200:
            blockers.append("ce2_fullwidth_trade_time_sample_too_small")

        blocked = _read_json(REQUIRED_EVIDENCE["ce2_fullwidth_still_blocked"])
        if int(blocked.get("candidate_count", -1)) != 0:
            blockers.append("ce2_fullwidth_still_blocked_nonzero")

    return rows, blockers


def build_prelaunch(
    *,
    output_root: Path,
    report_root: Path,
    repo_root: Path,
    local_python: Path,
    company_python: Path,
    local_shard_root: Path,
    company_shard_root: Path,
) -> dict[str, Any]:
    output_root = _resolve(output_root)
    report_root = _resolve(report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    evidence_rows, blockers = _validate_evidence()
    ready = not blockers
    company_commands = _search_commands(
        repo_root=str(DEFAULT_COMPANY_REPO_ROOT),
        python_exe=str(company_python),
        shard_root=str(company_shard_root),
        scale="company_heavy",
    )
    local_commands = _search_commands(
        repo_root=str(repo_root),
        python_exe=str(local_python),
        shard_root=str(local_shard_root),
        scale="local_checkpoint",
    )
    command_manifest = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "phase": "Phase3CF",
        "purpose": "reward-gated true1min large search launch commands after CE2 typed primitive validation",
        "company_heavy": company_commands,
        "local_checkpoint": local_commands,
        "execution_order": [
            "bs_adaptive_ucb_cem",
            "bt_ast_fresh",
            "ca_bridge",
            "bz_fragment_replay",
        ],
        "hard_boundaries": [
            "true trade_time 1min shards only",
            "no old 1D/tdx official backbone",
            "X0/R3 read-only",
            "CA bridge is ranking only, not proof",
            "BZ fragment replay is mandatory before followup language",
            "do not kill unrelated crypto-line processes",
        ],
    }
    _write_json(output_root / "phase3cf_command_manifest.json", command_manifest)

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3CF_LARGE_SEARCH_PRELAUNCH_READY" if ready else "PHASE3CF_LARGE_SEARCH_PRELAUNCH_BLOCKED",
        "ready_to_launch": ready,
        "blockers": blockers,
        "evidence": evidence_rows,
        "run_plan": str(REPO / "runtime/run_plans/phase3cf_reward_gated_large_search_run_plan_20260618.json"),
        "command_manifest": str(output_root / "phase3cf_command_manifest.json"),
        "primary_launch_target": "company_heavy",
        "secondary_launch_target": "local_checkpoint_only",
        "search_lanes": {
            "bs_adaptive_ucb_cem": "fresh + adaptive UCB/CEM, entropy floor retained",
            "bt_ast_fresh": "AST fresh/hybrid variants, fresh quota retained",
            "ca_bridge": "dedupe/ranking bridge into BZ only",
            "bz_fragment_replay": "Sortino/MCMC/day-block/cost fragment replay gate",
        },
    }
    _write_json(output_root / "phase3cf_large_search_prelaunch_summary.json", summary)
    _write_json(report_root / "phase3cf_large_search_prelaunch_summary.json", summary)

    md = [
        "# Phase3CF Reward-Gated Large Search Prelaunch",
        "",
        f"Decision: `{summary['decision']}`",
        "",
        "## Evidence",
        "",
        "| name | exists | path |",
        "|---|---:|---|",
    ]
    for row in evidence_rows:
        md.append(f"| {row['name']} | {row['exists']} | `{row['path']}` |")
    md.extend(
        [
            "",
            "## Launch Order",
            "",
            "1. `phase3bs-adaptive-ucb-cem-practice`",
            "2. `phase3bt-ast-algorithm-bakeoff`",
            "3. `phase3ca-build-bz-candidate-audit`",
            "4. `phase3bz-fragment-replay-audit`",
            "",
            "## Boundaries",
            "",
            "- true `trade_time` 1min shards only",
            "- no old 1D/TDX official backbone",
            "- X0/R3 read-only",
            "- proxy metrics are not promotion evidence",
            "- BZ fragment replay is mandatory before follow-up candidates",
            "",
            "## Command Manifest",
            "",
            f"`{output_root / 'phase3cf_command_manifest.json'}`",
            "",
        ]
    )
    (report_root / "PHASE3CF_REWARD_GATED_LARGE_SEARCH_PRELAUNCH_20260618.md").write_text(
        "\n".join(md),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--repo-root", type=Path, default=REPO)
    parser.add_argument("--local-python", type=Path, default=Path("G:/PythonProject/.venv/Scripts/python.exe"))
    parser.add_argument("--company-python", type=Path, default=Path("D:/HermesWorker/workspace/.venv/Scripts/python.exe"))
    parser.add_argument("--local-shard-root", type=Path, default=DEFAULT_LOCAL_SHARD_ROOT)
    parser.add_argument("--company-shard-root", type=Path, default=DEFAULT_COMPANY_SHARD_ROOT)
    args = parser.parse_args(argv)
    summary = build_prelaunch(
        output_root=args.output_root,
        report_root=args.report_root,
        repo_root=args.repo_root,
        local_python=args.local_python,
        company_python=args.company_python,
        local_shard_root=args.local_shard_root,
        company_shard_root=args.company_shard_root,
    )
    return 0 if summary["ready_to_launch"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
