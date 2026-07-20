from pathlib import Path
import subprocess

from scripts.preflight_cn_phase3cm_dag_cache import SOURCE_CLOSURE_PATHS


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_cn_phase3cm_1024_wave_77o.ps1"


def _script() -> str:
    return SCRIPT.read_text(encoding="utf-8-sig")


def test_1024_launch_authority_is_inside_source_closure() -> None:
    for relative_path in (
        "scripts/validate_cn_phase3cm_source_closure.py",
        "scripts/freeze_cn_phase3cm_1024_resource_contract.py",
        "scripts/run_cn_phase3cm_1024_wave_77o.ps1",
    ):
        assert relative_path in SOURCE_CLOSURE_PATHS


def test_1024_launcher_powershell_parses() -> None:
    command = (
        "$errors=$null;"
        f"[System.Management.Automation.Language.Parser]::ParseFile('{SCRIPT}',"
        "[ref]$null,[ref]$errors)|Out-Null;"
        "if($errors.Count){$errors|ForEach-Object{$_.ToString()};exit 1}"
    )
    completed = subprocess.run(
        ["powershell", "-NoProfile", "-Command", command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_1024_launcher_hard_gates_the_frozen_wave_inputs() -> None:
    script = _script()
    for marker in (
        "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND",
        "CN_PHASE3CM_PHASE_E_EXECUTION_PLANS_FROZEN",
        "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS",
        "CN_PHASE3CM_1024_RESOURCE_CONTRACT_PASS",
        "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS",
        "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_READJUDICATED_PASS",
        "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS",
        "CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH",
        "validate_cn_phase3cm_source_closure.py",
    ):
        assert marker in script
    assert "$ExpectedActivePairs = 584" in script
    assert "$ExpectedSessionPairs = 440" in script
    assert "$ExpectedTotalPairs = 1024" in script
    assert "$ExpectedCandidateMembers = 2048" in script
    assert "ExpectedSourceClosureManifestSha256" in script
    assert "ExpectedFrozenBindingSha256" in script
    assert "ActiveCapacityValidation.dag_plan_hash" in script
    assert "SessionCapacityValidation.dag_plan_hash" in script
    assert "ExpectedDagPlanHash" in script


def test_1024_launcher_freezes_topology_resources_and_resume() -> None:
    script = _script()
    assert "[int]$Contract.heavy_processes -ne 2" in script
    assert "[int]$Contract.global_active_native_compute_threads -gt 24" in script
    assert "rss_hard_bytes_by_backend.active_bar" in script
    assert "rss_hard_bytes_by_backend.stock_session" in script
    assert "$GlobalHardRss = [int64]$Resources.global_rss_hard_bytes" in script
    assert "$WallSecondsHardMax = [double]$Resources.wall_seconds_hard_max" in script
    assert "host_hours_hard_max -ne 12.0" in script
    assert "$ActiveRssGateFailure = $true" in script
    assert "$SessionRssGateFailure = $true" in script
    assert "active_rss_gate_pass" in script
    assert "session_rss_gate_pass" in script
    assert "checkpoint_every_blocks -le 0" in script
    assert "CN_STREAMING_CHECKPOINT_MANIFEST.json" in script
    assert 'if ($Resume) { $Arguments += "--resume" }' in script
    assert "Stop-CnProcessTrees -Roots $Roots" in script
    for variable in (
        "NUMBA_NUM_THREADS",
        "ARROW_NUM_THREADS",
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_MAX_THREADS",
        "POLARS_MAX_THREADS",
    ):
        assert variable in script


def test_1024_launcher_does_not_reintroduce_r6_or_speed_gate() -> None:
    script = _script()
    for obsolete in (
        "PredecessorEngineeringReceipt",
        "FrozenR6SuccessorReceiptSha256",
        "cn_phase3cm_r6_successor_engineering_receipt_v1",
        "CN_PHASE3CM_R6_SUCCESSOR_ENGINEERING_PASS",
        "backend_pair_counts.active_bar -ne 36",
        "backend_pair_counts.stock_session -ne 28",
        "pair_count -ne 64",
    ):
        assert obsolete not in script
    assert '[string]$Resources.speedup_threshold -ne "NONE"' in script
    assert 'speedup_threshold = "NONE"' in script
    assert 'performance_threshold_gate = "NOT_USED"' in script
    assert "speedup -ge" not in script.lower()
    assert "speedup -lt" not in script.lower()


def test_1024_launcher_keeps_research_boundaries_and_authority() -> None:
    script = _script()
    assert 'validation_reads = 0' in script
    assert 'holdout_reads = 0' in script
    assert 'forward_2026_reads = 0' in script
    assert 'promotion = "FORBIDDEN"' in script
    assert 'strict_stage_a = "NOT_AUTHORIZED"' in script
    assert 'formal_evaluator_authority = "UNCHANGED"' in script
    assert 'streaming_backend_authority = "EXPERIMENTAL_BACKEND"' in script
    assert 'kernel_state = "PARTIALLY_QUALIFIED"' in script
    assert 'cross_epoch_adaptive_memory = "FORBIDDEN"' in script
    assert '"--phase", "E"' in script
    assert '"--capacity-receipt-hash"' in script
    assert '"--max-block-rows"' in script
