from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
LAUNCHER = REPO / "scripts/run_cn_phase3cm_current_kernel_146_parity_77o.ps1"


def test_146_replay_launcher_gates_before_output_and_heavy_processes() -> None:
    script = LAUNCHER.read_text(encoding="utf-8-sig")

    portfolio_gate = script.index("frozen portfolio kernel source SHA-256 drift")
    sidecar_gate = script.index(
        "$FieldClosure = Confirm-CnSidecarClosure"
    )
    partition_gate = script.index(
        "partition union drifts from full frozen active candidate table"
    )
    main_output_creation = script.index(
        "New-Item -ItemType Directory -Force -Path $RunRoot",
        partition_gate,
    )
    process_start = script.index("Start-Process -FilePath $PowerShellExe")

    assert portfolio_gate < sidecar_gate < partition_gate < main_output_creation
    assert main_output_creation < process_start
    assert "Get-CnSha256 -Path $ShardPath" in script
    assert "capacity receipt drift" in script
    assert "historical command binding drift" in script
    assert "launch workspace must be clean" in script
    assert "merge-base --is-ancestor" in script


def test_146_replay_launcher_freezes_topology_and_authority() -> None:
    script = LAUNCHER.read_text(encoding="utf-8-sig")

    assert '"--phase", "E"' in script
    assert '"--execution-plan", $Evidence.execution_plan' in script
    assert "PHASE_E_EXACT_PLAN_PARITY_REPLAY" in script
    assert "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR" in script
    assert "run_cn_phase3cm_phase_e_qualification_77o.ps1" not in script
    assert "[int]$Execution.heavy_processes -ne 2" in script
    assert "[int]$Execution.compute_threads_per_process -ne 11" in script
    assert "[int]$Execution.active_native_compute_threads_total -ne 22" in script
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


def test_146_replay_launcher_is_resumable_and_fail_closed_without_speed_gate() -> None:
    script = LAUNCHER.read_text(encoding="utf-8-sig")

    assert "[switch]$Resume" in script
    assert 'if ($Resume) { $Arguments += "--resume" }' in script
    assert "GLOBAL_RSS_HARD_GATE" in script
    assert "PROCESS_RSS_HARD_GATE" in script
    assert "WALL_HARD_GATE" in script
    assert "OUTPUT_HARD_GATE" in script
    assert "Stop-CnProcessTrees -Roots $Roots" in script
    assert "$Qualification.semantic_parity_exact -eq $true" in script
    assert "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS" in script
    assert 'speedup_threshold_gate = "NOT_USED"' in script
    assert "wall_speedup -ge 2" not in script
    assert "Remove-Item -LiteralPath $StaleArtifact -Force" in script
    assert "historical_reference_root" in script

