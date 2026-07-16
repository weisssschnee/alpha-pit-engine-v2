param(
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
    [int]$PolarsThreads = 3,
    [int]$NumexprThreads = 4
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = [string]$PolarsThreads

$RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716"
$CandidateRoot = Join-Path $RepoRoot "runtime\cn_compositional_nline_large_search_20260715"
$ReferenceRoot = Join-Path $RuntimeRoot "parity\session_global_train_reference"
$LegacyOutput = Join-Path $RuntimeRoot "parity\session_legacy_global"
$LegacyReport = Join-Path $RuntimeRoot "parity\session_legacy_global_report"
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$SplitHash = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"

Push-Location $RepoRoot
try {
    & $PythonExe scripts\build_cn_phase3cm_global_reference_fixture.py `
        --input-root (Join-Path $RuntimeRoot "session_time_major_train_v2") `
        --output-root $ReferenceRoot `
        --split-manifest-hash $SplitHash `
        --polars-threads $PolarsThreads
    if ($LASTEXITCODE -ne 0) { throw "session global reference fixture failed: $LASTEXITCODE" }

    $env:POLARS_MAX_THREADS = "1"
    $env:NUMEXPR_MAX_THREADS = [string]$NumexprThreads
    & $PythonExe -m our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit `
        --candidate-audit (Join-Path $CandidateRoot "preflight_session_candidates.csv") `
        --shard-root $ReferenceRoot `
        --output-root $LegacyOutput `
        --report-root $LegacyReport `
        --candidate-limit 28 `
        --max-shards 1 `
        --sample-trade-times-per-shard 0 `
        --no-event-aware-sample-times `
        --horizons 1,5,15,30 `
        --split-manifest $SplitManifest `
        --candidate-receipt-table (Join-Path $CandidateRoot "preflight_session_candidate_receipts.jsonl") `
        --candidate-pair-receipt-table (Join-Path $CandidateRoot "preflight_session_pair_receipts.jsonl") `
        --unified-registry (Join-Path $RepoRoot "reports\cn_unified_capability_discovery_20260714\completed_f8169e1\registry\unified_capability_registry.json") `
        --data-release-hash cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827 `
        --min-obs-per-time 20 `
        --cost-bps 5 `
        --top-quantile 0.2 `
        --portfolio-mode long_only_top `
        --fast-mode `
        --numexpr-threads $NumexprThreads `
        --disable-incremental-checkpoints `
        --persistent-cache-mode off `
        --enforce-pair-shared-support `
        --write-pnl-rows `
        --write-reward-atoms
    if ($LASTEXITCODE -ne 0) { throw "session global legacy reference failed: $LASTEXITCODE" }
}
finally {
    Pop-Location
}
