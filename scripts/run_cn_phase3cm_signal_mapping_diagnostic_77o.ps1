param(
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
    [int]$ComputeThreads = 4
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = [string]$ComputeThreads
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = "1"

$RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716"
$CandidateRoot = Join-Path $RepoRoot "runtime\cn_compositional_nline_large_search_20260715"

Push-Location $RepoRoot
try {
    & $PythonExe scripts\diagnose_cn_phase3cm_signal_mapping_parity.py `
        --candidate-table (Join-Path $CandidateRoot "preflight_session_candidates.csv") `
        --candidate-id "cn.comp.139919dc6d410acad453" `
        --field-sidecar-root (Join-Path $RuntimeRoot "session_time_major_train_v2") `
        --label-sidecar-root (Join-Path $RuntimeRoot "session_time_major_train_v3_labels") `
        --global-fixture (Join-Path $RuntimeRoot "parity\session_global_train_reference\shard_00\phase3aq_wide_true1min\canary\phase3aq_true_1min_formula_canary.parquet") `
        --start-time "2024-01-02" `
        --target-time "2024-05-10T15:00:00" `
        --compute-threads $ComputeThreads
    if ($LASTEXITCODE -ne 0) { throw "signal mapping diagnostic failed: $LASTEXITCODE" }
}
finally {
    Pop-Location
}
