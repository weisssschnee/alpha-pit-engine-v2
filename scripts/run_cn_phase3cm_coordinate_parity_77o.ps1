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
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$SplitHash = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"

Push-Location $RepoRoot
try {
    & $PythonExe scripts\audit_cn_phase3cm_streaming_coordinate_parity.py `
        --candidate-table (Join-Path $CandidateRoot "preflight_session_candidates.csv") `
        --field-sidecar-root (Join-Path $RuntimeRoot "session_time_major_train_v2") `
        --label-sidecar-root (Join-Path $RuntimeRoot "session_time_major_train_v3_labels") `
        --legacy-pnl (Join-Path $RuntimeRoot "parity\session_legacy_global\phase3cm_portfolio_pnl_rows.csv") `
        --split-manifest $SplitManifest `
        --split-manifest-hash $SplitHash `
        --output (Join-Path $RuntimeRoot "parity\CN_SESSION_FULL_COORDINATE_PARITY.json") `
        --pair-count 14 `
        --compute-threads $ComputeThreads `
        --numeric-tolerance 1e-8 `
        --signal-tolerance 1e-7
    if ($LASTEXITCODE -ne 0) { throw "session full-coordinate parity failed: $LASTEXITCODE" }
}
finally {
    Pop-Location
}
