param(
    [ValidateSet("active_bar", "stock_session", "all")]
    [string]$Backend = "all",
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
    [int]$PolarsThreads = 3
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
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$SplitHash = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"
$routes = @()
if ($Backend -in @("active_bar", "all")) {
    $routes += ,@("time_major_train_v2", "time_major_train_v3_labels")
}
if ($Backend -in @("stock_session", "all")) {
    $routes += ,@("session_time_major_train_v2", "session_time_major_train_v3_labels")
}

Push-Location $RepoRoot
try {
    foreach ($route in $routes) {
        & $PythonExe scripts\build_cn_phase3cm_forward_label_sidecars.py `
            --source-root (Join-Path $RuntimeRoot $route[0]) `
            --output-root (Join-Path $RuntimeRoot $route[1]) `
            --split-manifest $SplitManifest `
            --split-manifest-hash $SplitHash `
            --horizons 1,5,15,30 `
            --max-shards 16 `
            --polars-threads $PolarsThreads
        if ($LASTEXITCODE -ne 0) { throw "global-continuity label build failed for $($route[0]): $LASTEXITCODE" }
    }
}
finally {
    Pop-Location
}
