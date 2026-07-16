param(
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
    [string]$MinuteSourceRoot = "D:\ChengboRemote\data\cn_true1min_development_only_release_v1_20260712_77o",
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
$CandidateRoot = Join-Path $RepoRoot "runtime\cn_compositional_nline_large_search_20260715"
$SessionSourceRoot = Join-Path $CandidateRoot "resource_preflight\session_shards"
New-Item -ItemType Directory -Force -Path $RuntimeRoot | Out-Null

Push-Location $RepoRoot
try {
    & $PythonExe scripts\build_cn_phase3cm_forward_label_sidecars.py `
        --source-root $MinuteSourceRoot `
        --output-root (Join-Path $RuntimeRoot "time_major_v1_labels") `
        --horizons 1,5,15,30 `
        --max-shards 16 `
        --polars-threads $PolarsThreads
    if ($LASTEXITCODE -ne 0) { throw "minute label sidecar build failed: $LASTEXITCODE" }

    & $PythonExe scripts\build_cn_phase3cm_time_major_sidecar.py `
        --source-root $SessionSourceRoot `
        --output-root (Join-Path $RuntimeRoot "session_time_major_v1") `
        --candidate-table (Join-Path $CandidateRoot "preflight_session_candidates.csv") `
        --max-shards 16 `
        --polars-threads $PolarsThreads `
        --parity
    if ($LASTEXITCODE -ne 0) { throw "session time-major sidecar build failed: $LASTEXITCODE" }

    & $PythonExe scripts\build_cn_phase3cm_forward_label_sidecars.py `
        --source-root $SessionSourceRoot `
        --output-root (Join-Path $RuntimeRoot "session_time_major_v1_labels") `
        --horizons 1,5,15,30 `
        --max-shards 16 `
        --polars-threads $PolarsThreads
    if ($LASTEXITCODE -ne 0) { throw "session label sidecar build failed: $LASTEXITCODE" }

    Set-Content -LiteralPath (Join-Path $RuntimeRoot "SIDECAR_BUILD_COMPLETE.txt") -Value "SIDECAR_BUILD_COMPLETE"
}
finally {
    Pop-Location
}
