param(
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
$OutputRoot = Join-Path $RuntimeRoot "shard_layout_audit"
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null

Push-Location $RepoRoot
try {
    foreach ($name in @("time_major_train_v2", "session_time_major_train_v2")) {
        $output = Join-Path $OutputRoot ($name + ".json")
        & $PythonExe scripts\audit_cn_phase3cm_shard_layout.py `
            --input-root (Join-Path $RuntimeRoot $name) `
            --polars-threads $PolarsThreads | Set-Content -LiteralPath $output -Encoding utf8
        if ($LASTEXITCODE -ne 0) { throw "shard layout audit failed for ${name}: $LASTEXITCODE" }
        Get-Content -LiteralPath $output
    }
}
finally {
    Pop-Location
}
