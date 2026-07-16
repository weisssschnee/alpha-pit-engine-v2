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
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$SplitHash = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"
$AuditRoot = Join-Path $RuntimeRoot "calendar_audit"
New-Item -ItemType Directory -Force -Path $AuditRoot | Out-Null

$roots = @(
    "time_major_train_v2",
    "time_major_train_v3_labels",
    "session_time_major_train_v2",
    "session_time_major_train_v3_labels"
)

Push-Location $RepoRoot
try {
    foreach ($name in $roots) {
        $output = Join-Path $AuditRoot ($name + ".json")
        & $PythonExe scripts\audit_cn_phase3cm_sidecar_calendar.py `
            --input-root (Join-Path $RuntimeRoot $name) `
            --split-manifest $SplitManifest `
            --split-manifest-hash $SplitHash `
            --polars-threads $PolarsThreads | Set-Content -LiteralPath $output -Encoding utf8
        if ($LASTEXITCODE -ne 0) { throw "calendar audit failed for ${name}: $LASTEXITCODE" }
        Get-Content -LiteralPath $output
    }
}
finally {
    Pop-Location
}
