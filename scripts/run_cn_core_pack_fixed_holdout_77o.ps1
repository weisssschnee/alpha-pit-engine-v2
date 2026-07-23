param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$FundamentalRoot,
    [Parameter(Mandatory = $true)]
    [string]$ChipRoot,
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
    [string]$CampaignRoot = "D:\ChengboRemote\runtime\cn_core_pack_large_development_20260722_9f3a5f2_30t_r5",
    [string]$MinuteSourceRoot = "D:\ChengboRemote\data\cn_true1min_development_only_release_v1_20260712_77o",
    [string]$OutputRoot = "D:\ChengboRemote\runtime\cn_core_pack_large_development_20260722_9f3a5f2_30t_r5\fixed_holdout_48d",
    [int]$PolarsThreads = 24,
    [int]$ActiveThreads = 30,
    [int]$SessionThreads = 2
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

$FreezeManifest = Join-Path $RepoRoot "runtime\run_plans\cn_core_pack_fixed_holdout_candidate_freeze_20260723.json"
$Registry = Join-Path $RepoRoot "runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json"
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$SplitManifestHash = "fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241"
$PreparedRoot = Join-Path $OutputRoot "prepared"
$ActiveFieldRoot = Join-Path $OutputRoot "sidecars\active_fields"
$ActiveLabelRoot = Join-Path $OutputRoot "sidecars\active_labels"
$SessionFieldRoot = Join-Path $OutputRoot "sidecars\session_fields"
$SessionLabelRoot = Join-Path $OutputRoot "sidecars\session_labels"
$EvaluationRoot = Join-Path $OutputRoot "evaluation"

New-Item -ItemType Directory -Force -Path $PreparedRoot | Out-Null

Push-Location $RepoRoot
try {
    & $PythonExe scripts\prepare_cn_core_pack_fixed_holdout.py `
        --campaign-root $CampaignRoot `
        --freeze-manifest $FreezeManifest `
        --output-root $PreparedRoot
    if ($LASTEXITCODE -ne 0) {
        throw "fixed holdout candidate preparation failed: $LASTEXITCODE"
    }

    & $PythonExe scripts\build_cn_phase3cm_time_major_sidecar.py `
        --source-root $MinuteSourceRoot `
        --evaluation-role holdout `
        --output-root $ActiveFieldRoot `
        --candidate-table (Join-Path $PreparedRoot "fixed_holdout_active_bar_candidates.csv") `
        --split-manifest $SplitManifest `
        --split-manifest-hash $SplitManifestHash `
        --max-shards 16 `
        --polars-threads $PolarsThreads `
        --parity
    if ($LASTEXITCODE -ne 0) {
        throw "active-bar holdout field sidecar build failed: $LASTEXITCODE"
    }

    & $PythonExe scripts\build_cn_phase3cm_forward_label_sidecars.py `
        --source-root $ActiveFieldRoot `
        --evaluation-role holdout `
        --output-root $ActiveLabelRoot `
        --split-manifest $SplitManifest `
        --split-manifest-hash $SplitManifestHash `
        --horizons 1,5,15,30 `
        --max-shards 16 `
        --polars-threads $PolarsThreads
    if ($LASTEXITCODE -ne 0) {
        throw "active-bar holdout label sidecar build failed: $LASTEXITCODE"
    }

    & $PythonExe scripts\build_cn_core_pack_validation_session_sidecar.py `
        --source-root $MinuteSourceRoot `
        --evaluation-role holdout `
        --output-root $SessionFieldRoot `
        --candidate-table (Join-Path $PreparedRoot "fixed_holdout_stock_session_candidates.csv") `
        --registry $Registry `
        --split-manifest $SplitManifest `
        --split-manifest-hash $SplitManifestHash `
        --fundamental-root $FundamentalRoot `
        --chip-root $ChipRoot `
        --max-shards 16
    if ($LASTEXITCODE -ne 0) {
        throw "stock-session holdout field sidecar build failed: $LASTEXITCODE"
    }

    & $PythonExe scripts\build_cn_phase3cm_forward_label_sidecars.py `
        --source-root $SessionFieldRoot `
        --evaluation-role holdout `
        --output-root $SessionLabelRoot `
        --split-manifest $SplitManifest `
        --split-manifest-hash $SplitManifestHash `
        --horizons 1,5,15,30 `
        --max-shards 16 `
        --polars-threads $PolarsThreads
    if ($LASTEXITCODE -ne 0) {
        throw "stock-session holdout label sidecar build failed: $LASTEXITCODE"
    }

    & $PythonExe scripts\run_cn_core_pack_fixed_holdout.py `
        --campaign-root $CampaignRoot `
        --freeze-manifest $FreezeManifest `
        --registry $Registry `
        --split-manifest $SplitManifest `
        --active-field-root $ActiveFieldRoot `
        --active-label-root $ActiveLabelRoot `
        --session-field-root $SessionFieldRoot `
        --session-label-root $SessionLabelRoot `
        --output-root $EvaluationRoot `
        --active-threads $ActiveThreads `
        --session-threads $SessionThreads
    if ($LASTEXITCODE -ne 0) {
        throw "fixed holdout evaluation failed: $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}
