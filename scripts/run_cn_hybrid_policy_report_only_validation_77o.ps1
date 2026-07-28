param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$CampaignRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$deploymentManifestRoot = 'D:\ChengboRemote\runtime\manifests'
$minuteSourceRoot = 'D:\ChengboRemote\data\phase3dz_true1min_sidecar_augmented_full16_20260702'
$fundamentalRoot = 'D:\ChengboRemote\data\cn_fundamental_akshare_fullA_partitioned_pit_v1_20260603'
$chipRoot = 'D:\ChengboRemote\data\chip_pit_v1_20260713'
$split = 'D:\ChengboRemote\workspace\cn_phase3cm_1024_sidecar_closure_0aba8c5\runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv'
$splitHash = 'fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241'

$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedCampaign = [IO.Path]::GetFullPath($CampaignRoot)
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
if (-not $resolvedRepo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $resolvedRepo"
}
if ($resolvedCampaign -ne
    'D:\ChengboRemote\runtime\cn_hybrid_search_productivity_medium_20260728_1536_07598a5'
) {
    throw "unexpected source campaign root: $resolvedCampaign"
}
if (-not $resolvedRoot.StartsWith(
    "$resolvedCampaign\report_only_validation_256_",
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected validation output root: $resolvedRoot"
}
if (-not (Test-Path -LiteralPath $python)) {
    throw "official Python missing: $python"
}
$deploymentManifest = @(
    Get-ChildItem -LiteralPath $deploymentManifestRoot -Filter '*.json' -File |
        ForEach-Object {
            try {
                $payload = Get-Content -LiteralPath $_.FullName -Raw |
                    ConvertFrom-Json
                if (
                    $payload.head -and
                    $payload.workspace -and
                    $payload.head.ToString().ToLowerInvariant() -eq
                        $RepoSha.ToLowerInvariant() -and
                    [IO.Path]::GetFullPath($payload.workspace.ToString()) -eq
                        $resolvedRepo
                ) {
                    [pscustomobject]@{
                        path = $_.FullName
                        sha256 = (
                            Get-FileHash -LiteralPath $_.FullName `
                                -Algorithm SHA256
                        ).Hash.ToLowerInvariant()
                    }
                }
            } catch {
                # Ignore unrelated historical deployment manifests.
            }
        }
)
if ($deploymentManifest.Count -ne 1) {
    throw "exact deployment manifest match count must be one: $($deploymentManifest.Count)"
}
$matching = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.ProcessId -ne $PID -and
            $_.CommandLine -and (
                $_.CommandLine -like "*$resolvedRoot*" -or
                $_.CommandLine -match
                    'run_cn_hybrid_policy_report_only_validation'
            )
        }
)
if ($matching) {
    throw "duplicate validation process detected: $($matching.ProcessId -join ',')"
}
$memory = Get-CimInstance Win32_OperatingSystem
$freeMemoryBytes = [int64]$memory.FreePhysicalMemory * 1024
if ($freeMemoryBytes -lt 24GB) {
    throw "minimum free memory gate failed: $freeMemoryBytes"
}
foreach ($path in @(
    (Join-Path $resolvedCampaign 'run_manifest.json'),
    (Join-Path $resolvedCampaign 'train_complete_manifest.json'),
    $minuteSourceRoot,
    $fundamentalRoot,
    $chipRoot,
    $split
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required validation input missing: $path"
    }
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "validation output root already exists: $resolvedRoot"
}

New-Item -ItemType Directory -Force -Path $resolvedRoot | Out-Null
$preparedRoot = Join-Path $resolvedRoot 'prepared'
$sidecarRoot = Join-Path $resolvedRoot 'sidecars'
$activeFieldRoot = Join-Path $sidecarRoot 'active_fields'
$activeLabelRoot = Join-Path $sidecarRoot 'active_labels'
$sessionFieldRoot = Join-Path $sidecarRoot 'session_fields'
$sessionLabelRoot = Join-Path $sidecarRoot 'session_labels'
$evaluationRoot = Join-Path $resolvedRoot 'evaluation'
$authorization = Join-Path $resolvedRepo (
    'runtime\run_plans\cn_hybrid_policy_report_only_validation_20260729.json'
)
$registry = Join-Path $resolvedRepo (
    'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
    'unified_capability_registry.json'
)

[ordered]@{
    schema_version = 'cn_hybrid_policy_report_only_validation_deployment_v1'
    repo_sha = $RepoSha
    repo = $resolvedRepo
    deployment_manifest_path = $deploymentManifest[0].path
    deployment_manifest_sha256 = $deploymentManifest[0].sha256
    source_campaign_root = $resolvedCampaign
    output_root = $resolvedRoot
    host = $env:COMPUTERNAME
    python = $python
    python_version = (
        & $python -c 'import platform; print(platform.python_version())'
    ).Trim()
    free_memory_bytes_at_launch = $freeMemoryBytes
    logical_cpu_count = (
        Get-CimInstance Win32_ComputerSystem
    ).NumberOfLogicalProcessors
    accepted_development_policy = 'HYBRID_TPE_AVAILABILITY'
    policy_selection_reopened = $false
    selected_pairs = 256
    selected_pairs_by_arm = [ordered]@{
        HYBRID_TPE_AVAILABILITY = 128
        AVAILABILITY_AWARE_UNIFORM = 128
    }
    evaluation_role = 'validation'
    usage = 'report_only'
    validation_feedback = 'FORBIDDEN'
    scheduler_write = 'FORBIDDEN'
    archive_write = 'FORBIDDEN'
    promotion = 'FORBIDDEN'
    holdout = 'SEALED'
    forward_2026 = 'SEALED'
    launched_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 5 |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'deployment_binding.json'
    ) -Encoding UTF8

$env:PYTHONPATH = Join-Path $resolvedRepo 'src'
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:NUMBA_NUM_THREADS = '32'
$env:ARROW_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:POLARS_MAX_THREADS = '24'
$env:JOBLIB_MULTIPROCESSING = '0'

$runner = Join-Path $resolvedRepo (
    'scripts\run_cn_hybrid_policy_report_only_validation.py'
)
$stdoutPath = Join-Path $resolvedRoot 'validation.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'validation.stderr.log'

try {
    & $python $runner prepare `
        --campaign-root $resolvedCampaign `
        --authorization $authorization `
        --output-root $preparedRoot *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "validation cohort preparation failed: $LASTEXITCODE"
    }

    & $python (Join-Path $resolvedRepo (
        'scripts\build_cn_phase3cm_time_major_sidecar.py'
    )) `
        --source-root $minuteSourceRoot `
        --evaluation-role validation `
        --output-root $activeFieldRoot `
        --candidate-table (
            Join-Path $preparedRoot 'validation_active_bar_candidates.csv'
        ) `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --max-shards 16 `
        --polars-threads 24 `
        --parity *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "active-bar validation field sidecar build failed: $LASTEXITCODE"
    }

    & $python (Join-Path $resolvedRepo (
        'scripts\build_cn_phase3cm_forward_label_sidecars.py'
    )) `
        --source-root $activeFieldRoot `
        --evaluation-role validation `
        --output-root $activeLabelRoot `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --horizons 1,5,15,30 `
        --max-shards 16 `
        --polars-threads 24 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "active-bar validation label sidecar build failed: $LASTEXITCODE"
    }

    & $python (Join-Path $resolvedRepo (
        'scripts\build_cn_core_pack_validation_session_sidecar.py'
    )) `
        --source-root $minuteSourceRoot `
        --evaluation-role validation `
        --output-root $sessionFieldRoot `
        --candidate-table (
            Join-Path $preparedRoot 'validation_stock_session_candidates.csv'
        ) `
        --registry $registry `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --fundamental-root $fundamentalRoot `
        --chip-root $chipRoot `
        --max-shards 16 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "stock-session validation field sidecar build failed: $LASTEXITCODE"
    }

    & $python (Join-Path $resolvedRepo (
        'scripts\build_cn_phase3cm_forward_label_sidecars.py'
    )) `
        --source-root $sessionFieldRoot `
        --evaluation-role validation `
        --output-root $sessionLabelRoot `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --horizons 1,5,15,30 `
        --max-shards 16 `
        --polars-threads 24 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "stock-session validation label sidecar build failed: $LASTEXITCODE"
    }

    $env:POLARS_MAX_THREADS = '1'
    & $python $runner execute `
        --campaign-root $resolvedCampaign `
        --authorization $authorization `
        --freeze-manifest (
            Join-Path $preparedRoot 'validation_candidate_freeze.json'
        ) `
        --registry $registry `
        --split-manifest $split `
        --active-field-root $activeFieldRoot `
        --active-label-root $activeLabelRoot `
        --session-field-root $sessionFieldRoot `
        --session-label-root $sessionLabelRoot `
        --output-root $evaluationRoot `
        --active-threads 32 `
        --session-threads 32 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "report-only validation failed: $LASTEXITCODE"
    }
} catch {
    $_ | Out-String | Add-Content -LiteralPath $stderrPath -Encoding UTF8
    [ordered]@{
        repo_sha = $RepoSha
        status = 'FAILED'
        completed_at = (Get-Date).ToUniversalTime().ToString('o')
        error = $_.Exception.Message
    } | ConvertTo-Json |
        Set-Content -LiteralPath (
            Join-Path $resolvedRoot 'validation_process_exit.json'
        ) -Encoding UTF8
    throw
}

[ordered]@{
    repo_sha = $RepoSha
    status = 'COMPLETED'
    exit_code = 0
    completed_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'validation_process_exit.json'
    ) -Encoding UTF8
