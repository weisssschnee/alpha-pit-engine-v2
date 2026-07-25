param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$inputRoot = 'D:\ChengboRemote\runtime\cn_slow_cross_sectional_search_inputs_20260726_678bf8f'
$trainBase = 'D:\ChengboRemote\runtime\cn_core_pack_aggressive_discovery_20260718_595c5fc\strict_wave_01024_sidecars_3509d0c'
$labelBase = 'D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git\runtime\cn_phase3cm_streaming_repair_20260716'
$validationBase = 'D:\ChengboRemote\runtime\cn_core_pack_large_development_20260722_9f3a5f2_30t_r5\validation_sidecars_candidate_bound_2dff602'
$split = 'D:\ChengboRemote\workspace\cn_phase3cm_1024_sidecar_closure_0aba8c5\runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv'

$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
if (-not $resolvedRepo.StartsWith('D:\ChengboRemote\workspace\', [StringComparison]::OrdinalIgnoreCase)) {
    throw "unexpected repo path: $resolvedRepo"
}
if (-not $resolvedRoot.StartsWith('D:\ChengboRemote\runtime\cn_slow_cross_sectional_evaluated384_', [StringComparison]::OrdinalIgnoreCase)) {
    throw "unexpected output root: $resolvedRoot"
}
if (-not (Test-Path -LiteralPath $python)) {
    throw "official Python missing: $python"
}
$actualSha = (& git -C $resolvedRepo rev-parse HEAD).Trim().ToLowerInvariant()
if ($actualSha -ne $RepoSha.ToLowerInvariant()) {
    throw "workspace SHA mismatch: expected=$RepoSha actual=$actualSha"
}
$matching = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and (
                $_.CommandLine -like "*$resolvedRoot*" -or
                $_.CommandLine -match 'cn-targeted-search-medium-campaign'
            )
        }
)
if ($matching) {
    throw "duplicate writer/search process detected: $($matching.ProcessId -join ',')"
}
$memory = Get-CimInstance Win32_OperatingSystem
$freeMemoryBytes = [int64]$memory.FreePhysicalMemory * 1024
if ($freeMemoryBytes -lt 24GB) {
    throw "minimum free memory gate failed: $freeMemoryBytes"
}
foreach ($path in @(
    (Join-Path $inputRoot 'manifest.json'),
    (Join-Path $inputRoot 'candidate_exact_archive.parquet'),
    (Join-Path $inputRoot 'behavior_archive.parquet'),
    (Join-Path $trainBase 'CN_PHASE3CM_1024_SIDECAR_CLOSURE.json'),
    (Join-Path $trainBase 'session_time_major_train_v1'),
    (Join-Path $labelBase 'session_time_major_train_v3_labels'),
    (Join-Path $validationBase 'validation_sidecar_closure.json'),
    (Join-Path $validationBase 'session_fields'),
    (Join-Path $validationBase 'session_labels'),
    $split
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required input missing: $path"
    }
}

New-Item -ItemType Directory -Force -Path $resolvedRoot | Out-Null
[ordered]@{
    schema_version = 'cn_slow_cross_sectional_evaluated384_deployment_binding_v1'
    repo_sha = $RepoSha
    repo = $resolvedRepo
    output_root = $resolvedRoot
    host = $env:COMPUTERNAME
    python = $python
    python_version = (& $python -c 'import platform; print(platform.python_version())').Trim()
    free_memory_bytes_at_launch = $freeMemoryBytes
    logical_cpu_count = (Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
    target_route = 'SLOW_CROSS_SECTIONAL_LEVEL'
    budget_counting_unit = 'PAIR_EVALUATED'
    minimum_actual_evaluated_pairs = 384
    stock_session_compute_threads = 30
    stock_session_pair_batch_size = 8
    evaluator_cache_cap_bytes = 8589934592
    portfolio_mode = 'LONG_ONLY_TOP'
    one_way_cost_bps = 5
    horizons_minutes = @(1, 5, 15, 30)
    validation_mode = 'AUTOMATIC_POST_TRAIN_REPORT_ONLY'
    holdout = 'SEALED'
    forward_2026 = 'SEALED'
    launched_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 5 |
    Set-Content -LiteralPath (Join-Path $resolvedRoot 'deployment_binding.json') -Encoding UTF8

$env:PYTHONPATH = Join-Path $resolvedRepo 'src'
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:NUMBA_NUM_THREADS = '30'
$env:ARROW_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:POLARS_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'

$campaignArgs = @(
    (Join-Path $resolvedRepo 'app.py'),
    'cn-targeted-search-medium-campaign', '--',
    '--campaign-profile', 'slow_cross_sectional_evaluated384',
    '--campaign-authorization', (Join-Path $resolvedRepo 'runtime\run_plans\cn_slow_cross_sectional_evaluated384_v1_authorization.json'),
    '--registry', (Join-Path $resolvedRepo 'runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json'),
    '--discovery-contract', (Join-Path $resolvedRepo 'runtime\run_plans\cn_core_pack_development_discovery_v1.json'),
    '--discovery-authorization', (Join-Path $resolvedRepo 'runtime\run_plans\cn_core_pack_development_discovery_v1_authorization.json'),
    '--split-manifest', $split,
    '--sidecar-closure', (Join-Path $trainBase 'CN_PHASE3CM_1024_SIDECAR_CLOSURE.json'),
    '--active-field-root', (Join-Path $trainBase 'active_time_major_train_v1'),
    '--active-label-root', (Join-Path $labelBase 'time_major_train_v3_labels'),
    '--session-field-root', (Join-Path $trainBase 'session_time_major_train_v1'),
    '--session-label-root', (Join-Path $labelBase 'session_time_major_train_v3_labels'),
    '--validation-sidecar-closure', (Join-Path $validationBase 'validation_sidecar_closure.json'),
    '--validation-active-field-root', (Join-Path $validationBase 'active_fields'),
    '--validation-active-label-root', (Join-Path $validationBase 'active_labels'),
    '--validation-session-field-root', (Join-Path $validationBase 'session_fields'),
    '--validation-session-label-root', (Join-Path $validationBase 'session_labels'),
    '--historical-candidate-archive', (Join-Path $inputRoot 'candidate_exact_archive.parquet'),
    '--historical-behavior-archive', (Join-Path $inputRoot 'behavior_archive.parquet'),
    '--historical-archive-manifest', (Join-Path $inputRoot 'manifest.json'),
    '--output-root', $resolvedRoot,
    '--seed-base', '2026072601',
    '--active-threads', '30',
    '--session-threads', '30'
)
$stdoutPath = Join-Path $resolvedRoot 'campaign.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'campaign.stderr.log'
$process = Start-Process -FilePath $python -ArgumentList $campaignArgs `
    -WorkingDirectory $resolvedRepo -Wait -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath
[ordered]@{
    repo_sha = $RepoSha
    exit_code = $process.ExitCode
    completed_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json |
    Set-Content -LiteralPath (Join-Path $resolvedRoot 'campaign_process_exit.json') -Encoding UTF8
exit $process.ExitCode
