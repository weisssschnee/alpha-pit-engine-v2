param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [Parameter(Mandatory = $true)]
    [string]$CampaignAuthorization,
    [switch]$PreflightOnly
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$deploymentManifestRoot = 'D:\ChengboRemote\runtime\manifests'
$prepRoot = 'D:\ChengboRemote\runtime\cn_hybrid_large_search_prep_20260729'
$p07Root = Join-Path $prepRoot 'p07_bounded_large_tranche'
$completedTrancheRoot = (
    'D:\ChengboRemote\runtime\' +
    'cn_hybrid_only_tranche_20260729_0445_9e9e113_3072'
)
$candidateArchive = Join-Path $p07Root (
    'candidate_exact_archive_after_hybrid_tranche.parquet'
)
$behaviorArchive = Join-Path $completedTrancheRoot 'behavior_archive.parquet'
$historyManifestRelative = (
    'runtime\run_plans\' +
    'cn_hybrid_bounded_large_tranche_v1_identity_manifest.json'
)
$contractRelative = (
    'runtime\run_plans\' +
    'cn_hybrid_bounded_large_tranche_v1_contract.json'
)
$p07Manifest = Join-Path $p07Root 'p07_manifest.json'
$trainBase = (
    'D:\ChengboRemote\runtime\' +
    'cn_core_pack_aggressive_discovery_20260718_595c5fc\' +
    'strict_wave_01024_sidecars_3509d0c'
)
$labelBase = (
    'D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git\' +
    'runtime\cn_phase3cm_streaming_repair_20260716'
)
$validationBase = (
    'D:\ChengboRemote\runtime\' +
    'cn_core_pack_large_development_20260722_9f3a5f2_30t_r5\' +
    'validation_sidecars_candidate_bound_2dff602'
)
$slowRoot = (
    'D:\ChengboRemote\runtime\' +
    'cn_slow_cross_sectional_evaluated384_20260726_351e503_384e'
)
$validationSessionFields = Join-Path $slowRoot (
    'validation_sidecars_slow_cross_d3de4b9\session_fields'
)
$validationClosure = Join-Path $slowRoot (
    'post_train_validation_repaired_d3de4b9\' +
    'validation_repair_closure.json'
)
$split = (
    'D:\ChengboRemote\workspace\' +
    'cn_phase3cm_1024_sidecar_closure_0aba8c5\' +
    'runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv'
)

$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
$resolvedAuthorization = [IO.Path]::GetFullPath($CampaignAuthorization)
if (-not $resolvedRepo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $resolvedRepo"
}
if (-not $resolvedRoot.StartsWith(
    'D:\ChengboRemote\runtime\cn_hybrid_bounded_large_tranche_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected output root: $resolvedRoot"
}
if (-not $resolvedAuthorization.StartsWith(
    $resolvedRepo + [IO.Path]::DirectorySeparatorChar,
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "campaign authorization must be inside exact repo: $resolvedAuthorization"
}
if (-not (Test-Path -LiteralPath $python)) {
    throw "official Python missing: $python"
}
if (-not (Test-Path -LiteralPath $resolvedAuthorization)) {
    throw "campaign authorization missing: $resolvedAuthorization"
}

$authorization = Get-Content -LiteralPath $resolvedAuthorization -Raw |
    ConvertFrom-Json
if ($authorization.campaign_profile -ne 'cn_hybrid_bounded_large_tranche_v1') {
    throw "campaign profile drift in authorization"
}
if ($PreflightOnly) {
    if (
        -not [bool]$authorization.qualification_authorized -or
        [bool]$authorization.execution_authorized
    ) {
        throw "preflight requires qualification-only authority"
    }
} elseif (
    -not [bool]$authorization.execution_authorized -or
    -not [bool]$authorization.financial_campaign_authorized
) {
    throw "financial tranche requires separate execution authority"
}

$optunaVersion = (
    & $python -c 'import optuna; print(optuna.__version__)'
).Trim()
if ($optunaVersion -ne '4.8.0') {
    throw "official Optuna 4.8.0 missing: $optunaVersion"
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
    throw (
        'exact deployment manifest match count must be one: ' +
        $deploymentManifest.Count
    )
}

$matching = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.ProcessId -ne $PID -and
            $_.Name -match '^python(w)?\.exe$' -and
            $_.CommandLine -and (
                $_.CommandLine -like "*$resolvedRoot*" -or
                $_.CommandLine -match 'cn-large-tpe-search-campaign'
            )
        }
)
if ($matching) {
    throw (
        'duplicate financial writer/search process detected: ' +
        ($matching.ProcessId -join ',')
    )
}
$memory = Get-CimInstance Win32_OperatingSystem
$freeMemoryBytes = [int64]$memory.FreePhysicalMemory * 1024
if ($freeMemoryBytes -lt 24GB) {
    throw "minimum free memory gate failed: $freeMemoryBytes"
}

$historyManifest = Join-Path $resolvedRepo $historyManifestRelative
$contract = Join-Path $resolvedRepo $contractRelative
$requiredHashes = [ordered]@{
    $candidateArchive = (
        '450491d7ceff74dfa8eb5e019b4dd15c982a00f6f990299e7afd620dc87b1ae6'
    )
    $behaviorArchive = (
        '0d51f2ea0ace7b06cf5c02a7ac970a629490c284c442f6dd693a1deb007dfc64'
    )
    $historyManifest = (
        '7de34a718dea8251e3f15852c98b2e38b732cd67ab2bc2ce38222c937aaf6133'
    )
    $contract = (
        'ba45d49fce0fd12e3e8e4fd216f36ec3b7b8a9e2a70e920d6493f48bf470770e'
    )
    $p07Manifest = (
        '436a4062b4a8216f8ad1a1a651dcc2c2fb71f4aa36713dad71f3ae507983a9d2'
    )
}
foreach ($entry in $requiredHashes.GetEnumerator()) {
    if (-not (Test-Path -LiteralPath $entry.Key)) {
        throw "required frozen input missing: $($entry.Key)"
    }
    $actualHash = (
        Get-FileHash -LiteralPath $entry.Key -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    if ($actualHash -ne $entry.Value) {
        throw "frozen input hash drift: $($entry.Key):$actualHash"
    }
}

$requiredPaths = @(
    (Join-Path $trainBase 'CN_PHASE3CM_1024_SIDECAR_CLOSURE.json'),
    (Join-Path $trainBase 'active_time_major_train_v1'),
    (Join-Path $trainBase 'session_time_major_train_v1'),
    (Join-Path $labelBase 'time_major_train_v3_labels'),
    (Join-Path $labelBase 'session_time_major_train_v3_labels'),
    (Join-Path $validationBase 'active_fields'),
    (Join-Path $validationBase 'active_labels'),
    (Join-Path $validationBase 'session_labels'),
    $validationSessionFields,
    $validationClosure,
    $split
)
foreach ($path in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required input missing: $path"
    }
}

New-Item -ItemType Directory -Force -Path $resolvedRoot | Out-Null
[ordered]@{
    schema_version = 'cn_hybrid_bounded_large_tranche_deployment_v1'
    repo_sha = $RepoSha
    repo = $resolvedRepo
    deployment_manifest_path = $deploymentManifest[0].path
    deployment_manifest_sha256 = $deploymentManifest[0].sha256
    campaign_authorization_path = $resolvedAuthorization
    campaign_authorization_sha256 = (
        Get-FileHash -LiteralPath $resolvedAuthorization -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    output_root = $resolvedRoot
    host = $env:COMPUTERNAME
    python = $python
    python_version = (
        & $python -c 'import platform; print(platform.python_version())'
    ).Trim()
    optuna_version = $optunaVersion
    free_memory_bytes_at_launch = $freeMemoryBytes
    logical_cpu_count = (
        Get-CimInstance Win32_ComputerSystem
    ).NumberOfLogicalProcessors
    campaign_profile = 'cn_hybrid_bounded_large_tranche_v1'
    formal_asks_per_checkpoint = 768
    maximum_checkpoints = 8
    maximum_formal_fresh_exact_asks = 6144
    fixed_route_formal_asks_per_checkpoint = [ordered]@{
        SLOW_TEMPORAL_CHANGE = 672
        FIRSTN_PATH = 8
        SLOW_CROSS_SECTIONAL_LEVEL = 24
        MARKET_REGIME_CONDITION = 16
        DISCLOSURE_EVENT = 48
    }
    policy_arm_ratio = [ordered]@{
        HYBRID_TPE_AVAILABILITY = 1.0
    }
    uniform_arm = 'FORBIDDEN'
    within_tranche_route_adaptation = 'FORBIDDEN'
    active_threads = 32
    session_threads = 32
    active_pair_batch_size = 12
    session_pair_batch_size = 12
    evaluator_cache_cap_bytes = 8589934592
    minimum_free_memory_bytes = 25769803776
    validation = 'FORBIDDEN_DURING_AND_AFTER_TRANCHE'
    holdout = 'SEALED'
    forward_2026 = 'SEALED'
    unlimited_or_20k_search_authorized = $false
    productive_family_diagnostics = [ordered]@{
        reporting_role = 'DIAGNOSTIC_ONLY_NOT_RUNTIME_STOP_GATE'
        productive_behavior_family_unique = $true
        productive_exact_per_behavior_family = $true
        top_10_productive_family_concentration = $true
        new_productive_behavior_families_by_checkpoint = $true
        productive_yield_by_checkpoint = $true
    }
    preflight_only = [bool]$PreflightOnly
    launched_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 6 |
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
$env:POLARS_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'

$campaignArgs = @(
    (Join-Path $resolvedRepo 'app.py'),
    'cn-large-tpe-search-campaign', '--',
    '--campaign-authorization', $resolvedAuthorization,
    '--registry',
    (Join-Path $resolvedRepo (
        'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
        'unified_capability_registry.json'
    )),
    '--discovery-contract',
    (Join-Path $resolvedRepo (
        'runtime\run_plans\cn_core_pack_development_discovery_v1.json'
    )),
    '--discovery-authorization',
    (Join-Path $resolvedRepo (
        'runtime\run_plans\' +
        'cn_core_pack_development_discovery_v1_authorization.json'
    )),
    '--split-manifest', $split,
    '--sidecar-closure',
    (Join-Path $trainBase 'CN_PHASE3CM_1024_SIDECAR_CLOSURE.json'),
    '--active-field-root',
    (Join-Path $trainBase 'active_time_major_train_v1'),
    '--active-label-root',
    (Join-Path $labelBase 'time_major_train_v3_labels'),
    '--session-field-root',
    (Join-Path $trainBase 'session_time_major_train_v1'),
    '--session-label-root',
    (Join-Path $labelBase 'session_time_major_train_v3_labels'),
    '--validation-sidecar-closure', $validationClosure,
    '--validation-active-field-root',
    (Join-Path $validationBase 'active_fields'),
    '--validation-active-label-root',
    (Join-Path $validationBase 'active_labels'),
    '--validation-session-field-root', $validationSessionFields,
    '--validation-session-label-root',
    (Join-Path $validationBase 'session_labels'),
    '--historical-candidate-archive', $candidateArchive,
    '--historical-behavior-archive', $behaviorArchive,
    '--historical-archive-manifest', $historyManifest,
    '--output-root', $resolvedRoot,
    '--seed-base', '2026072903',
    '--active-threads', '32',
    '--session-threads', '32',
    '--maximum-wall-seconds', '86400'
)
if ($PreflightOnly) {
    $campaignArgs += '--preflight-only'
}
$stdoutPath = Join-Path $resolvedRoot 'campaign.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'campaign.stderr.log'
$process = Start-Process -FilePath $python -ArgumentList $campaignArgs `
    -WorkingDirectory $resolvedRepo -Wait -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutPath `
    -RedirectStandardError $stderrPath
[ordered]@{
    repo_sha = $RepoSha
    exit_code = $process.ExitCode
    completed_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'campaign_process_exit.json'
    ) -Encoding UTF8
exit $process.ExitCode
