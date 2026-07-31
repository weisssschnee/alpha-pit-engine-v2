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
    [string]$CandidateArchive = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_prep_20260730\' +
        'candidate_exact_archive_after_bounded_large.parquet'
    ),
    [string]$BehaviorArchive = (
        'D:\ChengboRemote\runtime\' +
        'cn_hybrid_bounded_large_tranche_20260729_1030_6288d71_6144\' +
        'behavior_archive.parquet'
    ),
    [string]$WinnerGuide = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_prep_20260730\' +
        'winner_structural_guide.json'
    ),
    [string]$HistoryManifest = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_prep_20260730\' +
        'winner_guided_identity_manifest.json'
    ),
    [string]$QualifiedPreflightRoot = '',
    [ValidateSet('SEARCH_EXCLUSIVE_32', 'SEARCH_DUAL_24')]
    [string]$NodeResourceProfile = 'SEARCH_EXCLUSIVE_32',
    [string]$NodeResourceCapacity = (
        'runtime\run_plans\cn_alpha_node_resource_profiles_v1.json'
    ),
    [string]$NodeResourceStateRoot = (
        'D:\ChengboRemote\runtime\node_resource_governor'
    ),
    [switch]$PreflightOnly
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$deploymentManifestRoot = 'D:\ChengboRemote\runtime\manifests'
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
$resolvedNodeCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
    [IO.Path]::GetFullPath($NodeResourceCapacity)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $NodeResourceCapacity))
}
$candidateArchive = [IO.Path]::GetFullPath($CandidateArchive)
$behaviorArchive = [IO.Path]::GetFullPath($BehaviorArchive)
$winnerGuide = [IO.Path]::GetFullPath($WinnerGuide)
$historyManifest = [IO.Path]::GetFullPath($HistoryManifest)
if (-not $resolvedRepo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $resolvedRepo"
}
if (-not $resolvedRoot.StartsWith(
    'D:\ChengboRemote\runtime\cn_winner_guided_large_search_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected output root: $resolvedRoot"
}
if (-not $resolvedAuthorization.StartsWith(
    $resolvedRepo + [IO.Path]::DirectorySeparatorChar,
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "campaign authorization must be inside exact repo"
}
if (-not (Test-Path -LiteralPath $python)) {
    throw "official Python missing: $python"
}

$authorization = Get-Content -LiteralPath $resolvedAuthorization -Raw |
    ConvertFrom-Json
$nodeCapacity = Get-Content -LiteralPath $resolvedNodeCapacity -Raw |
    ConvertFrom-Json
$nodeProfileProperty = $nodeCapacity.profiles.PSObject.Properties[
    $NodeResourceProfile
]
if ($null -eq $nodeProfileProperty) {
    throw "node resource profile missing: $NodeResourceProfile"
}
$nodeProfile = $nodeProfileProperty.Value
$computeThreads = [int]$nodeProfile.cpu_threads
if (
    [string]$nodeProfile.role -ne 'SEARCH' -or
    $computeThreads -lt 1 -or
    $computeThreads -gt 32
) {
    throw "invalid search node resource profile: $NodeResourceProfile"
}
if ($authorization.node_resource_profiles_allowed) {
    if (
        $NodeResourceProfile -notin @(
            $authorization.node_resource_profiles_allowed
        ) -or
        [string]$authorization.resource_profile_switch_boundary -ne
        'BATCH_CLOSED_IMMUTABLE_ONLY'
    ) {
        throw "campaign authorization/resource profile mismatch"
    }
} elseif (
    [int]$authorization.active_threads -ne $computeThreads -or
    [int]$authorization.session_threads -ne $computeThreads
) {
    throw "legacy campaign authorization/resource profile thread mismatch"
}
if ($authorization.campaign_profile -notin @(
    'cn_winner_guided_large_search_v1',
    'cn_winner_guided_continuation_search_v1'
)) {
    throw "campaign profile drift"
}
New-Item -ItemType Directory -Force -Path $resolvedRoot | Out-Null
$effectiveAuthorization = $resolvedAuthorization
if ($PreflightOnly) {
    $qualification = Get-Content -LiteralPath $resolvedAuthorization -Raw |
        ConvertFrom-Json
    $qualification.status = 'ZERO_FINANCIAL_PREFLIGHT_AUTHORIZED'
    $qualification.execution_authorized = $false
    $qualification.financial_campaign_authorized = $false
    $qualification.qualification_authorized = $true
    $effectiveAuthorization = Join-Path $resolvedRoot (
        'qualification_authorization.json'
    )
    $qualification | ConvertTo-Json -Depth 12 |
        Set-Content -LiteralPath $effectiveAuthorization -Encoding UTF8
} elseif (
    -not [bool]$authorization.execution_authorized -or
    -not [bool]$authorization.financial_campaign_authorized
) {
    throw "winner-guided execution authority missing"
}

if (-not $PreflightOnly) {
    if (-not $QualifiedPreflightRoot) {
        throw "qualified preflight root is required for execution"
    }
    $preflightRoot = [IO.Path]::GetFullPath($QualifiedPreflightRoot)
    $preflightSupply = Join-Path $preflightRoot (
        'fresh_exact_supply_preflight.json'
    )
    $preflightExit = Join-Path $preflightRoot (
        'campaign_process_exit.json'
    )
    if (
        -not (Test-Path -LiteralPath $preflightSupply) -or
        -not (Test-Path -LiteralPath $preflightExit)
    ) {
        throw "qualified preflight closure missing"
    }
    $supply = Get-Content -LiteralPath $preflightSupply -Raw |
        ConvertFrom-Json
    $exit = Get-Content -LiteralPath $preflightExit -Raw |
        ConvertFrom-Json
    if (
        $supply.status -ne 'PASS' -or
        [int]$exit.exit_code -ne 0 -or
        [int]$supply.financial_reads -ne 0 -or
        [int]$supply.validation_reads -ne 0 -or
        [int]$supply.holdout_reads -ne 0 -or
        [int]$supply.forward_2026_reads -ne 0
    ) {
        throw "qualified preflight evidence failed"
    }
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
                    $_.FullName
                }
            } catch {
                # Ignore unrelated historical deployment manifests.
            }
        }
)
if ($deploymentManifest.Count -ne 1) {
    throw "exact deployment manifest match count must be one"
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

$requiredHashes = [ordered]@{
    $candidateArchive = [string](
        $authorization.historical_candidate_archive_sha256
    )
    $behaviorArchive = [string](
        $authorization.historical_behavior_archive_sha256
    )
    $winnerGuide = [string](
        $authorization.winner_structural_guide_sha256
    )
    $historyManifest = [string](
        $authorization.historical_manifest_sha256
    )
}
foreach ($entry in $requiredHashes.GetEnumerator()) {
    if (-not (Test-Path -LiteralPath $entry.Key)) {
        throw "required frozen input missing: $($entry.Key)"
    }
    $actual = (
        Get-FileHash -LiteralPath $entry.Key -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    if ($actual -ne $entry.Value) {
        throw "frozen input hash drift: $($entry.Key):$actual"
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

[ordered]@{
    schema_version = 'cn_winner_guided_large_search_deployment_v1'
    repo_sha = $RepoSha
    repo = $resolvedRepo
    deployment_manifest_path = $deploymentManifest[0]
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $deploymentManifest[0] -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    campaign_authorization_path = $effectiveAuthorization
    output_root = $resolvedRoot
    host = $env:COMPUTERNAME
    free_memory_bytes_at_launch = $freeMemoryBytes
    campaign_profile = [string]$authorization.campaign_profile
    formal_asks_per_checkpoint = [int]$authorization.asks_per_checkpoint
    maximum_checkpoints = [int]$authorization.maximum_checkpoints
    maximum_formal_fresh_exact_asks = [int](
        $authorization.maximum_raw_asks
    )
    fixed_route_formal_asks_per_checkpoint = (
        $authorization.fixed_route_formal_asks_per_checkpoint
    )
    winner_guide_sha256 = $requiredHashes[$winnerGuide]
    cross_campaign_optimizer_state_reused = $false
    cross_campaign_reward_rows_imported = 0
    node_resource_profile = $NodeResourceProfile
    node_resource_capacity_path = $resolvedNodeCapacity
    node_resource_capacity_sha256 = (
        Get-FileHash -LiteralPath $resolvedNodeCapacity -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    active_threads = $computeThreads
    session_threads = $computeThreads
    active_pair_batch_size = 12
    session_pair_batch_size = 12
    evaluator_cache_cap_bytes = 8589934592
    minimum_free_memory_bytes = 25769803776
    validation = 'FORBIDDEN_DURING_AND_AFTER_TRANCHE'
    holdout = 'SEALED'
    forward_2026 = 'SEALED'
    preflight_only = [bool]$PreflightOnly
    launched_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 8 |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'deployment_binding.json'
    ) -Encoding UTF8

$env:PYTHONPATH = Join-Path $resolvedRepo 'src'
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:NUMBA_NUM_THREADS = [string]$computeThreads
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
    '--campaign-authorization', $effectiveAuthorization,
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
    '--winner-structural-guide', $winnerGuide,
    '--output-root', $resolvedRoot,
    '--seed-base', [string]$authorization.seed_base,
    '--active-threads', [string]$computeThreads,
    '--session-threads', [string]$computeThreads,
    '--maximum-wall-seconds', [string]$authorization.maximum_wall_seconds
)
if ($PreflightOnly) {
    $campaignArgs += '--preflight-only'
}
$stdoutPath = Join-Path $resolvedRoot 'campaign.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'campaign.stderr.log'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "search-$PID"
$leaseReceiptRoot = Join-Path $resolvedRoot 'resource_leases'
New-Item -ItemType Directory -Force -Path $leaseReceiptRoot | Out-Null
$leaseReceipt = Join-Path $leaseReceiptRoot "$leaseId.json"
& $python $leaseManager acquire `
    --state-root $NodeResourceStateRoot `
    --capacity-manifest $resolvedNodeCapacity `
    --profile $NodeResourceProfile `
    --lease-id $leaseId `
    --owner-pid $PID `
    --workload-id $resolvedRoot `
    --receipt $leaseReceipt *>> $stdoutPath
if ($LASTEXITCODE -ne 0) {
    throw "node resource lease admission failed: $LASTEXITCODE"
}
$env:CN_NODE_RESOURCE_LEASE_REQUIRED = '1'
$env:CN_NODE_RESOURCE_LEASE_RECEIPT = $leaseReceipt
$env:CN_NODE_CPU_ENTITLEMENT = [string]$computeThreads
$process = $null
$processExitCode = 1
try {
    $process = Start-Process -FilePath $python -ArgumentList $campaignArgs `
        -WorkingDirectory $resolvedRepo -Wait -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
    $processExitCode = $process.ExitCode
} finally {
    & $python $leaseManager release `
        --state-root $NodeResourceStateRoot `
        --lease-id $leaseId `
        --owner-pid $PID *>> $stdoutPath
}
[ordered]@{
    repo_sha = $RepoSha
    exit_code = $processExitCode
    completed_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'campaign_process_exit.json'
    ) -Encoding UTF8
exit $processExitCode
