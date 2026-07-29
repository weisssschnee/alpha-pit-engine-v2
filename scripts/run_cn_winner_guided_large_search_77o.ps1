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
    [string]$QualifiedPreflightRoot = '',
    [switch]$PreflightOnly
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$deploymentManifestRoot = 'D:\ChengboRemote\runtime\manifests'
$prepRoot = (
    'D:\ChengboRemote\runtime\' +
    'cn_winner_guided_large_search_prep_20260730'
)
$candidateArchive = Join-Path $prepRoot (
    'candidate_exact_archive_after_bounded_large.parquet'
)
$winnerGuide = Join-Path $prepRoot 'winner_structural_guide.json'
$historyManifest = Join-Path $prepRoot (
    'winner_guided_identity_manifest.json'
)
$completedCampaignRoot = (
    'D:\ChengboRemote\runtime\' +
    'cn_hybrid_bounded_large_tranche_20260729_1030_6288d71_6144'
)
$behaviorArchive = Join-Path $completedCampaignRoot (
    'behavior_archive.parquet'
)
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
if (
    $authorization.campaign_profile -ne
        'cn_winner_guided_large_search_v1'
) {
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
    $candidateArchive = (
        '5a217edd05ff504755753b6dc593bbf7171d03e57995a630355782b75a19c70d'
    )
    $behaviorArchive = (
        '6783727d8877bdacf8c6b443c5af77b61a55a179d09aae65f86a22fe3b59ced1'
    )
    $winnerGuide = (
        '889bf9ecd092ddf3c0d712d4c13d61615a8649c312b24e4fc8ad76883e9e8c51'
    )
    $historyManifest = (
        '429bc4459d3fff0e4cf2ade73bea77105ac27822aa6d543c857e6af4c1a55a24'
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
    campaign_profile = 'cn_winner_guided_large_search_v1'
    formal_asks_per_checkpoint = 1536
    maximum_checkpoints = 8
    maximum_formal_fresh_exact_asks = 12288
    fixed_route_formal_asks_per_checkpoint = [ordered]@{
        SLOW_TEMPORAL_CHANGE = 1520
        FIRSTN_PATH = 12
        SLOW_CROSS_SECTIONAL_LEVEL = 4
        MARKET_REGIME_CONDITION = 0
        DISCLOSURE_EVENT = 0
    }
    winner_guide_sha256 = $requiredHashes[$winnerGuide]
    cross_campaign_optimizer_state_reused = $false
    cross_campaign_reward_rows_imported = 0
    active_threads = 32
    session_threads = 32
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
    '--seed-base', '2026073001',
    '--active-threads', '32',
    '--session-threads', '32',
    '--maximum-wall-seconds', '129600'
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
