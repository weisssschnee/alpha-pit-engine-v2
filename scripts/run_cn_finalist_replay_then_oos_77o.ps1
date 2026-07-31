param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$DeploymentManifest,
    [Parameter(Mandatory = $true)]
    [string]$CohortRoot,
    [Parameter(Mandatory = $true)]
    [string]$AuthorityRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [ValidateRange(1, 256)]
    [int]$ExpectedPairCount = 24,
    [ValidateRange(1, 24)]
    [int]$ValidationThreads = 8,
    [ValidateSet('VALIDATION_DUAL_8')]
    [string]$NodeResourceProfile = 'VALIDATION_DUAL_8',
    [string]$NodeResourceCapacity = (
        'runtime\run_plans\cn_alpha_node_resource_profiles_v1.json'
    ),
    [string]$NodeResourceStateRoot = (
        'D:\ChengboRemote\runtime\node_resource_governor'
    )
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw "this runner is authorized only on DESKTOP-77OPJ6F"
}

$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = (Resolve-Path -LiteralPath $RepoRoot).Path
$resolvedCohort = (Resolve-Path -LiteralPath $CohortRoot).Path
$resolvedAuthority = (Resolve-Path -LiteralPath $AuthorityRoot).Path
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedRoot = [System.IO.Path]::GetFullPath($OutputRoot)
$resolvedNodeCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
    [IO.Path]::GetFullPath($NodeResourceCapacity)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $NodeResourceCapacity))
}

if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "remote Python is missing: $python"
}
if (-not (Test-Path -LiteralPath $git -PathType Leaf)) {
    throw "portable Git is missing: $git"
}
if (Test-Path -LiteralPath $resolvedRoot -PathType Leaf) {
    throw "output root is a file: $resolvedRoot"
}
New-Item -ItemType Directory -Path $resolvedRoot -Force | Out-Null

$gitSafePath = $resolvedRepo.Replace('\', '/')
$gitSafeDirectory = "safe.directory=$gitSafePath"
$observedSha = (
    & $git -c $gitSafeDirectory -C $resolvedRepo rev-parse HEAD
).Trim()
if ($LASTEXITCODE -ne 0 -or $observedSha -ne $RepoSha) {
    throw "deployed repo SHA drift: expected=$RepoSha observed=$observedSha"
}
$dirty = @(
    & $git -c $gitSafeDirectory -C $resolvedRepo status --porcelain
)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw "deployed workspace must be clean"
}
$deployment = Get-Content -LiteralPath $resolvedDeployment -Raw |
    ConvertFrom-Json
if ([string]$deployment.repo_sha -ne $RepoSha) {
    throw "deployment manifest repo SHA drift"
}
if ([string]$deployment.workspace -ne $resolvedRepo) {
    throw "deployment manifest workspace drift"
}
$nodeCapacity = Get-Content -LiteralPath $resolvedNodeCapacity -Raw |
    ConvertFrom-Json
$nodeProfileProperty = $nodeCapacity.profiles.PSObject.Properties[
    $NodeResourceProfile
]
if ($null -eq $nodeProfileProperty) {
    throw "node resource profile missing: $NodeResourceProfile"
}
$nodeProfile = $nodeProfileProperty.Value
if (
    [string]$nodeProfile.role -ne 'VALIDATION' -or
    [int]$nodeProfile.cpu_threads -ne $ValidationThreads
) {
    throw "validation node resource profile/thread mismatch"
}

$memory = Get-CimInstance Win32_OperatingSystem
$freeBytes = [int64]$memory.FreePhysicalMemory * 1024
$memoryGate = [int64]24 * 1024 * 1024 * 1024
if ($freeBytes -lt $memoryGate) {
    throw "free memory below 24 GiB gate: $freeBytes"
}

$split = (
    'D:\ChengboRemote\workspace\cn_phase3cm_1024_sidecar_closure_0aba8c5\' +
    'runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv'
)
$splitHash = (
    Get-FileHash -LiteralPath $split -Algorithm SHA256
).Hash.ToLowerInvariant()
if (
    $splitHash -ne
    'fab9fb17642595456e10c4ad44357193f2dcdc1d39edd785b8298fbe9ca22241'
) {
    throw "fixed split manifest hash drift"
}
$registry = Join-Path $resolvedRepo (
    'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
    'unified_capability_registry.json'
)
$trainMinuteRoot = (
    'D:\ChengboRemote\data\' +
    'cn_true1min_development_only_release_v1_20260712_77o'
)
$validationMinuteRoot = (
    'D:\ChengboRemote\data\' +
    'phase3dz_true1min_sidecar_augmented_full16_20260702'
)
$fundamentalRoot = (
    'D:\ChengboRemote\data\' +
    'cn_fundamental_akshare_fullA_partitioned_pit_v1_20260603'
)
$chipRoot = 'D:\ChengboRemote\data\chip_pit_v1_20260713'
foreach ($path in @(
    $split,
    $registry,
    $trainMinuteRoot,
    $validationMinuteRoot,
    $fundamentalRoot,
    $chipRoot
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required input is missing: $path"
    }
}

$preparedRoot = Join-Path $resolvedRoot 'prepared'
$trainFieldRoot = Join-Path $resolvedRoot 'sidecars\train_session_fields'
$replayRoot = Join-Path $resolvedRoot 'replay'
$validationFieldRoot = (
    Join-Path $resolvedRoot 'sidecars\validation_session_fields'
)
$validationLabelRoot = (
    Join-Path $resolvedRoot 'sidecars\validation_session_labels'
)
$oosRoot = Join-Path $resolvedRoot 'oos'
$runner = Join-Path $resolvedRepo (
    'scripts\run_cn_finalist_replay_then_oos.py'
)
$sessionBuilder = Join-Path $resolvedRepo (
    'scripts\build_cn_core_pack_validation_session_sidecar.py'
)
$labelBuilder = Join-Path $resolvedRepo (
    'scripts\build_cn_phase3cm_forward_label_sidecars.py'
)
$stdoutPath = Join-Path $resolvedRoot 'replay_then_oos.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'replay_then_oos.stderr.log'

[ordered]@{
    schema_version = 'cn_finalist_replay_then_oos_deployment_binding_v1'
    status = 'ACTIVE_FIXED_COHORT_REPLAY_THEN_OOS'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest = $resolvedDeployment
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedDeployment -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    cohort_root = $resolvedCohort
    authority_root = $resolvedAuthority
    output_root = $resolvedRoot
    pair_count = $ExpectedPairCount
    candidate_member_count = $ExpectedPairCount * 2
    validation_threads = $ValidationThreads
    node_resource_profile = $NodeResourceProfile
    node_resource_capacity_path = $resolvedNodeCapacity
    node_resource_capacity_sha256 = (
        Get-FileHash -LiteralPath $resolvedNodeCapacity -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    validation_evidence_class = (
        'REUSED_FIXED_VALIDATION_REPORT_ONLY_NO_PROMOTION'
    )
    interstage_filtering = 'FORBIDDEN'
    optimizer_feedback_write = 'FORBIDDEN'
    scheduler_write = 'FORBIDDEN'
    archive_write = 'FORBIDDEN'
    promotion = 'FORBIDDEN'
    holdout_reads = 0
    forward_2026_reads = 0
    launched_at_utc = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 5 |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'deployment_binding.json'
    ) -Encoding UTF8

$env:PYTHONPATH = Join-Path $resolvedRepo 'src'
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:ARROW_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "validation-$PID"
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
$env:CN_NODE_CPU_ENTITLEMENT = [string]$ValidationThreads

try {
    $ErrorActionPreference = 'Continue'
    & $python $runner prepare `
        --cohort-root $resolvedCohort `
        --authority-root $resolvedAuthority `
        --split-manifest $split `
        --registry $registry `
        --output-root $resolvedRoot `
        --repo-sha $RepoSha `
        --expected-pair-count $ExpectedPairCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "fixed finalist cohort preparation failed: $LASTEXITCODE"
    }

    $candidateTable = Join-Path $preparedRoot (
        'finalist_stock_session_candidates.csv'
    )
    $freeze = Join-Path $preparedRoot (
        'finalist_replay_then_oos_freeze.json'
    )

    $env:NUMBA_NUM_THREADS = '1'
    $env:POLARS_MAX_THREADS = [string]$ValidationThreads
    if (-not (Test-Path -LiteralPath (
        Join-Path $trainFieldRoot (
            'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
        )
    ))) {
        & $python $sessionBuilder `
            --source-root $trainMinuteRoot `
            --evaluation-role train `
            --output-root $trainFieldRoot `
            --candidate-table $candidateTable `
            --registry $registry `
            --split-manifest $split `
            --split-manifest-hash $splitHash `
            --fundamental-root $fundamentalRoot `
            --chip-root $chipRoot `
            --max-shards 16 *>> $stdoutPath
        if ($LASTEXITCODE -ne 0) {
            throw "train session field sidecar build failed: $LASTEXITCODE"
        }
    }

    & $python $runner replay `
        --freeze-manifest $freeze `
        --train-field-root $trainFieldRoot `
        --output-root $replayRoot `
        --expected-pair-count $ExpectedPairCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "A-share executable replay failed: $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath (
        Join-Path $replayRoot 'REPLAY_COMPLETE.json'
    ))) {
        throw "A-share executable replay did not close"
    }

    if (-not (Test-Path -LiteralPath (
        Join-Path $validationFieldRoot (
            'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
        )
    ))) {
        & $python $sessionBuilder `
            --source-root $validationMinuteRoot `
            --evaluation-role validation `
            --output-root $validationFieldRoot `
            --candidate-table $candidateTable `
            --registry $registry `
            --split-manifest $split `
            --split-manifest-hash $splitHash `
            --fundamental-root $fundamentalRoot `
            --chip-root $chipRoot `
            --max-shards 16 *>> $stdoutPath
        if ($LASTEXITCODE -ne 0) {
            throw "validation session field sidecar build failed: $LASTEXITCODE"
        }
    }

    if (-not (Test-Path -LiteralPath (
        Join-Path $validationLabelRoot (
            'CN_FORWARD_LABEL_SIDECAR_MANIFEST.json'
        )
    ))) {
        & $python $labelBuilder `
            --source-root $validationFieldRoot `
            --evaluation-role validation `
            --output-root $validationLabelRoot `
            --split-manifest $split `
            --split-manifest-hash $splitHash `
            --horizons 1,5,15,30 `
            --max-shards 16 `
            --polars-threads $ValidationThreads *>> $stdoutPath
        if ($LASTEXITCODE -ne 0) {
            throw "validation session label sidecar build failed: $LASTEXITCODE"
        }
    }

    $env:NUMBA_NUM_THREADS = [string]$ValidationThreads
    $env:POLARS_MAX_THREADS = '1'
    & $python $runner oos `
        --freeze-manifest $freeze `
        --replay-root $replayRoot `
        --validation-field-root $validationFieldRoot `
        --validation-label-root $validationLabelRoot `
        --output-root $oosRoot `
        --threads $ValidationThreads `
        --expected-pair-count $ExpectedPairCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "unchanged-cohort report-only OOS failed: $LASTEXITCODE"
    }

    & $python $runner finalize `
        --freeze-manifest $freeze `
        --replay-root $replayRoot `
        --oos-root $oosRoot `
        --output-root $resolvedRoot `
        --expected-pair-count $ExpectedPairCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "replay/OOS immutable closure failed: $LASTEXITCODE"
    }
    $ErrorActionPreference = 'Stop'
} catch {
    $_ | Out-String |
        Add-Content -LiteralPath $stderrPath -Encoding UTF8
    [ordered]@{
        repo_sha = $RepoSha
        status = 'FAILED'
        completed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        error = $_.Exception.Message
    } | ConvertTo-Json |
        Set-Content -LiteralPath (
            Join-Path $resolvedRoot 'process_exit.json'
    ) -Encoding UTF8
    throw
} finally {
    & $python $leaseManager release `
        --state-root $NodeResourceStateRoot `
        --lease-id $leaseId `
        --owner-pid $PID *>> $stdoutPath
}

[ordered]@{
    repo_sha = $RepoSha
    status = 'COMPLETED'
    exit_code = 0
    completed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json |
    Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'process_exit.json'
    ) -Encoding UTF8
