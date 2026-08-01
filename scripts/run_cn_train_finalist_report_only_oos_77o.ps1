param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$DeploymentManifest,
    [Parameter(Mandatory = $true)]
    [string]$FinalistRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [ValidateRange(1, 8)]
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
    throw 'this runner is authorized only on DESKTOP-77OPJ6F'
}

$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = (Resolve-Path -LiteralPath $RepoRoot).Path
$resolvedFinalist = (Resolve-Path -LiteralPath $FinalistRoot).Path
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
$resolvedNodeCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
    [IO.Path]::GetFullPath($NodeResourceCapacity)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $NodeResourceCapacity))
}
foreach ($path in @($python, $git, $resolvedNodeCapacity)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "required runtime input is missing: $path"
    }
}
if (Test-Path -LiteralPath $resolvedRoot -PathType Leaf) {
    throw "output root is a file: $resolvedRoot"
}
New-Item -ItemType Directory -Path $resolvedRoot -Force | Out-Null
foreach ($closedPath in @(
    (Join-Path $resolvedRoot 'REPLAY_THEN_OOS_COMPLETE.json'),
    (Join-Path $resolvedRoot 'process_exit.json'),
    (Join-Path $resolvedRoot 'oos\OOS_COMPLETE.json')
)) {
    if (Test-Path -LiteralPath $closedPath) {
        throw "single-finalist OOS output is already terminal: $closedPath"
    }
}

$gitSafePath = $resolvedRepo.Replace('\', '/')
$gitSafeDirectory = "safe.directory=$gitSafePath"
$observedSha = (
    & $git -c $gitSafeDirectory -C $resolvedRepo rev-parse HEAD
).Trim()
if ($LASTEXITCODE -ne 0 -or $observedSha -ne $RepoSha) {
    throw "deployed repo SHA drift: expected=$RepoSha observed=$observedSha"
}
$dirty = @(& $git -c $gitSafeDirectory -C $resolvedRepo status --porcelain)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw 'deployed workspace must be clean'
}
$deployment = Get-Content -LiteralPath $resolvedDeployment -Raw |
    ConvertFrom-Json
if ([string]$deployment.repo_sha -ne $RepoSha) {
    throw 'deployment manifest repo SHA drift'
}
if ([string]$deployment.workspace -ne $resolvedRepo) {
    throw 'deployment manifest workspace drift'
}
$nodeCapacity = Get-Content -LiteralPath $resolvedNodeCapacity -Raw |
    ConvertFrom-Json
$profileProperty = $nodeCapacity.profiles.PSObject.Properties[
    $NodeResourceProfile
]
if ($null -eq $profileProperty) {
    throw "node resource profile missing: $NodeResourceProfile"
}
$profile = $profileProperty.Value
if (
    [string]$profile.role -ne 'VALIDATION' -or
    [int]$profile.cpu_threads -ne $ValidationThreads
) {
    throw 'validation resource profile/thread mismatch'
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
    throw 'fixed split manifest hash drift'
}
$registry = Join-Path $resolvedRepo (
    'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
    'unified_capability_registry.json'
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
    $validationMinuteRoot,
    $fundamentalRoot,
    $chipRoot
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required OOS input is missing: $path"
    }
}

$prepareRunner = Join-Path $resolvedRepo (
    'scripts\prepare_cn_train_finalist_report_only_oos.py'
)
$oosRunner = Join-Path $resolvedRepo (
    'scripts\run_cn_finalist_replay_then_oos.py'
)
$sessionBuilder = Join-Path $resolvedRepo (
    'scripts\build_cn_core_pack_validation_session_sidecar.py'
)
$labelBuilder = Join-Path $resolvedRepo (
    'scripts\build_cn_phase3cm_forward_label_sidecars.py'
)
$leaseManager = Join-Path $resolvedRepo (
    'scripts\manage_cn_node_resource_lease.py'
)
$preparedClosure = Join-Path $resolvedRoot (
    'FINALIST_REPORT_ONLY_OOS_PREPARED.json'
)
$preflightReceipt = Join-Path $resolvedRoot (
    'FINALIST_REPORT_ONLY_OOS_PREFLIGHT_VERIFICATION.json'
)
$freeze = Join-Path $resolvedRoot (
    'prepared\finalist_replay_then_oos_freeze.json'
)
$candidateTable = Join-Path $resolvedRoot (
    'prepared\finalist_stock_session_candidates.csv'
)
$derivedReplayRoot = Join-Path $resolvedRoot 'derived_replay_evidence'
$validationFieldRoot = Join-Path $resolvedRoot (
    'sidecars\validation_session_fields'
)
$validationLabelRoot = Join-Path $resolvedRoot (
    'sidecars\validation_session_labels'
)
$oosRoot = Join-Path $resolvedRoot 'oos'
$stdoutPath = Join-Path $resolvedRoot 'single_finalist_oos.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'single_finalist_oos.stderr.log'

[ordered]@{
    schema_version = 'cn_single_train_finalist_report_only_oos_deployment_v1'
    status = 'ACTIVE_EXPLICIT_SINGLE_PAIR_REPORT_ONLY_OOS'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest = $resolvedDeployment
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedDeployment -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    finalist_root = $resolvedFinalist
    output_root = $resolvedRoot
    pair_count = 1
    candidate_member_count = 2
    validation_threads = $ValidationThreads
    node_resource_profile = $NodeResourceProfile
    train_replay_recomputed = $false
    validation_usage = 'REPORT_ONLY'
    interstage_filtering = 'FORBIDDEN'
    optimizer_feedback_write = 'FORBIDDEN'
    scheduler_write = 'FORBIDDEN'
    archive_write = 'FORBIDDEN'
    promotion = 'FORBIDDEN'
    successor_search = 'FORBIDDEN'
    holdout_reads = 0
    forward_2026_reads = 0
    launched_at_utc = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (
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
    if (-not (Test-Path -LiteralPath $preparedClosure -PathType Leaf)) {
        & $python $prepareRunner prepare `
            --finalist-root $resolvedFinalist `
            --output-root $resolvedRoot `
            --repo-sha $RepoSha *>> $stdoutPath
        if ($LASTEXITCODE -ne 0) {
            throw "single-finalist OOS preparation failed: $LASTEXITCODE"
        }
    }
    & $python $prepareRunner verify `
        --output-root $resolvedRoot `
        --receipt $preflightReceipt *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "single-finalist zero-financial verification failed: $LASTEXITCODE"
    }

    $env:NUMBA_NUM_THREADS = '1'
    $env:POLARS_MAX_THREADS = [string]$ValidationThreads
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

    $env:NUMBA_NUM_THREADS = [string]$ValidationThreads
    $env:POLARS_MAX_THREADS = '1'
    & $python $oosRunner oos `
        --freeze-manifest $freeze `
        --replay-root $derivedReplayRoot `
        --validation-field-root $validationFieldRoot `
        --validation-label-root $validationLabelRoot `
        --output-root $oosRoot `
        --threads $ValidationThreads `
        --expected-pair-count 1 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "single-finalist report-only OOS failed: $LASTEXITCODE"
    }

    & $python $oosRunner finalize `
        --freeze-manifest $freeze `
        --replay-root $derivedReplayRoot `
        --oos-root $oosRoot `
        --output-root $resolvedRoot `
        --expected-pair-count 1 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "single-finalist OOS immutable closure failed: $LASTEXITCODE"
    }
    $ErrorActionPreference = 'Stop'
} catch {
    $_ | Out-String | Add-Content -LiteralPath $stderrPath -Encoding UTF8
    [ordered]@{
        repo_sha = $RepoSha
        status = 'FAILED'
        completed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        error = $_.Exception.Message
    } | ConvertTo-Json | Set-Content -LiteralPath (
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
} | ConvertTo-Json | Set-Content -LiteralPath (
    Join-Path $resolvedRoot 'process_exit.json'
) -Encoding UTF8
