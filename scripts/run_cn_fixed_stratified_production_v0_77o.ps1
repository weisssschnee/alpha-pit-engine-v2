param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$DeploymentManifest,
    [Parameter(Mandatory = $true)]
    [string]$PreflightRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [Parameter(Mandatory = $true)]
    [ValidateSet(
        'LAUNCH_HIGH_COST_CAMPAIGN',
        'SUCCESSOR_CAMPAIGN',
        'RETRY'
    )]
    [string]$RequestedAction,
    [Parameter(Mandatory = $true)]
    [string]$TargetRunId,
    [Parameter(Mandatory = $true)]
    [string]$ProjectControlAdmission,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ProjectControlAdmissionSha256,
    [ValidateSet('VALIDATION_EXCLUSIVE_32')]
    [string]$NodeResourceProfile = 'VALIDATION_EXCLUSIVE_32',
    [string]$NodeResourceCapacity = (
        'runtime\run_plans\cn_alpha_node_resource_profiles_v1.json'
    ),
    [string]$NodeResourceStateRoot = (
        'D:\ChengboRemote\runtime\node_resource_governor'
    ),
    [int]$MaximumWallSeconds = 14400,
    [double]$RequiredPairsPerHour = 57.25
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$minimumFreeMemoryBytes = [int64]24GB
$cacheCapBytes = [int64]8GB
$trainBase = (
    'D:\ChengboRemote\runtime\' +
    'cn_core_pack_aggressive_discovery_20260718_595c5fc\' +
    'strict_wave_01024_sidecars_3509d0c'
)
$labelBase = (
    'D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git\' +
    'runtime\cn_phase3cm_streaming_repair_20260716'
)
$split = (
    'D:\ChengboRemote\workspace\' +
    'cn_phase3cm_1024_sidecar_closure_0aba8c5\' +
    'runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv'
)

$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedManifest = [IO.Path]::GetFullPath($DeploymentManifest)
$resolvedPreflight = [IO.Path]::GetFullPath($PreflightRoot)
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
$resolvedAdmission = [IO.Path]::GetFullPath($ProjectControlAdmission)
$resolvedStateRoot = [IO.Path]::GetFullPath($NodeResourceStateRoot)
$resolvedNodeCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
    [IO.Path]::GetFullPath($NodeResourceCapacity)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $NodeResourceCapacity))
}

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw "unauthorized host: $($env:COMPUTERNAME)"
}
if (-not $resolvedRepo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $resolvedRepo"
}
if (-not $resolvedPreflight.StartsWith(
    'D:\ChengboRemote\runtime\cn_candidate_representation_v0_preflight_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected preflight root: $resolvedPreflight"
}
if (-not $resolvedRoot.StartsWith(
    'D:\ChengboRemote\runtime\cn_fixed_stratified_production_v0_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    $existing = @(Get-ChildItem -LiteralPath $resolvedRoot -Force)
    if ($existing.Count -ne 0) {
        throw "output root must be fresh: $resolvedRoot"
    }
} else {
    New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
}

foreach ($path in @(
    $python,
    $resolvedManifest,
    $resolvedAdmission,
    $resolvedPreflight,
    $resolvedNodeCapacity,
    $resolvedStateRoot,
    $split,
    (Join-Path $trainBase 'CN_PHASE3CM_1024_SIDECAR_CLOSURE.json'),
    (Join-Path $trainBase 'active_time_major_train_v1'),
    (Join-Path $trainBase 'session_time_major_train_v1'),
    (Join-Path $labelBase 'time_major_train_v3_labels'),
    (Join-Path $labelBase 'session_time_major_train_v3_labels'),
    (Join-Path $resolvedRepo (
        'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
        'unified_capability_registry.json'
    )),
    (Join-Path $resolvedPreflight (
        'CANDIDATE_REPRESENTATION_V0_PREFLIGHT_COMPLETE.json'
    ))
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required input missing: $path"
    }
}

$head = (& git -C $resolvedRepo rev-parse HEAD).Trim().ToLowerInvariant()
$status = @(& git -C $resolvedRepo status --porcelain)
if ($LASTEXITCODE -ne 0 -or $head -ne $RepoSha.ToLowerInvariant()) {
    throw "repo HEAD mismatch: expected=$RepoSha observed=$head"
}
if ($status.Count -ne 0) {
    throw "remote repo must be clean"
}

$manifest = Get-Content -LiteralPath $resolvedManifest -Raw | ConvertFrom-Json
$manifestSha = if ($manifest.head) {
    [string]$manifest.head
} else {
    [string]$manifest.repo_sha
}
$manifestWorkspace = if ($manifest.workspace) {
    [string]$manifest.workspace
} else {
    [string]$manifest.remote_workspace
}
if (
    $manifestSha.ToLowerInvariant() -ne $RepoSha.ToLowerInvariant() -or
    [IO.Path]::GetFullPath($manifestWorkspace) -ne $resolvedRepo
) {
    throw "deployment manifest binding mismatch"
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
$computeThreads = [int]$profile.cpu_threads
if (
    [string]$profile.role -ne 'VALIDATION' -or
    $computeThreads -ne 32 -or
    [int64]$profile.minimum_free_memory_bytes -ne $minimumFreeMemoryBytes
) {
    throw "invalid fixed-stratified node profile"
}

$matching = @(
    Get-CimInstance Win32_Process |
        Where-Object {
            $_.ProcessId -ne $PID -and
            $_.Name -match '^python(w)?\.exe$' -and
            $_.CommandLine -and (
                $_.CommandLine -like "*$resolvedRoot*" -or
                $_.CommandLine -match 'cn-fixed-stratified-production-v0'
            )
        }
)
if ($matching) {
    throw "duplicate fixed-stratified production process detected"
}
$memory = Get-CimInstance Win32_OperatingSystem
$freeMemoryBytes = [int64]$memory.FreePhysicalMemory * 1024
if ($freeMemoryBytes -lt $minimumFreeMemoryBytes) {
    throw "minimum free memory gate failed: $freeMemoryBytes"
}

$taskLogRoot = 'D:\ChengboRemote\runtime\task_logs'
New-Item -ItemType Directory -Force -Path $taskLogRoot | Out-Null
$taskStem = Split-Path -Leaf $resolvedRoot
$stdoutPath = Join-Path $taskLogRoot ($taskStem + '.stdout.log')
$stderrPath = Join-Path $taskLogRoot ($taskStem + '.stderr.log')
$exitPath = Join-Path $taskLogRoot ($taskStem + '.process_exit.json')
$leaseReceiptRoot = Join-Path $taskLogRoot 'resource_leases'
New-Item -ItemType Directory -Force -Path $leaseReceiptRoot | Out-Null
$leaseId = "validation-fixed-stratified-v0-$PID"
$leaseReceipt = Join-Path $leaseReceiptRoot ($leaseId + '.json')

[ordered]@{
    schema_version = 'cn_fixed_stratified_production_v0_deployment_v1'
    repo_sha = $RepoSha
    repo = $resolvedRepo
    deployment_manifest_path = $resolvedManifest
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedManifest -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    preflight_root = $resolvedPreflight
    preflight_closure_sha256 = (
        Get-FileHash -LiteralPath (
            Join-Path $resolvedPreflight (
                'CANDIDATE_REPRESENTATION_V0_PREFLIGHT_COMPLETE.json'
            )
        ) -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    node_resource_profile = $NodeResourceProfile
    node_resource_capacity_path = $resolvedNodeCapacity
    node_resource_capacity_sha256 = (
        Get-FileHash -LiteralPath $resolvedNodeCapacity -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    compute_threads = $computeThreads
    active_pair_batch_size = 12
    session_pair_batch_size = 24
    evaluator_cache_cap_bytes = $cacheCapBytes
    minimum_free_memory_bytes = $minimumFreeMemoryBytes
    maximum_wall_seconds = $MaximumWallSeconds
    required_pairs_per_hour = $RequiredPairsPerHour
    shared_tpe_credit = $false
    optimizer_feedback_accessed = $false
    dynamic_budget_reallocation_allowed = $false
    underfill_spillover_allowed = $false
    validation = 'SEALED'
    holdout = 'SEALED'
    forward_2026 = 'SEALED'
    launched_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (
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

$commandArgs = @(
    (Join-Path $resolvedRepo 'app.py'),
    'cn-fixed-stratified-production-v0',
    '--requested-action', $RequestedAction,
    '--target-run-id', $TargetRunId,
    '--project-control-admission', $resolvedAdmission,
    '--project-control-admission-sha256', (
        $ProjectControlAdmissionSha256.ToLowerInvariant()
    ),
    '--',
    '--preflight-root', $resolvedPreflight,
    '--registry',
    (Join-Path $resolvedRepo (
        'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
        'unified_capability_registry.json'
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
    '--output-root', $resolvedRoot,
    '--compute-threads', [string]$computeThreads,
    '--maximum-wall-seconds', [string]$MaximumWallSeconds,
    '--required-pairs-per-hour', [string]$RequiredPairsPerHour
)

$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
& $python $leaseManager acquire `
    --state-root $resolvedStateRoot `
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

$processExitCode = 1
$releaseExitCode = 1
try {
    $process = Start-Process -FilePath $python -ArgumentList $commandArgs `
        -WorkingDirectory $resolvedRepo -Wait -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath
    $processExitCode = $process.ExitCode
} finally {
    try {
        & $python $leaseManager release `
            --state-root $resolvedStateRoot `
            --lease-id $leaseId `
            --owner-pid $PID *>> $stdoutPath
        $releaseExitCode = $LASTEXITCODE
    } catch {
        $releaseExitCode = 1
        $_ | Out-String | Add-Content -LiteralPath $stderrPath
    }
    if ($releaseExitCode -ne 0 -and $processExitCode -eq 0) {
        $processExitCode = 91
    }
}
[ordered]@{
    repo_sha = $RepoSha
    output_root = $resolvedRoot
    exit_code = $processExitCode
    lease_release_exit_code = $releaseExitCode
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
    completed_at = (Get-Date).ToUniversalTime().ToString('o')
} | ConvertTo-Json | Set-Content -LiteralPath $exitPath -Encoding UTF8
exit $processExitCode
