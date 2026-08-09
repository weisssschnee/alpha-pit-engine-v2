param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$PhaseCFreezeRoot,
    [Parameter(Mandatory = $true)][string]$ExecutionContract,
    [Parameter(Mandatory = $true)][string]$TrainFieldRoot,
    [Parameter(Mandatory = $true)][string]$TrainPriceRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [ValidateRange(1, 32)][int]$ExecutorWorkerCount = 10,
    [string]$Registry = (
        'runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json'
    ),
    [string]$NodeResourceCapacity = (
        'runtime\run_plans\cn_alpha_node_resource_profiles_v1.json'
    ),
    [string]$NodeResourceStateRoot = (
        'D:\ChengboRemote\runtime\node_resource_governor'
    )
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Get-SharedReadSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)

    $stream = [IO.FileStream]::new(
        $Path,
        [IO.FileMode]::Open,
        [IO.FileAccess]::Read,
        [IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete
    )
    try {
        $hasher = [Security.Cryptography.SHA256]::Create()
        try {
            return (
                [BitConverter]::ToString($hasher.ComputeHash($stream))
            ).Replace('-', '').ToLowerInvariant()
        } finally {
            $hasher.Dispose()
        }
    } finally {
        $stream.Dispose()
    }
}

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw 'joint-program Phase C is authorized only on DESKTOP-77OPJ6F'
}
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedFreeze = (Resolve-Path -LiteralPath $PhaseCFreezeRoot).Path
$resolvedContract = (Resolve-Path -LiteralPath $ExecutionContract).Path
$resolvedFields = (Resolve-Path -LiteralPath $TrainFieldRoot).Path
$resolvedPrices = (Resolve-Path -LiteralPath $TrainPriceRoot).Path
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
$resolvedRegistry = if ([IO.Path]::IsPathRooted($Registry)) {
    [IO.Path]::GetFullPath($Registry)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $Registry))
}
$resolvedCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
    [IO.Path]::GetFullPath($NodeResourceCapacity)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $NodeResourceCapacity))
}
if (-not $resolvedRepo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $resolvedRepo"
}
if (-not $resolvedRoot.StartsWith(
    'D:\ChengboRemote\runtime\cn_joint_program_rolling_search_v0_phase_c_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "joint-program Phase C output root must be fresh: $resolvedRoot"
}
foreach ($path in @(
    $python,
    $git,
    $resolvedDeployment,
    $resolvedFreeze,
    $resolvedContract,
    $resolvedFields,
    $resolvedPrices,
    $resolvedRegistry,
    $resolvedCapacity
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required input missing: $path"
    }
}
$gitSafeDirectory = "safe.directory=$($resolvedRepo.Replace('\', '/'))"
$observedSha = (& $git -c $gitSafeDirectory -C $resolvedRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $observedSha -ne $RepoSha) {
    throw "deployed repo SHA drift: expected=$RepoSha observed=$observedSha"
}
$dirty = @(& $git -c $gitSafeDirectory -C $resolvedRepo status --porcelain)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw 'deployed workspace must be clean'
}
$deployment = Get-Content -LiteralPath $resolvedDeployment -Raw |
    ConvertFrom-Json
if (
    [string]$deployment.repo_sha -ne $RepoSha -or
    [string]$deployment.workspace -ne $resolvedRepo
) {
    throw 'deployment manifest binding drift'
}
$capacity = Get-Content -LiteralPath $resolvedCapacity -Raw |
    ConvertFrom-Json
$profile = $capacity.profiles.VALIDATION_EXCLUSIVE_32
if (
    [string]$profile.role -ne 'VALIDATION' -or
    [int]$profile.cpu_threads -ne 32 -or
    [int64]$profile.minimum_free_memory_bytes -ne [int64]24 * 1024 * 1024 * 1024
) {
    throw 'joint-program Phase C resource profile drift'
}
if ($ExecutorWorkerCount -ne 10) {
    throw 'joint-program Phase C executor worker count is frozen at 10'
}
$freeBytes = [int64](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
if ($freeBytes -lt [int64]24 * 1024 * 1024 * 1024) {
    throw "free memory below 24 GiB gate: $freeBytes"
}
$freezeClosure = Join-Path $resolvedFreeze (
    'CN_JOINT_PROGRAM_PHASE_C_PREFINANCIAL_FREEZE_COMPLETE.json'
)
$fieldManifest = Join-Path $resolvedFields (
    'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
)
$priceManifest = Join-Path $resolvedPrices (
    'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
)
foreach ($path in @($freezeClosure, $fieldManifest, $priceManifest)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "immutable Phase C input missing: $path"
    }
}

New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
$stdoutPath = Join-Path $resolvedRoot 'joint_program_phase_c.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'joint_program_phase_c.stderr.log'
[ordered]@{
    schema_version = 'cn_joint_program_phase_c_deployment_binding_v0'
    status = 'ACTIVE_DEVELOPMENT_ONLY_FACTORIZED_COMPARISON'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest = $resolvedDeployment
    deployment_manifest_sha256 = Get-SharedReadSha256 $resolvedDeployment
    phase_c_freeze_root = $resolvedFreeze
    phase_c_freeze_closure_sha256 = Get-SharedReadSha256 $freezeClosure
    execution_contract = $resolvedContract
    execution_contract_sha256 = Get-SharedReadSha256 $resolvedContract
    train_field_root = $resolvedFields
    train_price_root = $resolvedPrices
    output_root = $resolvedRoot
    node_resource_profile = 'VALIDATION_EXCLUSIVE_32'
    entitlement_threads = 32
    execution_backend = 'PROCESS_POOL'
    executor_worker_count = $ExecutorWorkerCount
    executor_lifecycle = 'CHECKPOINT_SCOPED_RECYCLE'
    maximum_inflight_records = 8
    native_threads_per_worker = 1
    validation_reads = 0
    holdout_reads = 0
    historical_2023_reads = 0
    forward_b_reads = 0
    forward_2026_reads = 0
    campaign_local_bandit_feedback = 'AUTHORIZED_CHECKPOINT_ONLY'
    formal_optimizer_feedback_write = 'FORBIDDEN'
    formal_scheduler_write = 'FORBIDDEN'
    archive_write = 'FORBIDDEN'
    promotion = 'FORBIDDEN'
} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (
    Join-Path $resolvedRoot 'deployment_binding.json'
) -Encoding UTF8

$env:PYTHONPATH = "$($resolvedRepo)\src;$resolvedRepo"
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:NUMBA_NUM_THREADS = '1'
$env:POLARS_MAX_THREADS = '1'
$env:ARROW_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "validation-joint-program-phase-c-$PID"
$leaseReceiptRoot = Join-Path $resolvedRoot 'resource_leases'
New-Item -ItemType Directory -Force -Path $leaseReceiptRoot | Out-Null
$leaseReceipt = Join-Path $leaseReceiptRoot "$leaseId.json"
& $python $leaseManager acquire `
    --state-root $NodeResourceStateRoot `
    --capacity-manifest $resolvedCapacity `
    --profile 'VALIDATION_EXCLUSIVE_32' `
    --lease-id $leaseId `
    --owner-pid $PID `
    --workload-id $resolvedRoot `
    --receipt $leaseReceipt *>> $stdoutPath
if ($LASTEXITCODE -ne 0) {
    throw "node resource lease admission failed: $LASTEXITCODE"
}
$env:CN_NODE_RESOURCE_LEASE_REQUIRED = '1'
$env:CN_NODE_RESOURCE_LEASE_RECEIPT = $leaseReceipt
$env:CN_NODE_CPU_ENTITLEMENT = '32'

try {
    $ErrorActionPreference = 'Continue'
    & $python (Join-Path $resolvedRepo (
        'scripts\run_cn_joint_program_phase_c_v0.py'
    )) `
        --phase-c-freeze-root $resolvedFreeze `
        --execution-contract $resolvedContract `
        --train-field-root $resolvedFields `
        --train-price-root $resolvedPrices `
        --registry $resolvedRegistry `
        --node-resource-capacity $resolvedCapacity `
        --output-root $resolvedRoot `
        --builder-commit-sha $RepoSha `
        --executor-workers $ExecutorWorkerCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "joint-program Phase C failed: $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath (
        Join-Path $resolvedRoot 'CN_JOINT_PROGRAM_PHASE_C_COMPLETE.json'
    ))) {
        throw 'joint-program Phase C did not close'
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
