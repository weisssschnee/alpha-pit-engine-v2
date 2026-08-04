param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$FinalistRoot,
    [Parameter(Mandatory = $true)][string]$SourceReplayRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$SelectionPayloadSha256,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$DecoderPolicySha256,
    [ValidateSet('VALIDATION_EXCLUSIVE_32')]
    [string]$NodeResourceProfile = 'VALIDATION_EXCLUSIVE_32',
    [ValidateRange(1, 32)][int]$WorkerCount = 32,
    [ValidateRange(1, 32)][int]$ExecutorWorkerCount = 12,
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
    throw 'Decoder V2 report-only OOS is authorized only on DESKTOP-77OPJ6F'
}
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedFinalist = (Resolve-Path -LiteralPath $FinalistRoot).Path
$resolvedSource = (Resolve-Path -LiteralPath $SourceReplayRoot).Path
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
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
    'D:\ChengboRemote\runtime\cn_decoder_v2_topk10_equal_report_only_oos_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected OOS output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "OOS output root must be fresh: $resolvedRoot"
}
foreach ($path in @(
    $python,
    $git,
    $resolvedDeployment,
    $resolvedFinalist,
    $resolvedSource,
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
    throw 'deployed OOS workspace must be clean'
}
$deployment = Get-Content -LiteralPath $resolvedDeployment -Raw |
    ConvertFrom-Json
if (
    [string]$deployment.repo_sha -ne $RepoSha -or
    [string]$deployment.workspace -ne $resolvedRepo
) {
    throw 'OOS deployment manifest binding drift'
}
$capacity = Get-Content -LiteralPath $resolvedCapacity -Raw | ConvertFrom-Json
$profileProperty = $capacity.profiles.PSObject.Properties[$NodeResourceProfile]
if ($null -eq $profileProperty) {
    throw "node resource profile missing: $NodeResourceProfile"
}
$profile = $profileProperty.Value
if (
    [string]$profile.role -ne 'VALIDATION' -or
    [int]$profile.cpu_threads -ne $WorkerCount -or
    $ExecutorWorkerCount -gt $WorkerCount
) {
    throw 'OOS resource entitlement drift'
}
$freeBytes = [int64](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
if ($freeBytes -lt [int64]24 * 1024 * 1024 * 1024) {
    throw "free memory below 24 GiB gate: $freeBytes"
}
$finalistManifest = Join-Path $resolvedFinalist 'finalist_manifest.json'
$sourceContract = Join-Path $resolvedSource (
    'prepared\replay_then_oos_execution_contract.json'
)
$fieldManifest = Join-Path $resolvedSource (
    'sidecars\validation_session_fields\CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
)
$labelManifest = Join-Path $resolvedSource (
    'sidecars\validation_session_labels\CN_FORWARD_LABEL_SIDECAR_MANIFEST.json'
)
foreach ($path in @(
    $finalistManifest,
    $sourceContract,
    $fieldManifest,
    $labelManifest
)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "immutable OOS input missing: $path"
    }
}

New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
$stdoutPath = Join-Path $resolvedRoot 'oos.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'oos.stderr.log'
[ordered]@{
    schema_version = 'cn_decoder_v2_oos_deployment_binding_v1'
    status = 'ACTIVE_ADAPTIVE_REPORT_ONLY_VALIDATION_OOS'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest = $resolvedDeployment
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedDeployment -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    finalist_root = $resolvedFinalist
    finalist_manifest_sha256 = (
        Get-FileHash -LiteralPath $finalistManifest -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    source_replay_root = $resolvedSource
    source_contract_sha256 = (
        Get-FileHash -LiteralPath $sourceContract -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    selection_payload_sha256 = $SelectionPayloadSha256
    decoder_id = 'TOPK_10_EQUAL'
    decoder_policy_sha256 = $DecoderPolicySha256
    pair_count = 22
    candidate_member_count = 44
    interstage_filter_applied = $false
    intention_to_treat = $true
    train_recomputed = $false
    evaluation_role = 'validation'
    data_role = 'validation_report_only'
    node_resource_profile = $NodeResourceProfile
    worker_count = $WorkerCount
    execution_backend = 'PROCESS_POOL'
    executor_worker_count = $ExecutorWorkerCount
    native_threads_per_executor_worker = 1
    minimum_free_memory_bytes = [int64]24 * 1024 * 1024 * 1024
    validation_reads = 'POSITIVE_REQUIRED'
    holdout_reads = 0
    forward_2026_reads = 0
    optimizer_feedback_write = 'FORBIDDEN'
    scheduler_write = 'FORBIDDEN'
    archive_write = 'FORBIDDEN'
    promotion = 'FORBIDDEN'
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (
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
$leaseId = "validation-decoder-v2-oos-$PID"
$leaseReceiptRoot = Join-Path $resolvedRoot 'resource_leases'
New-Item -ItemType Directory -Force -Path $leaseReceiptRoot | Out-Null
$leaseReceipt = Join-Path $leaseReceiptRoot "$leaseId.json"
& $python $leaseManager acquire `
    --state-root $NodeResourceStateRoot `
    --capacity-manifest $resolvedCapacity `
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
$env:CN_NODE_CPU_ENTITLEMENT = [string]$WorkerCount

try {
    $ErrorActionPreference = 'Continue'
    & $python (Join-Path $resolvedRepo (
        'scripts\run_cn_portfolio_decoder_v2_oos.py'
    )) `
        --finalist-root $resolvedFinalist `
        --source-replay-root $resolvedSource `
        --output-root $resolvedRoot `
        --builder-commit-sha $RepoSha `
        --expected-selection-payload-sha256 $SelectionPayloadSha256 `
        --expected-decoder-policy-sha256 $DecoderPolicySha256 `
        --worker-count $WorkerCount `
        --executor-worker-count $ExecutorWorkerCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "Decoder V2 report-only OOS failed: $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath (
        Join-Path $resolvedRoot 'OOS_COMPLETE.json'
    ))) {
        throw 'Decoder V2 report-only OOS did not close'
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
