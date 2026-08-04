param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$ReplayOosRoot,
    [Parameter(Mandatory = $true)][string]$MarkToMarketRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$SelectionPayloadSha256,
    [ValidateSet('VALIDATION_DUAL_8')]
    [string]$NodeResourceProfile = 'VALIDATION_DUAL_8',
    [ValidateRange(1, 8)][int]$WorkerCount = 8,
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
    throw 'portfolio decoder autopsy is authorized only on DESKTOP-77OPJ6F'
}
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedReplay = (Resolve-Path -LiteralPath $ReplayOosRoot).Path
$resolvedMtm = (Resolve-Path -LiteralPath $MarkToMarketRoot).Path
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
$resolvedNodeCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
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
    'D:\ChengboRemote\runtime\cn_portfolio_decoder_autopsy_v1_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "decoder autopsy output root must be fresh: $resolvedRoot"
}
foreach ($path in @(
    $python,
    $git,
    $resolvedDeployment,
    $resolvedReplay,
    $resolvedMtm,
    $resolvedNodeCapacity
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
$capacity = Get-Content -LiteralPath $resolvedNodeCapacity -Raw |
    ConvertFrom-Json
$profileProperty = $capacity.profiles.PSObject.Properties[$NodeResourceProfile]
if ($null -eq $profileProperty) {
    throw "node resource profile missing: $NodeResourceProfile"
}
$profile = $profileProperty.Value
if (
    [string]$profile.role -ne 'VALIDATION' -or
    [int]$profile.cpu_threads -ne 8
) {
    throw 'decoder autopsy resource profile/thread mismatch'
}
$freeBytes = [int64](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
if ($freeBytes -lt [int64]24 * 1024 * 1024 * 1024) {
    throw "free memory below 24 GiB gate: $freeBytes"
}
$freeze = Join-Path $resolvedReplay (
    'prepared\finalist_replay_then_oos_freeze.json'
)
$trainSidecar = Join-Path $resolvedReplay (
    'sidecars\train_session_fields\CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
)
$mtmClosure = Join-Path $resolvedMtm 'MARK_TO_MARKET_REPLAY_COMPLETE.json'
foreach ($path in @($freeze, $trainSidecar, $mtmClosure)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "immutable decoder input missing: $path"
    }
}

New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
$stdoutPath = Join-Path $resolvedRoot 'decoder_autopsy.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'decoder_autopsy.stderr.log'
[ordered]@{
    schema_version = 'cn_portfolio_decoder_autopsy_deployment_binding_v1'
    status = 'ACTIVE_TRAIN_ONLY_DIAGNOSTIC'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest = $resolvedDeployment
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedDeployment -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    replay_oos_root = $resolvedReplay
    replay_freeze_sha256 = (
        Get-FileHash -LiteralPath $freeze -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    mark_to_market_root = $resolvedMtm
    mark_to_market_closure_sha256 = (
        Get-FileHash -LiteralPath $mtmClosure -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    selection_payload_sha256 = $SelectionPayloadSha256
    output_root = $resolvedRoot
    node_resource_profile = $NodeResourceProfile
    worker_count = $WorkerCount
    horizon_semantics = 'STOCK_SESSION_SHIFT_NOT_MINUTE'
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    optimizer_feedback_write = 'FORBIDDEN'
    scheduler_write = 'FORBIDDEN'
    archive_write = 'FORBIDDEN'
    promotion = 'FORBIDDEN'
} | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (
    Join-Path $resolvedRoot 'deployment_binding.json'
) -Encoding UTF8

$env:PYTHONPATH = "$($resolvedRepo)\src;$resolvedRepo"
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:NUMBA_NUM_THREADS = '8'
$env:POLARS_MAX_THREADS = '8'
$env:ARROW_NUM_THREADS = '8'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "validation-decoder-autopsy-$PID"
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
$env:CN_NODE_CPU_ENTITLEMENT = '8'

try {
    $ErrorActionPreference = 'Continue'
    & $python (Join-Path $resolvedRepo (
        'scripts\build_cn_portfolio_decoder_autopsy_v1.py'
    )) --replay-oos-root $resolvedReplay `
       --mark-to-market-root $resolvedMtm `
       --output-root $resolvedRoot `
       --builder-commit-sha $RepoSha `
       --worker-count $WorkerCount `
       --expected-selection-payload-sha256 $SelectionPayloadSha256 `
       *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "portfolio decoder autopsy failed: $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath (
        Join-Path $resolvedRoot 'DECODER_AUTOPSY_COMPLETE.json'
    ))) {
        throw 'portfolio decoder autopsy did not close'
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
