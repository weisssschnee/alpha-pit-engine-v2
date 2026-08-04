param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$FreezeManifest,
    [Parameter(Mandatory = $true)][string]$TrainFieldRoot,
    [Parameter(Mandatory = $true)][string]$StrictReplayRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [ValidateSet('VALIDATION_DUAL_8')]
    [string]$NodeResourceProfile = 'VALIDATION_DUAL_8',
    [int]$ValidationThreads = 8,
    [switch]$PersistAccountingLedgers,
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
    throw 'mark-to-market replay is authorized only on DESKTOP-77OPJ6F'
}
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedFreeze = (Resolve-Path -LiteralPath $FreezeManifest).Path
$resolvedTrainFields = (Resolve-Path -LiteralPath $TrainFieldRoot).Path
$resolvedStrictReplay = (Resolve-Path -LiteralPath $StrictReplayRoot).Path
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
    'D:\ChengboRemote\runtime\cn_finalist_mark_to_market_replay_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "mark-to-market output root must be fresh: $resolvedRoot"
}
foreach ($path in @(
    $python,
    $git,
    $resolvedDeployment,
    $resolvedFreeze,
    $resolvedTrainFields,
    $resolvedStrictReplay,
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
    [int]$profile.cpu_threads -ne $ValidationThreads
) {
    throw 'validation resource profile/thread mismatch'
}
$freeBytes = [int64](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
if ($freeBytes -lt [int64]24 * 1024 * 1024 * 1024) {
    throw "free memory below 24 GiB gate: $freeBytes"
}
$replayClosure = Join-Path $resolvedStrictReplay 'replay\REPLAY_COMPLETE.json'
$trainClosure = Join-Path $resolvedStrictReplay 'TRAIN_REPLAY_ONLY_COMPLETE.json'
$trainSidecarClosure = Join-Path $resolvedTrainFields (
    'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
)
foreach ($path in @($replayClosure, $trainClosure, $trainSidecarClosure)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "immutable strict replay input missing: $path"
    }
}

New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
$stdoutPath = Join-Path $resolvedRoot 'mark_to_market.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'mark_to_market.stderr.log'
[ordered]@{
    schema_version = 'cn_finalist_mark_to_market_deployment_binding_v1'
    status = 'ACTIVE_DIAGNOSTIC_ONLY'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest = $resolvedDeployment
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedDeployment -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    freeze_manifest = $resolvedFreeze
    freeze_manifest_sha256 = (
        Get-FileHash -LiteralPath $resolvedFreeze -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    strict_replay_closure = $replayClosure
    strict_replay_closure_sha256 = (
        Get-FileHash -LiteralPath $replayClosure -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    train_field_root = $resolvedTrainFields
    output_root = $resolvedRoot
    node_resource_profile = $NodeResourceProfile
    validation_threads = $ValidationThreads
    ending_book_policy = 'FINAL_PIT_CLOSE_MARK_TO_MARKET_NO_TERMINAL_SALE'
    persist_accounting_ledgers = [bool]$PersistAccountingLedgers
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

$env:PYTHONPATH = Join-Path $resolvedRepo 'src'
$env:PYTHONUTF8 = '1'
$env:CN_CAMPAIGN_REPO_SHA = $RepoSha
$env:NUMBA_NUM_THREADS = [string]$ValidationThreads
$env:POLARS_MAX_THREADS = [string]$ValidationThreads
$env:ARROW_NUM_THREADS = [string]$ValidationThreads
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "validation-mtm-$PID"
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
    $replayArguments = @(
        '--freeze-manifest', $resolvedFreeze,
        '--train-field-root', $resolvedTrainFields,
        '--strict-replay-root', $resolvedStrictReplay,
        '--output-root', $resolvedRoot
    )
    if ($PersistAccountingLedgers) {
        $replayArguments += '--persist-accounting-ledgers'
    }
    & $python (Join-Path $resolvedRepo (
        'scripts\run_cn_finalist_mark_to_market_replay.py'
    )) @replayArguments *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "mark-to-market replay failed: $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath (
        Join-Path $resolvedRoot 'MARK_TO_MARKET_REPLAY_COMPLETE.json'
    ))) {
        throw 'mark-to-market replay did not close'
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
