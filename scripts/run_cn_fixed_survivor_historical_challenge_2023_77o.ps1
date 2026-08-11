param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$ConfirmationRoot,
    [Parameter(Mandatory = $true)][string]$AuthorizationPath,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$AuthorizationSha256,
    [Parameter(Mandatory = $true)][string]$AccessStartedPath,
    [Parameter(Mandatory = $true)][string]$HistoricalArchive,
    [Parameter(Mandatory = $true)][string]$HistoricalDailyStInputRoot,
    [Parameter(Mandatory = $true)][string]$PublicSourceRoot,
    [Parameter(Mandatory = $true)][string]$ExecutionContractPath,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$SelectionPayloadSha256,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$DecoderPolicySha256,
    [ValidateSet('VALIDATION_EXCLUSIVE_32')]
    [string]$NodeResourceProfile = 'VALIDATION_EXCLUSIVE_32',
    [ValidateRange(1, 32)][int]$WorkerCount = 32,
    [ValidateRange(1, 32)][int]$ExecutorWorkerCount = 12,
    [ValidateRange(1, 24)][int]$SidecarThreads = 24,
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
    param([Parameter(Mandatory = $true)][string]$LiteralPath)
    $share = [IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete
    $stream = [IO.File]::Open(
        $LiteralPath, [IO.FileMode]::Open, [IO.FileAccess]::Read, $share
    )
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString($sha.ComputeHash($stream)) -replace '-', '').ToLowerInvariant()
    } finally {
        $sha.Dispose()
        $stream.Dispose()
    }
}

function Copy-SharedReadFile {
    param(
        [Parameter(Mandatory = $true)][string]$LiteralPath,
        [Parameter(Mandatory = $true)][string]$Destination
    )
    $share = [IO.FileShare]::ReadWrite -bor [IO.FileShare]::Delete
    $source = [IO.File]::Open(
        $LiteralPath, [IO.FileMode]::Open, [IO.FileAccess]::Read, $share
    )
    $target = [IO.File]::Open(
        $Destination, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write,
        [IO.FileShare]::None
    )
    try {
        $source.CopyTo($target)
        $target.Flush($true)
    } finally {
        $target.Dispose()
        $source.Dispose()
    }
}

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw 'Historical challenge is authorized only on DESKTOP-77OPJ6F'
}
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = (Resolve-Path -LiteralPath $Repo).Path
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedConfirmation = (Resolve-Path -LiteralPath $ConfirmationRoot).Path
$resolvedAuthorization = (Resolve-Path -LiteralPath $AuthorizationPath).Path
$resolvedAccess = (Resolve-Path -LiteralPath $AccessStartedPath).Path
$resolvedArchive = (Resolve-Path -LiteralPath $HistoricalArchive).Path
$resolvedDailyStInput = (Resolve-Path -LiteralPath $HistoricalDailyStInputRoot).Path
$resolvedPublic = (Resolve-Path -LiteralPath $PublicSourceRoot).Path
$resolvedExecution = (Resolve-Path -LiteralPath $ExecutionContractPath).Path
$resolvedRoot = [IO.Path]::GetFullPath($OutputRoot)
$resolvedCapacity = if ([IO.Path]::IsPathRooted($NodeResourceCapacity)) {
    [IO.Path]::GetFullPath($NodeResourceCapacity)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $NodeResourceCapacity))
}
if (-not $resolvedRepo.StartsWith('D:\ChengboRemote\workspace\')) {
    throw "Unexpected repo path: $resolvedRepo"
}
if (-not $resolvedRoot.StartsWith(
    'D:\ChengboRemote\runtime\cn_fixed10_historical_challenge_2023_'
)) {
    throw "Unexpected historical challenge output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "Historical challenge output root must be fresh: $resolvedRoot"
}
foreach ($path in @(
    $python, $git, $resolvedDeployment, $resolvedConfirmation,
    $resolvedAuthorization, $resolvedAccess, $resolvedArchive,
    $resolvedDailyStInput, $resolvedPublic, $resolvedExecution,
    $resolvedCapacity
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Required input missing: $path"
    }
}

$safe = "safe.directory=$($resolvedRepo.Replace('\', '/'))"
$observedSha = (& $git -c $safe -C $resolvedRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $observedSha -ne $RepoSha) {
    throw "Deployed repo SHA drift: expected=$RepoSha observed=$observedSha"
}
$dirty = @(& $git -c $safe -C $resolvedRepo status --porcelain)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw 'Deployed historical-challenge workspace must be clean'
}
$deployment = Get-Content -LiteralPath $resolvedDeployment -Raw | ConvertFrom-Json
$deploymentWorkspace = if ($null -ne $deployment.PSObject.Properties['remote_workspace']) {
    [string]$deployment.remote_workspace
} else {
    [string]$deployment.workspace
}
if (
    [string]$deployment.repo_sha -ne $RepoSha -or
    $deploymentWorkspace -ne $resolvedRepo
) {
    throw 'Historical-challenge deployment manifest binding drift'
}
$historicalAdmissionVerifier = Join-Path $resolvedRepo (
    'scripts\verify_cn_historical_challenge_admission.py'
)
$historicalRoleRegistry = Join-Path $resolvedRepo (
    'runtime\run_plans\evaluation_data_roles_v1.json'
)
$historicalAccessStarted = Join-Path $resolvedRepo (
    'runtime\run_plans\cn_historical_challenge_2023_access_started.json'
)
$historicalOutcome = Join-Path $resolvedRepo (
    'runtime\run_plans\cn_fixed10_historical_challenge_2023_outcome_20260806.json'
)
$historicalAdmissionOutput = @(& $python $historicalAdmissionVerifier `
    --role-registry $historicalRoleRegistry `
    --access-started $historicalAccessStarted `
    --outcome $historicalOutcome 2>&1)
if ($LASTEXITCODE -ne 0) {
    throw (($historicalAdmissionOutput | Out-String).Trim())
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedAuthorization).Hash.ToLowerInvariant() -ne $AuthorizationSha256) {
    throw 'Historical challenge authorization hash drift'
}
$archiveHash = Get-SharedReadSha256 -LiteralPath $resolvedArchive
if (
    (Get-Item -LiteralPath $resolvedArchive).Length -ne 4008027005 -or
    $archiveHash -ne 'b05e2ca0b732821edf48a065c88b402d5c173b6a6b1266e0407bef8d9a546923'
) {
    throw 'Historical challenge archive binding drift'
}
$publicManifest = Join-Path $resolvedPublic 'source_snapshot_manifest.json'
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $publicManifest).Hash.ToLowerInvariant() -ne '0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d') {
    throw 'Public source snapshot manifest hash drift'
}
$executionHash = Get-SharedReadSha256 -LiteralPath $resolvedExecution
if ($executionHash -ne '23d756eeda5341c9a87091c3b6a420d6314acdb6399c4bd64ee046df96552ec9') {
    throw 'Execution contract hash drift'
}
$capacity = Get-Content -LiteralPath $resolvedCapacity -Raw | ConvertFrom-Json
$profileProperty = $capacity.profiles.PSObject.Properties[$NodeResourceProfile]
if ($null -eq $profileProperty) {
    throw "Node resource profile missing: $NodeResourceProfile"
}
$profile = $profileProperty.Value
if (
    [string]$profile.role -ne 'VALIDATION' -or
    [int]$profile.cpu_threads -ne $WorkerCount -or
    $ExecutorWorkerCount -gt $WorkerCount
) {
    throw 'Historical challenge resource entitlement drift'
}
$freeBytes = [int64](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
if ($freeBytes -lt [int64]24 * 1024 * 1024 * 1024) {
    throw "Free memory below 24 GiB gate: $freeBytes"
}

New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
$stdoutPath = Join-Path $resolvedRoot 'historical_challenge.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'historical_challenge.stderr.log'
$preparedRoot = Join-Path $resolvedRoot 'prepared'
$snapshotRoot = Join-Path $resolvedRoot 'input_snapshots'
$sessionizedRoot = Join-Path $resolvedRoot 'sessionized_2023'
$splitRoot = Join-Path $resolvedRoot 'split_authority'
$dailyStRoot = Join-Path $resolvedRoot 'daily_st_authority'
$fieldRoot = Join-Path $resolvedRoot 'sidecars\historical_challenge_session_fields'
$labelRoot = Join-Path $resolvedRoot 'sidecars\historical_challenge_session_labels'
$sessionRoot = Join-Path $resolvedRoot 'historical_challenge_session_authority'
$resultRoot = Join-Path $resolvedRoot 'challenge'
$registry = Join-Path $resolvedRepo (
    'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
    'unified_capability_registry.json'
)
$fundamentalRoot = (
    'D:\ChengboRemote\data\' +
    'cn_fundamental_akshare_fullA_partitioned_pit_v1_20260603'
)
$chipRoot = 'D:\ChengboRemote\data\chip_pit_v1_20260713'
foreach ($path in @($registry, $fundamentalRoot, $chipRoot)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "Historical materialization input missing: $path"
    }
}
New-Item -ItemType Directory -Path $snapshotRoot | Out-Null
$executionSnapshot = Join-Path $snapshotRoot 'replay_then_oos_execution_contract.json'
$accessSnapshot = Join-Path $snapshotRoot 'historical_challenge_access_started.json'
Copy-SharedReadFile -LiteralPath $resolvedExecution -Destination $executionSnapshot
Copy-SharedReadFile -LiteralPath $resolvedAccess -Destination $accessSnapshot

[ordered]@{
    schema_version = 'cn_fixed10_historical_challenge_2023_deployment_binding_v1'
    status = 'HISTORICAL_CHALLENGE_ACCESS_SPENT_PREPARED'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest_sha256 = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedDeployment
    ).Hash.ToLowerInvariant()
    authorization_sha256 = $AuthorizationSha256
    access_started_sha256 = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $accessSnapshot
    ).Hash.ToLowerInvariant()
    source_archive_sha256 = $archiveHash
    selection_payload_sha256 = $SelectionPayloadSha256
    decoder_policy_sha256 = $DecoderPolicySha256
    execution_contract_sha256 = $executionHash
    evaluation_role = 'historical_challenge'
    data_role = 'historical_challenge_report_only'
    node_resource_profile = $NodeResourceProfile
    worker_count = $WorkerCount
    executor_worker_count = $ExecutorWorkerCount
    sidecar_threads = $SidecarThreads
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    forward_b_reads = 0
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
$env:CN_FIXED10_RUN_MODE = 'historical_challenge'
$env:ARROW_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "validation-historical-challenge-2023-$PID"
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
    throw "Node resource lease admission failed: $LASTEXITCODE"
}
$env:CN_NODE_RESOURCE_LEASE_REQUIRED = '1'
$env:CN_NODE_RESOURCE_LEASE_RECEIPT = $leaseReceipt
$env:CN_NODE_CPU_ENTITLEMENT = [string]$WorkerCount

try {
    $ErrorActionPreference = 'Continue'
    & $python (Join-Path $resolvedRepo 'scripts\convert_cn_yearly_1min_zip_to_session_shards.py') `
        --archive $resolvedArchive `
        --expected-archive-sha256 $archiveHash `
        --year 2023 `
        --output-root $sessionizedRoot `
        --shard-count 12 `
        --worker-count $ExecutorWorkerCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "2023 archive conversion failed: $LASTEXITCODE" }

    & $python (Join-Path $resolvedRepo 'scripts\build_cn_historical_challenge_split_manifest.py') `
        --sessionized-root $sessionizedRoot `
        --output-root $splitRoot *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Historical split freeze failed: $LASTEXITCODE" }
    $splitReceipt = Get-Content -LiteralPath (
        Join-Path $splitRoot 'HISTORICAL_CHALLENGE_SPLIT_FROZEN.json'
    ) -Raw | ConvertFrom-Json
    $split = Join-Path $splitRoot 'historical_challenge_split_manifest.csv'
    $splitHash = [string]$splitReceipt.split_manifest_sha256

    & $python (Join-Path $resolvedRepo 'scripts\build_cn_historical_daily_st_source.py') `
        --input-root $resolvedDailyStInput `
        --year 2023 `
        --output-root $dailyStRoot `
        --worker-count $ExecutorWorkerCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Historical daily ST build failed: $LASTEXITCODE" }
    $dailySt = Join-Path $dailyStRoot 'historical_daily_st_2023.parquet'
    $dailyStHash = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $dailySt
    ).Hash.ToLowerInvariant()

    & $python (Join-Path $resolvedRepo 'scripts\prepare_cn_fixed_survivor_forward_2026.py') `
        --confirmation-root $resolvedConfirmation `
        --authorization-path $resolvedAuthorization `
        --output-root $preparedRoot `
        --expected-authorization-sha256 $AuthorizationSha256 `
        --expected-selection-payload-sha256 $SelectionPayloadSha256 `
        --expected-forward-split-sha256 $splitHash `
        --builder-commit-sha $RepoSha *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Historical fixed-ten preparation failed: $LASTEXITCODE" }

    $env:NUMBA_NUM_THREADS = '1'
    $env:POLARS_MAX_THREADS = [string]$SidecarThreads
    & $python (Join-Path $resolvedRepo 'scripts\build_cn_core_pack_validation_session_sidecar.py') `
        --source-root $sessionizedRoot `
        --evaluation-role historical_challenge `
        --output-root $fieldRoot `
        --candidate-table (Join-Path $preparedRoot 'confirmation_candidates.csv') `
        --registry $registry `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --fundamental-root $fundamentalRoot `
        --chip-root $chipRoot `
        --max-shards 12 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Historical field sidecar failed: $LASTEXITCODE" }

    & $python (Join-Path $resolvedRepo 'scripts\build_cn_phase3cm_forward_label_sidecars.py') `
        --source-root $fieldRoot `
        --evaluation-role historical_challenge `
        --output-root $labelRoot `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --horizons 1,5,15,30 `
        --max-shards 12 `
        --polars-threads $SidecarThreads *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Historical label sidecar failed: $LASTEXITCODE" }

    $fieldManifest = Join-Path $fieldRoot 'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
    $fieldManifestHash = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $fieldManifest
    ).Hash.ToLowerInvariant()
    $env:POLARS_MAX_THREADS = '1'
    & $python (Join-Path $resolvedRepo 'scripts\build_cn_validation_session_authority.py') `
        --field-manifest $fieldManifest `
        --public-source-root $resolvedPublic `
        --output-root $sessionRoot `
        --expected-field-manifest-sha256 $fieldManifestHash `
        --expected-source-manifest-sha256 '0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d' `
        --historical-daily-st-source $dailySt `
        --expected-daily-st-source-sha256 $dailyStHash `
        --builder-commit-sha $RepoSha `
        --evaluation-role historical_challenge `
        --date-min ([string]$splitReceipt.date_min) `
        --date-max ([string]$splitReceipt.date_max) `
        --schema-version cn_historical_challenge_session_authority_v1 `
        --status HISTORICAL_CHALLENGE_SESSION_AUTHORITY_COMPLETE `
        --evidence-scope ONE_SHOT_BACKWARD_OOT_2023_FIXED10_REPORT_ONLY *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Historical session authority failed: $LASTEXITCODE" }

    $env:NUMBA_NUM_THREADS = '1'
    $env:POLARS_MAX_THREADS = '1'
    & $python (Join-Path $resolvedRepo 'scripts\run_cn_fixed_survivor_forward_2026.py') `
        --confirmation-root $resolvedConfirmation `
        --authorization-path $resolvedAuthorization `
        --expected-authorization-sha256 $AuthorizationSha256 `
        --execution-contract-path $executionSnapshot `
        --expected-execution-contract-sha256 $executionHash `
        --field-root $fieldRoot `
        --label-root $labelRoot `
        --session-authority-root $sessionRoot `
        --output-root $resultRoot `
        --builder-commit-sha $RepoSha `
        --expected-selection-payload-sha256 $SelectionPayloadSha256 `
        --expected-decoder-policy-sha256 $DecoderPolicySha256 `
        --expected-forward-split-sha256 $splitHash `
        --worker-count $WorkerCount `
        --executor-worker-count $ExecutorWorkerCount *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) { throw "Fixed-ten historical challenge failed: $LASTEXITCODE" }
    if (-not (Test-Path -LiteralPath (
        Join-Path $resultRoot 'HISTORICAL_CHALLENGE_2023_COMPLETE.json'
    ))) {
        throw 'Fixed-ten historical challenge did not close'
    }
    $ErrorActionPreference = 'Stop'
} catch {
    $_ | Out-String | Add-Content -LiteralPath $stderrPath -Encoding UTF8
    [ordered]@{
        repo_sha = $RepoSha
        status = 'FAILED'
        completed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        error = $_.Exception.Message
        historical_challenge_asset_state = 'SPENT'
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
    historical_challenge_asset_state = 'SPENT'
} | ConvertTo-Json | Set-Content -LiteralPath (
    Join-Path $resolvedRoot 'process_exit.json'
) -Encoding UTF8
