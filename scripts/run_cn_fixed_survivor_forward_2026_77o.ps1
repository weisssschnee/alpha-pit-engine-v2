param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$ConfirmationRoot,
    [Parameter(Mandatory = $true)][string]$AuthorizationPath,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{64}$')]
    [string]$AuthorizationSha256,
    [Parameter(Mandatory = $true)][string]$ForwardMinuteRoot,
    [Parameter(Mandatory = $true)][string]$ForwardDailyStSource,
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
        $LiteralPath,
        [IO.FileMode]::Open,
        [IO.FileAccess]::Read,
        $share
    )
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        return ([BitConverter]::ToString(
            $sha.ComputeHash($stream)
        ) -replace '-', '').ToLowerInvariant()
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
        $LiteralPath,
        [IO.FileMode]::Open,
        [IO.FileAccess]::Read,
        $share
    )
    $target = [IO.File]::Open(
        $Destination,
        [IO.FileMode]::CreateNew,
        [IO.FileAccess]::Write,
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
    throw '2026 forward confirmation is authorized only on DESKTOP-77OPJ6F'
}
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$resolvedRepo = (Resolve-Path -LiteralPath $Repo).Path
$resolvedDeployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$resolvedConfirmation = (Resolve-Path -LiteralPath $ConfirmationRoot).Path
$resolvedAuthorization = (Resolve-Path -LiteralPath $AuthorizationPath).Path
$resolvedMinute = (Resolve-Path -LiteralPath $ForwardMinuteRoot).Path
$resolvedDailySt = (Resolve-Path -LiteralPath $ForwardDailyStSource).Path
$resolvedPublic = (Resolve-Path -LiteralPath $PublicSourceRoot).Path
$resolvedExecution = (Resolve-Path -LiteralPath $ExecutionContractPath).Path
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
    'D:\ChengboRemote\runtime\cn_fixed10_forward_2026_confirmation_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected forward output root: $resolvedRoot"
}
if (Test-Path -LiteralPath $resolvedRoot) {
    throw "forward output root must be fresh: $resolvedRoot"
}
foreach ($path in @(
    $python, $git, $resolvedDeployment, $resolvedConfirmation,
    $resolvedAuthorization, $resolvedMinute, $resolvedDailySt,
    $resolvedPublic, $resolvedExecution, $resolvedCapacity
)) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required input missing: $path"
    }
}
$safe = "safe.directory=$($resolvedRepo.Replace('\', '/'))"
$observedSha = (& $git -c $safe -C $resolvedRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $observedSha -ne $RepoSha) {
    throw "deployed repo SHA drift: expected=$RepoSha observed=$observedSha"
}
$dirty = @(& $git -c $safe -C $resolvedRepo status --porcelain)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw 'deployed forward workspace must be clean'
}
$deployment = Get-Content -LiteralPath $resolvedDeployment -Raw |
    ConvertFrom-Json
if (
    [string]$deployment.repo_sha -ne $RepoSha -or
    [string]$deployment.workspace -ne $resolvedRepo
) {
    throw 'forward deployment manifest binding drift'
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedAuthorization).Hash.ToLowerInvariant() -ne $AuthorizationSha256) {
    throw 'forward authorization hash drift'
}
$split = Join-Path $resolvedRepo (
    'runtime\run_plans\cn_fixed10_forward_2026_split_manifest.csv'
)
$splitHash = (
    Get-FileHash -Algorithm SHA256 -LiteralPath $split
).Hash.ToLowerInvariant()
if ($splitHash -ne '200290efbee38001c2e9087fa332a11d28ff6c676ac44a1fd3b8fbd392283dea') {
    throw 'forward split hash drift'
}
$sourceFiles = @(Get-ChildItem -LiteralPath $resolvedMinute -File -Filter '*.parquet' | Sort-Object Name)
$splitRows = @(Import-Csv -LiteralPath $split)
$sourceDates = @($sourceFiles | ForEach-Object { $_.BaseName.Replace('date=', '') })
$splitDates = @($splitRows | ForEach-Object { ([datetime]$_.trade_date).ToString('yyyyMMdd') })
if (
    $sourceFiles.Count -ne 63 -or
    $splitRows.Count -ne 63 -or
    (Compare-Object $sourceDates $splitDates).Count -ne 0 -or
    [int64](($sourceFiles | Measure-Object Length -Sum).Sum) -ne 1517219879
) {
    throw 'forward source metadata/calendar drift before financial read'
}
if ($sourceDates[0] -ne '20260105' -or $sourceDates[-1] -ne '20260410') {
    throw 'forward source boundary drift'
}
$dailyStHash = (
    Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedDailySt
).Hash.ToLowerInvariant()
if ($dailyStHash -ne '611769438f2d018ad1e3e2e6e0c23e3cde9593193a17b4cb113f08b6b45fa173') {
    throw 'forward daily ST source hash drift'
}
$publicManifest = Join-Path $resolvedPublic 'source_snapshot_manifest.json'
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $publicManifest).Hash.ToLowerInvariant() -ne '0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d') {
    throw 'public source snapshot manifest hash drift'
}
$executionHash = Get-SharedReadSha256 -LiteralPath $resolvedExecution
if ($executionHash -ne '23d756eeda5341c9a87091c3b6a420d6314acdb6399c4bd64ee046df96552ec9') {
    throw 'execution contract hash drift'
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
    throw 'forward resource entitlement drift'
}
$freeBytes = [int64](Get-CimInstance Win32_OperatingSystem).FreePhysicalMemory * 1024
if ($freeBytes -lt [int64]24 * 1024 * 1024 * 1024) {
    throw "free memory below 24 GiB gate: $freeBytes"
}

New-Item -ItemType Directory -Path $resolvedRoot | Out-Null
$stdoutPath = Join-Path $resolvedRoot 'forward.stdout.log'
$stderrPath = Join-Path $resolvedRoot 'forward.stderr.log'
$preparedRoot = Join-Path $resolvedRoot 'prepared'
$inputSnapshotRoot = Join-Path $resolvedRoot 'input_snapshots'
$fieldRoot = Join-Path $resolvedRoot 'sidecars\forward_2026_session_fields'
$labelRoot = Join-Path $resolvedRoot 'sidecars\forward_2026_session_labels'
$sessionRoot = Join-Path $resolvedRoot 'forward_2026_session_authority'
$resultRoot = Join-Path $resolvedRoot 'confirmation'
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
        throw "forward materialization input missing: $path"
    }
}
New-Item -ItemType Directory -Path $inputSnapshotRoot | Out-Null
$executionSnapshot = Join-Path $inputSnapshotRoot 'replay_then_oos_execution_contract.json'
Copy-SharedReadFile -LiteralPath $resolvedExecution -Destination $executionSnapshot
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $executionSnapshot).Hash.ToLowerInvariant() -ne $executionHash) {
    throw 'execution contract snapshot hash drift'
}
[ordered]@{
    schema_version = 'cn_fixed10_forward_2026_deployment_binding_v1'
    status = 'AUTHORIZED_ZERO_FINANCIAL_PREFLIGHT_COMPLETE'
    repo_sha = $RepoSha
    workspace = $resolvedRepo
    deployment_manifest_sha256 = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $resolvedDeployment
    ).Hash.ToLowerInvariant()
    authorization_sha256 = $AuthorizationSha256
    selection_payload_sha256 = $SelectionPayloadSha256
    decoder_policy_sha256 = $DecoderPolicySha256
    pair_count = 10
    candidate_member_count = 20
    forward_trade_date_count = 63
    forward_source_file_count = 63
    forward_source_bytes = 1517219879
    forward_split_manifest_sha256 = $splitHash
    execution_contract_sha256 = $executionHash
    evaluation_role = 'forward_2026'
    data_role = 'forward_2026_report_only'
    node_resource_profile = $NodeResourceProfile
    worker_count = $WorkerCount
    executor_worker_count = $ExecutorWorkerCount
    sidecar_threads = $SidecarThreads
    minimum_free_memory_bytes = [int64]24 * 1024 * 1024 * 1024
    preflight_validation_reads = 0
    preflight_holdout_reads = 0
    preflight_forward_financial_rows_read = 0
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
$env:ARROW_NUM_THREADS = '1'
$env:OMP_NUM_THREADS = '1'
$env:MKL_NUM_THREADS = '1'
$env:OPENBLAS_NUM_THREADS = '1'
$env:NUMEXPR_NUM_THREADS = '1'
$env:NUMEXPR_MAX_THREADS = '1'
$env:JOBLIB_MULTIPROCESSING = '0'
$leaseManager = Join-Path $resolvedRepo 'scripts\manage_cn_node_resource_lease.py'
$leaseId = "validation-forward-2026-$PID"
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
    & $python (Join-Path $resolvedRepo 'scripts\prepare_cn_fixed_survivor_forward_2026.py') `
        --confirmation-root $resolvedConfirmation `
        --authorization-path $resolvedAuthorization `
        --output-root $preparedRoot `
        --expected-authorization-sha256 $AuthorizationSha256 `
        --expected-selection-payload-sha256 $SelectionPayloadSha256 `
        --expected-forward-split-sha256 $splitHash `
        --builder-commit-sha $RepoSha *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "forward zero-read preparation failed: $LASTEXITCODE"
    }
    [ordered]@{
        schema_version = 'cn_forward_2026_access_transition_v1'
        status = 'FORWARD_2026_SPENT_ON_FIRST_FINANCIAL_READ'
        transition_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        authorization_sha256 = $AuthorizationSha256
        source_root = $resolvedMinute
        validation_reads = 0
        holdout_reads = 0
        optimizer_feedback_write = 'FORBIDDEN'
        scheduler_write = 'FORBIDDEN'
        archive_write = 'FORBIDDEN'
        promotion = 'FORBIDDEN'
    } | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (
        Join-Path $resolvedRoot 'forward_access_started.json'
    ) -Encoding UTF8

    $env:NUMBA_NUM_THREADS = '1'
    $env:POLARS_MAX_THREADS = [string]$SidecarThreads
    & $python (Join-Path $resolvedRepo 'scripts\build_cn_core_pack_validation_session_sidecar.py') `
        --source-root $resolvedMinute `
        --evaluation-role forward_2026 `
        --output-root $fieldRoot `
        --candidate-table (Join-Path $preparedRoot 'confirmation_candidates.csv') `
        --registry $registry `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --fundamental-root $fundamentalRoot `
        --chip-root $chipRoot `
        --max-shards 63 *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "forward field sidecar build failed: $LASTEXITCODE"
    }
    & $python (Join-Path $resolvedRepo 'scripts\build_cn_phase3cm_forward_label_sidecars.py') `
        --source-root $fieldRoot `
        --evaluation-role forward_2026 `
        --output-root $labelRoot `
        --split-manifest $split `
        --split-manifest-hash $splitHash `
        --horizons 1,5,15,30 `
        --max-shards 63 `
        --polars-threads $SidecarThreads *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "forward label sidecar build failed: $LASTEXITCODE"
    }
    $fieldManifest = Join-Path $fieldRoot 'CN_DEVELOPMENT_TIME_MAJOR_EXECUTION_LAYOUT_V2.json'
    $fieldManifestHash = (
        Get-FileHash -Algorithm SHA256 -LiteralPath $fieldManifest
    ).Hash.ToLowerInvariant()
    $env:POLARS_MAX_THREADS = '1'
    & $python (Join-Path $resolvedRepo 'scripts\build_cn_forward_2026_session_authority.py') `
        --field-manifest $fieldManifest `
        --public-source-root $resolvedPublic `
        --output-root $sessionRoot `
        --expected-field-manifest-sha256 $fieldManifestHash `
        --expected-source-manifest-sha256 '0c06baa7c24109ea8a1064a291565ce6352ab2a0ca32f20f6cc257261e25514d' `
        --historical-daily-st-source $resolvedDailySt `
        --expected-daily-st-source-sha256 $dailyStHash `
        --builder-commit-sha $RepoSha *>> $stdoutPath
    if ($LASTEXITCODE -ne 0) {
        throw "forward session authority build failed: $LASTEXITCODE"
    }
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
    if ($LASTEXITCODE -ne 0) {
        throw "fixed10 forward confirmation failed: $LASTEXITCODE"
    }
    if (-not (Test-Path -LiteralPath (Join-Path $resultRoot 'FORWARD_2026_COMPLETE.json'))) {
        throw 'fixed10 forward confirmation did not close'
    }
    $ErrorActionPreference = 'Stop'
} catch {
    $_ | Out-String | Add-Content -LiteralPath $stderrPath -Encoding UTF8
    [ordered]@{
        repo_sha = $RepoSha
        status = 'FAILED'
        completed_at_utc = (Get-Date).ToUniversalTime().ToString('o')
        error = $_.Exception.Message
        forward_asset_state = if (Test-Path -LiteralPath (
            Join-Path $resolvedRoot 'forward_access_started.json'
        )) { 'SPENT' } else { 'UNOPENED' }
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
    forward_asset_state = 'SPENT'
} | ConvertTo-Json | Set-Content -LiteralPath (
    Join-Path $resolvedRoot 'process_exit.json'
) -Encoding UTF8
