param(
    [Parameter(Mandatory = $true)][string]$PartitionContract,
    [Parameter(Mandatory = $true)][string]$CapacityReceipt0,
    [Parameter(Mandatory = $true)][string]$CapacityReceipt1,
    [Parameter(Mandatory = $true)][string]$SourceClosureManifest,
    [Parameter(Mandatory = $true)][string]$OutputName,
    [double]$WallSecondsHardMax = 0.0,
    [switch]$Resume,
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][string]$CandidateRoot,
    [Parameter(Mandatory = $true)][string]$RunParentRoot,
    [Parameter(Mandatory = $true)][string]$Binding,
    [Parameter(Mandatory = $true)][string]$FieldRoot,
    [Parameter(Mandatory = $true)][string]$LabelRoot,
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
if ($WallSecondsHardMax -lt 0.0) { throw "wall-seconds hard maximum cannot be negative" }
$Contract = Get-Content -LiteralPath $PartitionContract -Raw | ConvertFrom-Json
if ($Contract.status -ne "CN_PHASE3CM_BACKEND_PARTITIONS_FROZEN") {
    throw "partition contract status drift"
}
if ([int]$Contract.partition_count -ne 2 -or [int]$Contract.heavy_processes -ne 2) {
    throw "partition launcher requires exactly two heavy processes"
}
if ([int]$Contract.global_active_native_compute_threads -gt 24) {
    throw "global native compute thread budget exceeds 24"
}
if ($Contract.data_role -ne "development_train_only" -or
    $Contract.promotion -ne "FORBIDDEN" -or
    [int]$Contract.sealed_reads.validation -ne 0 -or
    [int]$Contract.sealed_reads.holdout -ne 0 -or
    [int]$Contract.sealed_reads.forward_2026 -ne 0) {
    throw "partition research boundary drift"
}
$ObservedRepoSha = [string]$Contract.repo_sha
if ($ObservedRepoSha -notmatch '^[0-9a-f]{40}$') {
    throw "partition repo SHA is not exact"
}
$ContractBindingPath = [string]$Contract.binding.path
if (-not (Test-Path -LiteralPath $Binding) -or -not (Test-Path -LiteralPath $ContractBindingPath)) {
    throw "partition binding artifact is missing"
}
$BindingResolved = (Resolve-Path -LiteralPath $Binding).Path
$ContractBindingResolved = (Resolve-Path -LiteralPath $ContractBindingPath).Path
if (-not [string]::Equals($BindingResolved, $ContractBindingResolved, [StringComparison]::OrdinalIgnoreCase)) {
    throw "launcher binding path differs from frozen partition contract"
}
$ObservedBindingSha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $BindingResolved).Hash.ToLowerInvariant()
if ($ObservedBindingSha256 -ne [string]$Contract.binding.sha256) {
    throw "launcher binding content differs from frozen partition contract"
}

$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = "1"
. (Join-Path $RepoRoot "scripts\cn_phase3cm_process_tree_monitor.ps1")

function Test-CnZeroAccessEvidence {
    param([object]$Payload)
    if ($null -eq $Payload) { return $false }
    $IntegerTypes = @(
        [byte], [sbyte], [int16], [uint16], [int32], [uint32], [int64], [uint64]
    )
    foreach ($Name in @("validation_reads", "holdout_reads", "forward_2026_reads")) {
        $Property = $Payload.PSObject.Properties[$Name]
        if ($null -eq $Property -or $null -eq $Property.Value) { return $false }
        $Value = $Property.Value
        if (-not ($IntegerTypes -contains $Value.GetType())) { return $false }
        if ([int64]$Value -ne 0) { return $false }
    }
    return $true
}

$ContractValidator = Join-Path $RepoRoot "scripts\freeze_cn_phase3cm_backend_partitions.py"
$ContractValidationOutput = @(& $PythonExe $ContractValidator `
    --validate-contract $PartitionContract `
    --expected-repo-sha $ObservedRepoSha `
    --source-closure-manifest $SourceClosureManifest)
if ($LASTEXITCODE -ne 0 -or $ContractValidationOutput.Count -eq 0) {
    throw "partition contract validation failed"
}
$ContractValidation = $ContractValidationOutput[-1] | ConvertFrom-Json
if ($ContractValidation.status -ne "CN_PHASE3CM_BACKEND_PARTITION_CONTRACT_VALIDATED") {
    throw "partition contract validation status drift"
}

$CapacityReceiptPaths = @($CapacityReceipt0, $CapacityReceipt1)
$CapacityValidations = @()
$CapacityValidator = Join-Path $RepoRoot "scripts\preflight_cn_phase3cm_dag_cache.py"
for ($Index = 0; $Index -lt 2; $Index += 1) {
    $Partition = $Contract.partitions[$Index]
    $ValidationOutput = @(& $PythonExe $CapacityValidator `
        --validate-receipt $CapacityReceiptPaths[$Index] `
        --candidate-table ([string]$Partition.candidate_table) `
        --execution-plan ([string]$Partition.execution_plan) `
        --backend ([string]$Contract.logical_backend) `
        --field-sidecar-root $FieldRoot `
        --label-sidecar-root $LabelRoot `
        --repo-root $RepoRoot `
        --source-closure-manifest $SourceClosureManifest `
        --expected-repo-sha $ObservedRepoSha)
    if ($LASTEXITCODE -ne 0 -or $ValidationOutput.Count -eq 0) {
        throw "capacity receipt validation failed for partition $Index"
    }
    $Validation = $ValidationOutput[-1] | ConvertFrom-Json
    if ($Validation.status -ne "CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH") {
        throw "capacity receipt status drift for partition $Index"
    }
    if ([string]$Validation.execution_plan_hash -ne [string]$Partition.execution_plan_hash) {
        throw "capacity receipt plan identity drift for partition $Index"
    }
    $CapacityValidations += $Validation
}

$ObservedSourceClosureManifestSha256 = (
    Get-FileHash -Algorithm SHA256 -LiteralPath $SourceClosureManifest
).Hash.ToLowerInvariant()
$FrozenSourceClosure = $Contract.source_closure_manifest
if ($ObservedSourceClosureManifestSha256 -ne [string]$FrozenSourceClosure.sha256) {
    throw "source closure manifest differs from frozen partition contract"
}
foreach ($Validation in $CapacityValidations) {
    if ([string]$Validation.source_closure_manifest_sha256 -ne $ObservedSourceClosureManifestSha256 -or
        [string]$Validation.source_closure_manifest_hash -ne [string]$FrozenSourceClosure.manifest_hash -or
        [string]$Validation.source_closure_hash -ne [string]$FrozenSourceClosure.source_closure_hash) {
        throw "capacity receipt source closure differs from frozen partition contract"
    }
}
if ([string]$CapacityValidations[0].source_closure_manifest_hash -ne
    [string]$CapacityValidations[1].source_closure_manifest_hash) {
    throw "capacity receipts do not bind the same source closure manifest"
}

# No output directory or heavy process exists before all frozen gates above pass.
$RunRoot = Join-Path $RunParentRoot $OutputName
if ((Test-Path $RunRoot) -and -not $Resume) {
    throw "partition output already exists; refuse to overwrite: $RunRoot"
}
if ($Resume -and -not (Test-Path $RunRoot)) {
    throw "partition resume requested without an existing output root: $RunRoot"
}
New-Item -ItemType Directory -Force -Path $RunRoot | Out-Null
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$Wrapper = Join-Path $RepoRoot "scripts\invoke_cn_phase3cm_backend_with_exit_receipt.ps1"
$PowerShellExe = (Get-Command powershell.exe).Source
$Processes = @()
$Roots = @{}
$PartitionNames = @()
$OutputRoots = @()
$ExitReceiptPaths = @()
$ResultPaths = @()
$CommandPaths = @()
$CommandHashes = @()

for ($Index = 0; $Index -lt 2; $Index += 1) {
    $Partition = $Contract.partitions[$Index]
    $Plan = Get-Content -LiteralPath ([string]$Partition.execution_plan) -Raw | ConvertFrom-Json
    $PairBatchSize = [int](($Plan.pair_batches | ForEach-Object { @($_).Count } | Measure-Object -Maximum).Maximum)
    $PartitionRoot = Join-Path $RunRoot ("partition_{0:D2}" -f $Index)
    New-Item -ItemType Directory -Force -Path $PartitionRoot | Out-Null
    $OutputRoots += $PartitionRoot
    $ExitReceiptPath = Join-Path $PartitionRoot "CN_BACKEND_EXIT_RECEIPT.json"
    $ResultPath = Join-Path $PartitionRoot "CN_STREAMING_BACKEND_RESULT.json"
    $ExitReceiptPaths += $ExitReceiptPath
    $ResultPaths += $ResultPath
    # Resume retains only periodic checkpoints.  Final artifacts and exit receipts
    # belong to the new process attempt and must never be reusable as stale success.
    foreach ($StaleArtifact in @($ExitReceiptPath, $ResultPath)) {
        if (Test-Path -LiteralPath $StaleArtifact) {
            Remove-Item -LiteralPath $StaleArtifact -Force
        }
    }
    $Arguments = @(
        "scripts\run_cn_phase3cm_streaming_qualification.py",
        "--backend", [string]$Contract.logical_backend,
        "--phase", "E",
        "--pair-count", [string]$Partition.pair_count,
        "--candidate-table", [string]$Partition.candidate_table,
        "--binding", $Binding,
        "--split-manifest", $SplitManifest,
        "--artifact-root", $CandidateRoot,
        "--field-sidecar-root", $FieldRoot,
        "--label-sidecar-root", $LabelRoot,
        "--output-root", $PartitionRoot,
        "--execution-plan", [string]$Partition.execution_plan,
        "--block-sessions", [string]$Plan.block_size,
        "--pair-batch-size", [string]$PairBatchSize,
        "--compute-threads", [string]$Partition.compute_threads,
        "--max-block-rows", [string]$CapacityValidations[$Index].max_block_rows,
        "--capacity-receipt-hash", [string]$CapacityValidations[$Index].receipt_hash
    )
    if ($Resume) { $Arguments += "--resume" }
    $ThreadEnvironment = [ordered]@{
        NUMBA_NUM_THREADS = [string]$Partition.compute_threads
        ARROW_NUM_THREADS = "1"
        OMP_NUM_THREADS = "1"
        MKL_NUM_THREADS = "1"
        OPENBLAS_NUM_THREADS = "1"
        NUMEXPR_MAX_THREADS = "1"
        POLARS_MAX_THREADS = "1"
    }
    $CommandPath = Join-Path $PartitionRoot "CN_BACKEND_COMMAND.json"
    Write-CnAtomicJson -Path $CommandPath -Payload ([ordered]@{
        schema_version = "cn_phase3cm_backend_command_v1"
        partition_id = [string]$Partition.partition_id
        backend = [string]$Contract.logical_backend
        arguments = $Arguments
        thread_environment = $ThreadEnvironment
    })
    $PartitionNames += [string]$Partition.partition_id
    $CommandPaths += $CommandPath
    $CommandHashes += (Get-FileHash -Algorithm SHA256 -LiteralPath $CommandPath).Hash.ToLowerInvariant()
}

$RssTimelinePath = Join-Path $RunRoot "CN_GLOBAL_PROCESS_TREE_RSS_TIMELINE.csv"
"sampled_at,partition_00_process_ids,partition_00_rss_bytes,partition_01_process_ids,partition_01_rss_bytes,global_rss_bytes" | Set-Content -LiteralPath $RssTimelinePath -Encoding UTF8
$GlobalPeakRss = [int64]0
$GlobalHardRss = [int64]($Contract.partitions | ForEach-Object {
    $Plan = Get-Content -LiteralPath ([string]$_.execution_plan) -Raw | ConvertFrom-Json
    [int64]$Plan.global_rss_hard_bytes
} | Measure-Object -Minimum).Minimum
$GlobalGateFailure = $false
$WallGateFailure = $false
$LaunchFailure = $null
$RssSampleCount = 0
$PartitionPeaks = @{}
foreach ($Name in $PartitionNames) { $PartitionPeaks[$Name] = [int64]0 }
$Stopwatch = [Diagnostics.Stopwatch]::StartNew()
try {
    for ($Index = 0; $Index -lt 2; $Index += 1) {
        $Partition = $Contract.partitions[$Index]
        $PartitionRoot = $OutputRoots[$Index]
        $Process = Start-Process -FilePath $PowerShellExe -ArgumentList @(
            "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
            "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
            "-ArgumentFile", $CommandPaths[$Index], "-ExitReceipt", $ExitReceiptPaths[$Index]
        ) -WorkingDirectory $RepoRoot `
            -RedirectStandardOutput (Join-Path $PartitionRoot "stdout.log") `
            -RedirectStandardError (Join-Path $PartitionRoot "stderr.log") `
            -WindowStyle Hidden -PassThru
        $Processes += $Process
        $Roots[[string]$Partition.partition_id] = $Process.Id
    }

    while ($true) {
        $Running = @()
        foreach ($Process in $Processes) {
            $Process.Refresh()
            if (-not $Process.HasExited) { $Running += $Process }
        }
        $Snapshot = Get-CnProcessTreeRssSnapshot -Roots $Roots
        $TimelineLine = @(
            $Snapshot.sampled_at,
            ($Snapshot.groups[$PartitionNames[0]].live_process_ids -join "|"),
            [string]$Snapshot.groups[$PartitionNames[0]].rss_bytes,
            ($Snapshot.groups[$PartitionNames[1]].live_process_ids -join "|"),
            [string]$Snapshot.groups[$PartitionNames[1]].rss_bytes,
            [string]$Snapshot.total_rss_bytes
        ) -join ","
        Add-Content -LiteralPath $RssTimelinePath -Value $TimelineLine -Encoding UTF8
        $RssSampleCount += 1
        $CurrentGlobalRss = [int64]$Snapshot.total_rss_bytes
        $GlobalPeakRss = [Math]::Max($GlobalPeakRss, $CurrentGlobalRss)
        foreach ($Name in $Roots.Keys) {
            $PartitionPeaks[$Name] = [Math]::Max(
                [int64]$PartitionPeaks[$Name],
                [int64]$Snapshot.groups[$Name].rss_bytes
            )
        }
        if ($CurrentGlobalRss -ge $GlobalHardRss) {
            $GlobalGateFailure = $true
            Stop-CnProcessTrees -Roots $Roots
            break
        }
        if ($WallSecondsHardMax -gt 0.0 -and $Stopwatch.Elapsed.TotalSeconds -ge $WallSecondsHardMax) {
            $WallGateFailure = $true
            Stop-CnProcessTrees -Roots $Roots
            break
        }
        if ($Running.Count -eq 0) { break }
        Start-Sleep -Seconds 2
    }
}
catch {
    $LaunchFailure = $_.Exception.Message
    if ($Roots.Count -gt 0) { Stop-CnProcessTrees -Roots $Roots }
}
finally {
    foreach ($Process in $Processes) {
        try { $Process.WaitForExit(); $Process.Refresh() } catch { }
    }
    $Stopwatch.Stop()
}

$ExitCodes = @()
$Results = @()
$PartitionEvidence = @()
$Pass = (-not $GlobalGateFailure -and -not $WallGateFailure -and $null -eq $LaunchFailure)
$AccessEvidenceComplete = $true
$ValidationReads = 0
$HoldoutReads = 0
$Forward2026Reads = 0
for ($Index = 0; $Index -lt 2; $Index += 1) {
    $Partition = $Contract.partitions[$Index]
    $ExitReceipt = if (Test-Path $ExitReceiptPaths[$Index]) {
        Get-Content -LiteralPath $ExitReceiptPaths[$Index] -Raw | ConvertFrom-Json
    } else { $null }
    $ActualExitCode = if ($Index -lt $Processes.Count -and $Processes[$Index].HasExited) {
        [int]$Processes[$Index].ExitCode
    } else { -1 }
    $ExitCode = if ($null -ne $ExitReceipt) { [int]$ExitReceipt.exit_code } else { -1 }
    $ExitCodes += $ExitCode
    $Result = if (Test-Path $ResultPaths[$Index]) {
        Get-Content -LiteralPath $ResultPaths[$Index] -Raw | ConvertFrom-Json
    } else { $null }
    $Results += $Result
    $ResultAccessEvidenceComplete = Test-CnZeroAccessEvidence -Payload $Result
    if (-not $ResultAccessEvidenceComplete) {
        $AccessEvidenceComplete = $false
    } else {
        $ValidationReads += [int]$Result.validation_reads
        $HoldoutReads += [int]$Result.holdout_reads
        $Forward2026Reads += [int]$Result.forward_2026_reads
    }
    $ExpectedPairIds = @($Partition.pair_ids | ForEach-Object { [string]$_ } | Sort-Object)
    $ObservedPairIds = if ($null -ne $Result) {
        @($Result.pair_results | ForEach-Object { [string]$_.pair_id } | Sort-Object)
    } else { @() }
    $PairIdentityPass = (
        $ExpectedPairIds.Count -eq $ObservedPairIds.Count -and
        (Compare-Object $ExpectedPairIds $ObservedPairIds).Count -eq 0
    )
    $ExitReceiptPass = (
        $null -ne $ExitReceipt -and
        $ExitReceipt.schema_version -eq "cn_phase3cm_backend_exit_receipt_v1" -and
        $ExitReceipt.status -eq "CN_PHASE3CM_BACKEND_PROCESS_COMPLETED" -and
        $ActualExitCode -eq 0 -and
        $ExitCode -eq $ActualExitCode -and
        [string]$ExitReceipt.argument_file_sha256 -eq [string]$CommandHashes[$Index]
    )
    $PartitionPass = (
        $ExitReceiptPass -and
        $null -ne $Result -and
        $Result.status -eq "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED" -and
        $Result.backend -eq [string]$Contract.logical_backend -and
        $Result.phase -eq "E" -and
        [int]$Result.pair_count -eq [int]$Partition.pair_count -and
        [int]$Result.candidate_count -eq (2 * [int]$Partition.pair_count) -and
        [string]$Result.execution_plan_hash -eq [string]$Partition.execution_plan_hash -and
        [string]$Result.capacity_receipt_hash -eq [string]$CapacityValidations[$Index].receipt_hash -and
        $Result.block_row_guard_status -eq "BLOCK_ROW_GUARD_PASS" -and
        [int64]$Result.max_block_rows_contract -eq [int64]$CapacityValidations[$Index].max_block_rows -and
        [int64]$Result.max_observed_block_rows -le [int64]$CapacityValidations[$Index].max_block_rows -and
        $Result.parallelism_status -eq "PARALLELISM_ENGAGED" -and
        [int]$Result.coordinate_rows_retained -eq 0 -and
        $ResultAccessEvidenceComplete -and
        [int]$Result.validation_reads -eq 0 -and
        [int]$Result.holdout_reads -eq 0 -and
        [int]$Result.forward_2026_reads -eq 0 -and
        [string]$Result.input_binding_hash -eq [string]$Contract.binding.binding_hash -and
        $PairIdentityPass
    )
    if (-not $PartitionPass) { $Pass = $false }
    $PartitionEvidence += [ordered]@{
        partition_id = [string]$Partition.partition_id
        pass = $PartitionPass
        process_exit_code = $ActualExitCode
        exit_receipt_code = $ExitCode
        exit_receipt_pass = $ExitReceiptPass
        command_sha256 = [string]$CommandHashes[$Index]
        pair_count = [int]$Partition.pair_count
        pair_identity_pass = $PairIdentityPass
        access_evidence_complete = $ResultAccessEvidenceComplete
        peak_process_tree_rss_bytes = [int64]$PartitionPeaks[[string]$Partition.partition_id]
        capacity_receipt = $CapacityReceiptPaths[$Index]
        capacity_receipt_hash = [string]$CapacityValidations[$Index].receipt_hash
        backend_result = $ResultPaths[$Index]
        backend_result_sha256 = if (Test-Path $ResultPaths[$Index]) {
            (Get-FileHash -Algorithm SHA256 -LiteralPath $ResultPaths[$Index]).Hash.ToLowerInvariant()
        } else { $null }
    }
}
$WallGatePass = (-not $WallGateFailure -and ($WallSecondsHardMax -le 0.0 -or $Stopwatch.Elapsed.TotalSeconds -le $WallSecondsHardMax))
if (-not $WallGatePass -or $GlobalPeakRss -ge $GlobalHardRss -or -not $AccessEvidenceComplete) {
    $Pass = $false
}
$Receipt = [ordered]@{
    schema_version = "cn_phase3cm_partitioned_backend_execution_receipt_v1"
    status = if ($Pass) {
        "CN_PHASE3CM_PARTITIONED_BACKEND_PASS"
    } elseif ($GlobalGateFailure) {
        "CN_PHASE3CM_PARTITIONED_BACKEND_GLOBAL_RSS_FAIL"
    } elseif (-not $WallGatePass) {
        "CN_PHASE3CM_PARTITIONED_BACKEND_WALL_FAIL"
    } elseif ($null -ne $LaunchFailure) {
        "CN_PHASE3CM_PARTITIONED_BACKEND_LAUNCH_FAIL"
    } else {
        "CN_PHASE3CM_PARTITIONED_BACKEND_FAIL"
    }
    partition_contract = $PartitionContract
    partition_contract_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $PartitionContract).Hash.ToLowerInvariant()
    contract_hash = [string]$Contract.contract_hash
    repo_sha = $ObservedRepoSha
    source_closure_manifest = $SourceClosureManifest
    source_closure_manifest_sha256 = $ObservedSourceClosureManifestSha256
    source_closure_manifest_hash = [string]$FrozenSourceClosure.manifest_hash
    source_closure_hash = [string]$FrozenSourceClosure.source_closure_hash
    logical_backend = [string]$Contract.logical_backend
    pair_count = [int]$Contract.pair_count
    partition_count = 2
    wall_seconds = $Stopwatch.Elapsed.TotalSeconds
    wall_seconds_hard_max = if ($WallSecondsHardMax -gt 0.0) { $WallSecondsHardMax } else { $null }
    wall_gate_enforced = $WallSecondsHardMax -gt 0.0
    wall_gate_terminated_processes = $WallGateFailure
    launch_failure = $LaunchFailure
    global_peak_rss_bytes = $GlobalPeakRss
    global_hard_rss_bytes = $GlobalHardRss
    rss_sample_count = $RssSampleCount
    rss_timeline = $RssTimelinePath
    rss_timeline_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $RssTimelinePath).Hash.ToLowerInvariant()
    partitions = $PartitionEvidence
    reused_backend_result = $Contract.reused_backend_result
    access_evidence_complete = $AccessEvidenceComplete
    validation_reads = if ($AccessEvidenceComplete) { $ValidationReads } else { $null }
    holdout_reads = if ($AccessEvidenceComplete) { $HoldoutReads } else { $null }
    forward_2026_reads = if ($AccessEvidenceComplete) { $Forward2026Reads } else { $null }
    promotion = "FORBIDDEN"
}
$ReceiptPath = Join-Path $RunRoot "CN_PARTITIONED_BACKEND_EXECUTION_RECEIPT.json"
Write-CnAtomicJson -Path $ReceiptPath -Payload $Receipt
Write-Output ($Receipt | ConvertTo-Json -Depth 12 -Compress)
if (-not $Pass) { exit 1 }
