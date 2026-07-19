param(
    [Parameter(Mandatory = $true)][string]$ReplayContract,
    [Parameter(Mandatory = $true)][string]$ExpectedRepoSha,
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [switch]$Resume,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$FrozenKernelBaseSha = "b071e36684d26d051e949aac18e9f86a79428ed3"
$FrozenPortfolioSourceSha256 = "5b665d33d74eb4b5f451f06b4ad7e4b352f779c3e2e4836bb2d76950c413d110"

function Get-CnSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Resolve-CnPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Base,
        [switch]$Directory
    )
    $Candidate = if ([IO.Path]::IsPathRooted($Path)) { $Path } else { Join-Path $Base $Path }
    if ($Directory) {
        if (-not (Test-Path -LiteralPath $Candidate -PathType Container)) {
            throw "required directory is missing: $Candidate"
        }
    }
    elseif (-not (Test-Path -LiteralPath $Candidate -PathType Leaf)) {
        throw "required file is missing: $Candidate"
    }
    return (Resolve-Path -LiteralPath $Candidate).Path
}

function Confirm-CnFileBinding {
    param(
        [Parameter(Mandatory = $true)]$Spec,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Base
    )
    $Path = Resolve-CnPath -Path ([string]$Spec.path) -Base $Base
    $Observed = Get-CnSha256 -Path $Path
    if ($Observed -ne [string]$Spec.sha256) { throw "$Label SHA-256 drift" }
    return $Path
}

function Test-CnSequenceEqual {
    param([object[]]$Left, [object[]]$Right)
    if ($Left.Count -ne $Right.Count) { return $false }
    for ($Index = 0; $Index -lt $Left.Count; $Index += 1) {
        if ([string]$Left[$Index] -ne [string]$Right[$Index]) { return $false }
    }
    return $true
}

function Get-CnArgumentValue {
    param([object[]]$Arguments, [string]$Name)
    $Index = [Array]::IndexOf([object[]]$Arguments, [object]$Name)
    if ($Index -lt 0 -or $Index + 1 -ge $Arguments.Count) {
        throw "historical command lacks $Name"
    }
    return [string]$Arguments[$Index + 1]
}

function Test-CnZeroAccessEvidence {
    param([object]$Payload)
    if ($null -eq $Payload) { return $false }
    foreach ($Name in @("validation_reads", "holdout_reads", "forward_2026_reads")) {
        $Property = $Payload.PSObject.Properties[$Name]
        if ($null -eq $Property -or $null -eq $Property.Value -or [int64]$Property.Value -ne 0) {
            return $false
        }
    }
    return $true
}

function Get-CnDirectoryBytes {
    param([Parameter(Mandatory = $true)][string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return [int64]0 }
    $Measure = Get-ChildItem -LiteralPath $Path -Recurse -File -ErrorAction Stop |
        Measure-Object -Property Length -Sum
    return [int64]($Measure.Sum -as [int64])
}

function Confirm-CnSidecarClosure {
    param(
        [Parameter(Mandatory = $true)]$Spec,
        [Parameter(Mandatory = $true)][string]$ExpectedStatus,
        [Parameter(Mandatory = $true)][string]$SplitSha256,
        [Parameter(Mandatory = $true)][string]$Base
    )
    $Root = Resolve-CnPath -Path ([string]$Spec.root) -Base $Base -Directory
    $ManifestPath = Confirm-CnFileBinding -Spec $Spec.manifest -Label "sidecar manifest" -Base $Base
    $Manifest = Get-Content -LiteralPath $ManifestPath -Raw | ConvertFrom-Json
    if ([string]$Manifest.status -ne $ExpectedStatus -or
        [string]$Manifest.data_role -ne "development_train_only" -or
        -not (Test-CnZeroAccessEvidence -Payload $Manifest) -or
        [string]$Manifest.split_manifest_hash -ne $SplitSha256) {
        throw "sidecar manifest boundary or status drift: $ManifestPath"
    }
    $Shards = @($Manifest.shards)
    if ($Shards.Count -ne [int]$Spec.manifest.expected_parquet_count -or $Shards.Count -ne 16) {
        throw "sidecar manifest must bind exactly 16 parquet files: $ManifestPath"
    }
    $RootPrefix = $Root.TrimEnd("\") + "\"
    $Seen = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $TotalBytes = [int64]0
    foreach ($Shard in $Shards) {
        $ShardPath = Resolve-CnPath -Path ([string]$Shard.output_path) -Base $Root
        if (-not $ShardPath.StartsWith($RootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
            throw "sidecar parquet escapes frozen root: $ShardPath"
        }
        if (-not $Seen.Add($ShardPath)) { throw "duplicate sidecar parquet: $ShardPath" }
        if ((Get-CnSha256 -Path $ShardPath) -ne [string]$Shard.output_sha256) {
            throw "sidecar parquet SHA-256 drift: $ShardPath"
        }
        $TotalBytes += [int64](Get-Item -LiteralPath $ShardPath).Length
    }
    return [ordered]@{
        root = $Root
        manifest = $ManifestPath
        manifest_sha256 = Get-CnSha256 -Path $ManifestPath
        verified_parquet_count = $Shards.Count
        verified_parquet_bytes = $TotalBytes
    }
}

$RepoRoot = Resolve-CnPath -Path $RepoRoot -Base (Get-Location).Path -Directory
$ReplayContract = Resolve-CnPath -Path $ReplayContract -Base $RepoRoot
$Contract = Get-Content -LiteralPath $ReplayContract -Raw | ConvertFrom-Json
if ($Contract.schema_version -ne "cn_phase3cm_current_kernel_146_parity_replay_v1" -or
    $Contract.authorization_status -ne "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_REPLAY_AUTHORIZED") {
    throw "146 replay contract is not authorized"
}
if ($ExpectedRepoSha -notmatch '^[0-9a-f]{40}$') { throw "ExpectedRepoSha must be exact lowercase Git SHA" }
$ObservedRepoSha = ((& git -C $RepoRoot rev-parse HEAD) | Select-Object -Last 1).Trim().ToLowerInvariant()
if ($LASTEXITCODE -ne 0 -or $ObservedRepoSha -ne $ExpectedRepoSha) { throw "launch workspace does not match ExpectedRepoSha" }
$Dirty = @(& git -C $RepoRoot status --porcelain)
if ($LASTEXITCODE -ne 0 -or $Dirty.Count -ne 0) { throw "launch workspace must be clean" }
if ([string]$Contract.source_binding.frozen_kernel_base_repo_sha -ne $FrozenKernelBaseSha) {
    throw "frozen kernel base SHA drift"
}
& git -C $RepoRoot merge-base --is-ancestor $FrozenKernelBaseSha $ExpectedRepoSha
if ($LASTEXITCODE -ne 0) { throw "launch commit is not a descendant of the frozen kernel base" }
$PortfolioSource = Resolve-CnPath -Path ([string]$Contract.source_binding.portfolio_source_path) -Base $RepoRoot
if ([string]$Contract.source_binding.portfolio_source_sha256 -ne $FrozenPortfolioSourceSha256 -or
    (Get-CnSha256 -Path $PortfolioSource) -ne $FrozenPortfolioSourceSha256) {
    throw "frozen portfolio kernel source SHA-256 drift"
}

$Execution = $Contract.execution_contract
if ([string]$Execution.phase -ne "E" -or
    [string]$Execution.execution_mode -ne "PHASE_E_EXACT_PLAN_PARITY_REPLAY" -or
    [string]$Execution.authority_scope -ne "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR" -or
    [int]$Execution.heavy_processes -ne 2 -or
    [int]$Execution.compute_threads_per_process -ne 11 -or
    [int]$Execution.active_native_compute_threads_total -ne 22 -or
    [int]$Execution.active_native_compute_threads_total -gt [int]$Execution.global_native_compute_threads_max -or
    [int]$Execution.global_native_compute_threads_max -gt 24) {
    throw "frozen parity execution topology drift"
}
$ExpectedThreadEnvironment = [ordered]@{
    NUMBA_NUM_THREADS = "11"
    ARROW_NUM_THREADS = "1"
    OMP_NUM_THREADS = "1"
    MKL_NUM_THREADS = "1"
    OPENBLAS_NUM_THREADS = "1"
    NUMEXPR_MAX_THREADS = "1"
    POLARS_MAX_THREADS = "1"
}
foreach ($Name in $ExpectedThreadEnvironment.Keys) {
    if ([string]$Execution.thread_environment.$Name -ne [string]$ExpectedThreadEnvironment[$Name]) {
        throw "thread environment drift: $Name"
    }
}
if ($Contract.data_access_contract.validation -ne "FORBIDDEN_ZERO_READS_REQUIRED" -or
    $Contract.data_access_contract.holdout -ne "FORBIDDEN_ZERO_READS_REQUIRED" -or
    $Contract.data_access_contract.forward_2026 -ne "SEALED_ZERO_READS_REQUIRED" -or
    $Contract.data_access_contract.candidate_promotion -ne "FORBIDDEN") {
    throw "data access contract drift"
}

if (-not $PythonExe) { $PythonExe = [string]$Execution.python_executable }
$PythonExe = Resolve-CnPath -Path $PythonExe -Base $RepoRoot
$Wrapper = Resolve-CnPath -Path "scripts\invoke_cn_phase3cm_backend_with_exit_receipt.ps1" -Base $RepoRoot
$Runner = Resolve-CnPath -Path "scripts\run_cn_phase3cm_streaming_qualification.py" -Base $RepoRoot
$Qualifier = Resolve-CnPath -Path "scripts\qualify_cn_batched_portfolio_kernel.py" -Base $RepoRoot
$CheckpointComparator = Resolve-CnPath -Path "scripts\compare_cn_phase3cm_scaling_checkpoints.py" -Base $RepoRoot
$Finalizer = Resolve-CnPath -Path "scripts\finalize_cn_phase3cm_current_kernel_146_parity.py" -Base $RepoRoot
. (Resolve-CnPath -Path "scripts\cn_phase3cm_process_tree_monitor.ps1" -Base $RepoRoot)

$PartitionContractPath = Confirm-CnFileBinding -Spec $Contract.historical_input_binding.partition_contract -Label "historical partition contract" -Base $RepoRoot
$PartitionContractPayload = Get-Content -LiteralPath $PartitionContractPath -Raw | ConvertFrom-Json
if ([string]$PartitionContractPayload.contract_hash -ne [string]$Contract.historical_input_binding.partition_contract.contract_hash) {
    throw "historical partition contract content hash drift"
}
$BindingPath = Confirm-CnFileBinding -Spec $Contract.historical_input_binding.input_binding -Label "historical input binding" -Base $RepoRoot
$BindingPayload = Get-Content -LiteralPath $BindingPath -Raw | ConvertFrom-Json
if ([string]$BindingPayload.binding_hash -ne [string]$Contract.historical_input_binding.input_binding.binding_hash) {
    throw "historical input binding content hash drift"
}
$FullCandidateTable = Confirm-CnFileBinding -Spec $Contract.historical_input_binding.full_active_candidate_table -Label "full candidate table" -Base $RepoRoot
$ArtifactRoot = Resolve-CnPath -Path ([string]$Contract.historical_input_binding.artifact_root) -Base $RepoRoot -Directory
$SplitManifest = Confirm-CnFileBinding -Spec $Contract.sidecar_and_split_binding.split_manifest -Label "split manifest" -Base $RepoRoot
$SplitSha256 = Get-CnSha256 -Path $SplitManifest
$FieldClosure = Confirm-CnSidecarClosure -Spec $Contract.sidecar_and_split_binding.active_field_sidecar -ExpectedStatus "TIME_MAJOR_LAYOUT_PARITY_PASS" -SplitSha256 $SplitSha256 -Base $RepoRoot
$LabelClosure = Confirm-CnSidecarClosure -Spec $Contract.sidecar_and_split_binding.label_sidecar -ExpectedStatus "GLOBAL_SYMBOL_CONTINUITY_LABEL_SIDECARS_READY" -SplitSha256 $SplitSha256 -Base $RepoRoot

$Partitions = @($Contract.partitions)
if ($Partitions.Count -ne 2) { throw "146 replay requires exactly two partitions" }
$PartitionEvidence = @()
$AllPairIds = @()
$AllCandidateIds = @()
foreach ($Partition in $Partitions) {
    if ([int]$Partition.pair_count -ne 73 -or [int]$Partition.candidate_count -ne 146) {
        throw "each partition must contain exactly 73 pairs / 146 candidates"
    }
    $CandidateTable = Confirm-CnFileBinding -Spec $Partition.candidate_table -Label "$($Partition.partition_id) candidate table" -Base $RepoRoot
    $Rows = @(Import-Csv -LiteralPath $CandidateTable)
    $ObservedCandidateIds = @($Rows | ForEach-Object { [string]$_.candidate_id })
    $ObservedPairIds = @($Rows | ForEach-Object { [string]$_.pair_id } | Select-Object -Unique)
    if (-not (Test-CnSequenceEqual $ObservedPairIds @($Partition.expected_pair_ids)) -or
        -not (Test-CnSequenceEqual $ObservedCandidateIds @($Partition.expected_candidate_ids))) {
        throw "$($Partition.partition_id) candidate or pair identity/order drift"
    }
    $PlanPath = Confirm-CnFileBinding -Spec $Partition.historical_execution_plan -Label "$($Partition.partition_id) execution plan" -Base $RepoRoot
    $Plan = Get-Content -LiteralPath $PlanPath -Raw | ConvertFrom-Json
    if ([string]$Plan.execution_plan_hash -ne [string]$Partition.historical_execution_plan.execution_plan_hash -or
        [string]$Plan.phase -ne "E" -or [int]$Plan.compute_threads -ne 11 -or
        [int]$Plan.block_size -ne [int]$Execution.block_sessions -or
        [int]$Plan.checkpoint_every_blocks -ne [int]$Execution.checkpoint_every_blocks -or
        [int64]$Plan.rss_soft_bytes -ne [int64]$Contract.resource_gates.per_process_rss_soft_bytes -or
        [int64]$Plan.rss_hard_bytes -ne [int64]$Contract.resource_gates.per_process_rss_hard_bytes -or
        [int64]$Plan.global_rss_hard_bytes -ne [int64]$Contract.resource_gates.global_rss_hard_bytes) {
        throw "$($Partition.partition_id) frozen execution plan drift"
    }
    $PlanPairIds = @($Plan.pair_batches | ForEach-Object { @($_) } | ForEach-Object { [string]$_ })
    if (-not (Test-CnSequenceEqual $PlanPairIds @($Partition.expected_pair_ids))) {
        throw "$($Partition.partition_id) frozen plan pair order drift"
    }
    $CapacityPath = Confirm-CnFileBinding -Spec $Partition.capacity_receipt -Label "$($Partition.partition_id) capacity receipt" -Base $RepoRoot
    $Capacity = Get-Content -LiteralPath $CapacityPath -Raw | ConvertFrom-Json
    if ([string]$Capacity.receipt_hash -ne [string]$Partition.capacity_receipt_hash -or
        [string]$Partition.capacity_receipt.receipt_hash -ne [string]$Partition.capacity_receipt_hash -or
        [int64]$Capacity.max_block_rows -ne [int64]$Partition.max_block_rows -or
        [int64]$Partition.capacity_receipt.max_block_rows -ne [int64]$Partition.max_block_rows) {
        throw "$($Partition.partition_id) capacity receipt drift"
    }
    $HistoricalCommandPath = Confirm-CnFileBinding -Spec $Partition.historical_backend_command -Label "$($Partition.partition_id) historical command" -Base $RepoRoot
    $HistoricalCommand = Get-Content -LiteralPath $HistoricalCommandPath -Raw | ConvertFrom-Json
    $HistoricalArgs = @($HistoricalCommand.arguments | ForEach-Object { [string]$_ })
    if ((Get-CnArgumentValue $HistoricalArgs "--phase") -ne "E" -or
        (Get-CnArgumentValue $HistoricalArgs "--candidate-table") -ne $CandidateTable -or
        (Get-CnArgumentValue $HistoricalArgs "--execution-plan") -ne $PlanPath -or
        [int64](Get-CnArgumentValue $HistoricalArgs "--max-block-rows") -ne [int64]$Partition.max_block_rows -or
        (Get-CnArgumentValue $HistoricalArgs "--capacity-receipt-hash") -ne [string]$Partition.capacity_receipt_hash) {
        throw "$($Partition.partition_id) historical command binding drift"
    }
    $ReferenceRoot = Resolve-CnPath -Path ([string]$Partition.historical_reference_root) -Base $RepoRoot -Directory
    foreach ($Artifact in @("CN_STREAMING_BACKEND_RESULT.json", "CN_STREAMING_REWARD_ATOMS.csv", "CN_STREAMING_REDUCER_CONTRACT.json", "CN_FROZEN_EXECUTION_PLAN.json")) {
        [void](Resolve-CnPath -Path (Join-Path $ReferenceRoot $Artifact) -Base $RepoRoot)
    }
    $AllPairIds += $ObservedPairIds
    $AllCandidateIds += $ObservedCandidateIds
    $PartitionEvidence += [ordered]@{
        partition = $Partition
        candidate_table = $CandidateTable
        execution_plan = $PlanPath
        plan = $Plan
        capacity_receipt = $CapacityPath
        reference_root = $ReferenceRoot
    }
}
if (@($AllPairIds | Select-Object -Unique).Count -ne 146 -or
    @($AllCandidateIds | Select-Object -Unique).Count -ne 292) {
    throw "partition union does not cover exactly 146 pairs / 292 candidates"
}
$FullRows = @(Import-Csv -LiteralPath $FullCandidateTable)
$FullPairIds = @($FullRows | ForEach-Object { [string]$_.pair_id } | Select-Object -Unique | Sort-Object)
$FullCandidateIds = @($FullRows | ForEach-Object { [string]$_.candidate_id } | Sort-Object)
if (-not (Test-CnSequenceEqual $FullPairIds @($AllPairIds | Sort-Object)) -or
    -not (Test-CnSequenceEqual $FullCandidateIds @($AllCandidateIds | Sort-Object))) {
    throw "partition union drifts from full frozen active candidate table"
}

# All source, plan, capacity, sidecar and access gates above run before output creation.
$RunRoot = [string]$Execution.output_root
if ((Test-Path -LiteralPath $RunRoot) -and -not $Resume) { throw "unique replay output already exists: $RunRoot" }
if ($Resume -and -not (Test-Path -LiteralPath $RunRoot -PathType Container)) { throw "resume output root does not exist: $RunRoot" }
if ($RunRoot.StartsWith((Split-Path -Parent $PartitionEvidence[0].reference_root), [StringComparison]::OrdinalIgnoreCase)) {
    throw "replay output must not overlap immutable historical reference"
}
if ((Get-CnDirectoryBytes -Path $RunRoot) -ge [int64]$Contract.resource_gates.output_bytes_hard) {
    throw "existing replay output already breaches the hard output cap"
}
New-Item -ItemType Directory -Force -Path $RunRoot | Out-Null
$FinalParityPath = Join-Path $RunRoot "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_RECEIPT.json"
if (Test-Path -LiteralPath $FinalParityPath) { Remove-Item -LiteralPath $FinalParityPath -Force }

$env:PYTHONPATH = Join-Path $RepoRoot "src"
$Processes = @()
$Roots = @{}
$OutputRoots = @()
$ResultPaths = @()
$ExitReceiptPaths = @()
$CommandPaths = @()
$CommandHashes = @()
$PartitionNames = @()
$PowerShellExe = (Get-Command powershell.exe).Source
for ($Index = 0; $Index -lt 2; $Index += 1) {
    $Evidence = $PartitionEvidence[$Index]
    $Partition = $Evidence.partition
    $OutputRoot = Join-Path $RunRoot ([string]$Partition.replay_output_subdir)
    New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
    $OutputRoots += $OutputRoot
    $ResultPath = Join-Path $OutputRoot "CN_STREAMING_BACKEND_RESULT.json"
    $ExitReceiptPath = Join-Path $OutputRoot "CN_BACKEND_EXIT_RECEIPT.json"
    foreach ($StaleArtifact in @(
        $ResultPath,
        $ExitReceiptPath,
        (Join-Path $OutputRoot "CN_KERNEL_PARITY_QUALIFICATION.json"),
        (Join-Path $OutputRoot "CN_CHECKPOINT_PARITY.json")
    )) {
        if (Test-Path -LiteralPath $StaleArtifact) { Remove-Item -LiteralPath $StaleArtifact -Force }
    }
    $Arguments = @(
        "scripts\run_cn_phase3cm_streaming_qualification.py",
        "--backend", "active_bar",
        "--phase", "E",
        "--pair-count", [string]$Partition.pair_count,
        "--candidate-table", $Evidence.candidate_table,
        "--binding", $BindingPath,
        "--split-manifest", $SplitManifest,
        "--artifact-root", $ArtifactRoot,
        "--field-sidecar-root", [string]$FieldClosure.root,
        "--label-sidecar-root", [string]$LabelClosure.root,
        "--output-root", $OutputRoot,
        "--execution-plan", $Evidence.execution_plan,
        "--block-sessions", [string]$Execution.block_sessions,
        "--pair-batch-size", [string]$Execution.pair_batch_size,
        "--compute-threads", [string]$Execution.compute_threads_per_process,
        "--max-block-rows", [string]$Partition.max_block_rows,
        "--capacity-receipt-hash", [string]$Partition.capacity_receipt_hash
    )
    if ($Resume) { $Arguments += "--resume" }
    $CommandPath = Join-Path $OutputRoot "CN_BACKEND_COMMAND.json"
    Write-CnAtomicJson -Path $CommandPath -Payload ([ordered]@{
        schema_version = "cn_phase3cm_current_kernel_146_replay_command_v1"
        authority_scope = "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR"
        partition_id = [string]$Partition.partition_id
        expected_repo_sha = $ExpectedRepoSha
        arguments = $Arguments
        thread_environment = $ExpectedThreadEnvironment
    })
    $PartitionNames += [string]$Partition.partition_id
    $CommandPaths += $CommandPath
    $CommandHashes += Get-CnSha256 -Path $CommandPath
    $ResultPaths += $ResultPath
    $ExitReceiptPaths += $ExitReceiptPath
}

$RssTimelinePath = Join-Path $RunRoot "CN_GLOBAL_PROCESS_TREE_RSS_TIMELINE.csv"
"sampled_at,partition_00_process_ids,partition_00_rss_bytes,partition_01_process_ids,partition_01_rss_bytes,global_rss_bytes,output_bytes" |
    Set-Content -LiteralPath $RssTimelinePath -Encoding UTF8
$GlobalPeakRss = [int64]0
$OutputPeakBytes = Get-CnDirectoryBytes -Path $RunRoot
$CurrentOutputBytes = $OutputPeakBytes
$RssSampleCount = 0
$GateFailure = $null
$LaunchFailure = $null
$PartitionPeaks = @{}
foreach ($Name in $PartitionNames) { $PartitionPeaks[$Name] = [int64]0 }
$Stopwatch = [Diagnostics.Stopwatch]::StartNew()
try {
    for ($Index = 0; $Index -lt 2; $Index += 1) {
        $OutputRoot = $OutputRoots[$Index]
        $Process = Start-Process -FilePath $PowerShellExe -ArgumentList @(
            "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
            "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
            "-ArgumentFile", $CommandPaths[$Index], "-ExitReceipt", $ExitReceiptPaths[$Index]
        ) -WorkingDirectory $RepoRoot `
            -RedirectStandardOutput (Join-Path $OutputRoot "stdout.log") `
            -RedirectStandardError (Join-Path $OutputRoot "stderr.log") `
            -WindowStyle Hidden -PassThru
        $Processes += $Process
        $Roots[$PartitionNames[$Index]] = $Process.Id
    }
    while ($true) {
        $Running = @()
        foreach ($Process in $Processes) { $Process.Refresh(); if (-not $Process.HasExited) { $Running += $Process } }
        $Snapshot = Get-CnProcessTreeRssSnapshot -Roots $Roots
        # Directory traversal is deliberately sampled every ~30 seconds; doing it
        # on every two-second RSS tick would make checkpoint-heavy runs I/O bound.
        if (($RssSampleCount % 15) -eq 0 -or $Running.Count -eq 0) {
            $CurrentOutputBytes = Get-CnDirectoryBytes -Path $RunRoot
        }
        $OutputPeakBytes = [Math]::Max($OutputPeakBytes, $CurrentOutputBytes)
        $GlobalPeakRss = [Math]::Max($GlobalPeakRss, [int64]$Snapshot.total_rss_bytes)
        foreach ($Name in $PartitionNames) {
            $PartitionPeaks[$Name] = [Math]::Max([int64]$PartitionPeaks[$Name], [int64]$Snapshot.groups[$Name].rss_bytes)
        }
        @(
            $Snapshot.sampled_at,
            ($Snapshot.groups[$PartitionNames[0]].live_process_ids -join "|"),
            [string]$Snapshot.groups[$PartitionNames[0]].rss_bytes,
            ($Snapshot.groups[$PartitionNames[1]].live_process_ids -join "|"),
            [string]$Snapshot.groups[$PartitionNames[1]].rss_bytes,
            [string]$Snapshot.total_rss_bytes,
            [string]$CurrentOutputBytes
        ) -join "," | Add-Content -LiteralPath $RssTimelinePath -Encoding UTF8
        $RssSampleCount += 1
        if ([int64]$Snapshot.total_rss_bytes -ge [int64]$Contract.resource_gates.global_rss_hard_bytes) {
            $GateFailure = "GLOBAL_RSS_HARD_GATE"; Stop-CnProcessTrees -Roots $Roots; break
        }
        if (@($PartitionNames | Where-Object { [int64]$Snapshot.groups[$_].rss_bytes -ge [int64]$Contract.resource_gates.per_process_rss_hard_bytes }).Count -gt 0) {
            $GateFailure = "PROCESS_RSS_HARD_GATE"; Stop-CnProcessTrees -Roots $Roots; break
        }
        if ($Stopwatch.Elapsed.TotalSeconds -ge [double]$Contract.resource_gates.wall_seconds_hard) {
            $GateFailure = "WALL_HARD_GATE"; Stop-CnProcessTrees -Roots $Roots; break
        }
        if ($CurrentOutputBytes -ge [int64]$Contract.resource_gates.output_bytes_hard) {
            $GateFailure = "OUTPUT_HARD_GATE"; Stop-CnProcessTrees -Roots $Roots; break
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
    foreach ($Process in $Processes) { try { $Process.WaitForExit(); $Process.Refresh() } catch { } }
    $Stopwatch.Stop()
}

$Pass = ($null -eq $GateFailure -and $null -eq $LaunchFailure)
$PartitionResults = @()
$QualificationPaths = @()
$CheckpointPaths = @()
for ($Index = 0; $Index -lt 2; $Index += 1) {
    $Partition = $PartitionEvidence[$Index].partition
    $OutputRoot = $OutputRoots[$Index]
    $ExitReceipt = if (Test-Path $ExitReceiptPaths[$Index]) { Get-Content $ExitReceiptPaths[$Index] -Raw | ConvertFrom-Json } else { $null }
    $Result = if (Test-Path $ResultPaths[$Index]) { Get-Content $ResultPaths[$Index] -Raw | ConvertFrom-Json } else { $null }
    $ActualExitCode = if ($Index -lt $Processes.Count -and $Processes[$Index].HasExited) { [int]$Processes[$Index].ExitCode } else { -1 }
    $ObservedPairIds = if ($null -ne $Result) { @($Result.pair_results | ForEach-Object { [string]$_.pair_id }) } else { @() }
    $ExecutionPass = (
        $null -ne $ExitReceipt -and [int]$ExitReceipt.exit_code -eq 0 -and $ActualExitCode -eq 0 -and
        [string]$ExitReceipt.argument_file_sha256 -eq [string]$CommandHashes[$Index] -and
        $null -ne $Result -and $Result.status -eq "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED" -and
        $Result.phase -eq "E" -and $Result.backend -eq "active_bar" -and
        [int]$Result.pair_count -eq 73 -and [int]$Result.candidate_count -eq 146 -and
        [string]$Result.execution_plan_hash -eq [string]$Partition.historical_execution_plan.execution_plan_hash -and
        [string]$Result.capacity_receipt_hash -eq [string]$Partition.capacity_receipt_hash -and
        $Result.block_row_guard_status -eq "BLOCK_ROW_GUARD_PASS" -and
        [int64]$Result.max_observed_block_rows -le [int64]$Partition.max_block_rows -and
        $Result.parallelism_status -eq "PARALLELISM_ENGAGED" -and
        [int]$Result.coordinate_rows_retained -eq 0 -and
        (Test-CnZeroAccessEvidence -Payload $Result) -and
        $Result.promotion -eq "FORBIDDEN" -and $Result.strict_stage_a -eq "NOT_AUTHORIZED" -and
        (Test-CnSequenceEqual $ObservedPairIds @($Partition.expected_pair_ids))
    )
    $QualificationPath = Join-Path $OutputRoot "CN_KERNEL_PARITY_QUALIFICATION.json"
    $CheckpointPath = Join-Path $OutputRoot "CN_CHECKPOINT_PARITY.json"
    $QualificationPass = $false
    $CheckpointPass = $false
    if ($ExecutionPass) {
        & $PythonExe $Qualifier --reference-root $PartitionEvidence[$Index].reference_root --candidate-root $OutputRoot --output $QualificationPath
        $Qualification = if (Test-Path $QualificationPath) { Get-Content $QualificationPath -Raw | ConvertFrom-Json } else { $null }
        $QualificationPass = ($null -ne $Qualification -and $Qualification.comparable -eq $true -and $Qualification.semantic_parity_exact -eq $true)
        & $PythonExe $CheckpointComparator --reference-root $PartitionEvidence[$Index].reference_root --candidate-root $OutputRoot --backend active_bar --output $CheckpointPath
        $Checkpoint = if (Test-Path $CheckpointPath) { Get-Content $CheckpointPath -Raw | ConvertFrom-Json } else { $null }
        $CheckpointPass = ($null -ne $Checkpoint -and $Checkpoint.status -eq "CN_PHASE3CM_SCALING_PROBE_PARITY_PASS")
    }
    if (-not ($ExecutionPass -and $QualificationPass -and $CheckpointPass)) { $Pass = $false }
    $QualificationPaths += $QualificationPath
    $CheckpointPaths += $CheckpointPath
    $PartitionResults += [ordered]@{
        partition_id = [string]$Partition.partition_id
        execution_pass = $ExecutionPass
        qualification_exact_pass = $QualificationPass
        checkpoint_exact_pass = $CheckpointPass
        process_exit_code = $ActualExitCode
        peak_process_tree_rss_bytes = [int64]$PartitionPeaks[[string]$Partition.partition_id]
        backend_result = $ResultPaths[$Index]
        qualification_receipt = $QualificationPath
        checkpoint_receipt = $CheckpointPath
    }
}
$FinalizerArguments = @("--contract", $ReplayContract)
for ($Index = 0; $Index -lt 2; $Index += 1) {
    $PartitionId = [string]$PartitionEvidence[$Index].partition.partition_id
    $FinalizerArguments += @("--qualification", "${PartitionId}=$($QualificationPaths[$Index])")
    $FinalizerArguments += @("--checkpoint", "${PartitionId}=$($CheckpointPaths[$Index])")
}
$FinalizerArguments += @("--output", $FinalParityPath)
& $PythonExe $Finalizer @FinalizerArguments
$FinalParity = if (Test-Path -LiteralPath $FinalParityPath) {
    Get-Content -LiteralPath $FinalParityPath -Raw | ConvertFrom-Json
} else { $null }
$FinalParityPass = (
    $null -ne $FinalParity -and
    $FinalParity.status -eq "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS" -and
    $FinalParity.identity_coverage_exact -eq $true
)
if (-not $FinalParityPass) { $Pass = $false }
$FinalOutputBytes = Get-CnDirectoryBytes -Path $RunRoot
if ($FinalOutputBytes -ge [int64]$Contract.resource_gates.output_bytes_hard) { $Pass = $false }
$Receipt = [ordered]@{
    schema_version = "cn_phase3cm_current_kernel_146_replay_execution_receipt_v1"
    status = if ($Pass) { "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_EXECUTION_AND_PARITY_PASS" } else { "CN_PHASE3CM_CURRENT_KERNEL_146_REPLAY_FAIL_CLOSED" }
    authority_scope = "PARITY_REPLAY_ONLY_NOT_FORMAL_EVALUATOR"
    contract = $ReplayContract
    contract_sha256 = Get-CnSha256 -Path $ReplayContract
    expected_repo_sha = $ExpectedRepoSha
    observed_repo_sha = $ObservedRepoSha
    portfolio_source_sha256 = Get-CnSha256 -Path $PortfolioSource
    resumed = [bool]$Resume
    wall_seconds = $Stopwatch.Elapsed.TotalSeconds
    wall_seconds_hard = [double]$Contract.resource_gates.wall_seconds_hard
    global_peak_rss_bytes = $GlobalPeakRss
    global_rss_hard_bytes = [int64]$Contract.resource_gates.global_rss_hard_bytes
    output_peak_bytes = $OutputPeakBytes
    output_final_bytes = $FinalOutputBytes
    output_bytes_hard = [int64]$Contract.resource_gates.output_bytes_hard
    gate_failure = $GateFailure
    launch_failure = $LaunchFailure
    rss_sample_count = $RssSampleCount
    rss_timeline = $RssTimelinePath
    field_sidecar_closure = $FieldClosure
    label_sidecar_closure = $LabelClosure
    partitions = $PartitionResults
    final_parity_receipt = $FinalParityPath
    final_parity_pass = $FinalParityPass
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    promotion = "FORBIDDEN"
    strict_stage_a = "NOT_AUTHORIZED"
    speedup_threshold_gate = "NOT_USED"
}
$ReceiptPath = Join-Path $RunRoot "CN_CURRENT_KERNEL_146_REPLAY_EXECUTION_RECEIPT.json"
Write-CnAtomicJson -Path $ReceiptPath -Payload $Receipt
Write-Output ($Receipt | ConvertTo-Json -Depth 12 -Compress)
if (-not $Pass) { exit 2 }
