param(
    [Parameter(Mandatory = $true)]
    [string]$CombinedContract,
    [Parameter(Mandatory = $true)]
    [string]$ActiveCapacityReceipt,
    [Parameter(Mandatory = $true)]
    [string]$SessionCapacityReceipt,
    [Parameter(Mandatory = $true)]
    [string]$PredecessorEngineeringReceipt,
    [Parameter(Mandatory = $true)]
    [string]$PredecessorEngineeringReceiptSha256,
    [Parameter(Mandatory = $true)]
    [string]$SourceClosureManifest,
    [string]$OutputName = "phase_e_strict_wave_final",
    [double]$WallSecondsHardMax = 0.0,
    [switch]$Resume,
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$CandidateRoot = "",
    [string]$RuntimeRoot = "",
    [string]$RunParentRoot = "",
    [string]$Binding = "",
    [string]$ActiveFieldRoot = "",
    [string]$ActiveLabelRoot = "",
    [string]$SessionFieldRoot = "",
    [string]$SessionLabelRoot = "",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
$Contract = Get-Content -LiteralPath $CombinedContract -Raw | ConvertFrom-Json
if ($Contract.status -ne "CN_PHASE3CM_PHASE_E_EXECUTION_PLANS_FROZEN") {
    throw "Phase E combined contract status drift"
}
if ([int]$Contract.heavy_processes -ne 2) { throw "Phase E requires exactly two heavy processes" }
if ([int]$Contract.global_active_native_compute_threads -gt 24) {
    throw "global native compute thread budget exceeds 24"
}
$ActivePairCount = [int]$Contract.plans.active_bar.pair_count
$SessionPairCount = [int]$Contract.plans.stock_session.pair_count
$TotalPairCount = $ActivePairCount + $SessionPairCount
if ($ActivePairCount -le 0 -or $SessionPairCount -le 0 -or $TotalPairCount -le 0) {
    throw "Phase E strict wave requires non-zero active and session pair counts"
}
if ($WallSecondsHardMax -lt 0.0) { throw "wall-seconds hard maximum cannot be negative" }

$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = "1"
. (Join-Path $RepoRoot "scripts\cn_phase3cm_process_tree_monitor.ps1")

if (-not $RuntimeRoot) { $RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716" }
if (-not $CandidateRoot) { $CandidateRoot = Join-Path $RepoRoot "runtime\cn_compositional_nline_large_search_20260715" }
if (-not $RunParentRoot) { $RunParentRoot = $RuntimeRoot }
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
if (-not $Binding) { $Binding = Join-Path $RuntimeRoot "CN_STREAMING_REPAIR_FROZEN_INPUT_BINDING.json" }
if (-not $ActiveFieldRoot) { $ActiveFieldRoot = Join-Path $RuntimeRoot "time_major_train_v2" }
if (-not $ActiveLabelRoot) { $ActiveLabelRoot = Join-Path $RuntimeRoot "time_major_train_v3_labels" }
if (-not $SessionFieldRoot) { $SessionFieldRoot = Join-Path $RuntimeRoot "session_time_major_train_v2" }
if (-not $SessionLabelRoot) { $SessionLabelRoot = Join-Path $RuntimeRoot "session_time_major_train_v3_labels" }
$ActiveCandidateTable = Join-Path $CandidateRoot "preflight_active_candidates.csv"
$SessionCandidateTable = Join-Path $CandidateRoot "preflight_session_candidates.csv"
$ActiveExecutionPlanPath = [string]$Contract.plans.active_bar.path
$SessionExecutionPlanPath = [string]$Contract.plans.stock_session.path
$ActivePlan = Get-Content -LiteralPath $ActiveExecutionPlanPath -Raw | ConvertFrom-Json
$SessionPlan = Get-Content -LiteralPath $SessionExecutionPlanPath -Raw | ConvertFrom-Json

$FrozenR6SuccessorReceiptSha256 = "dbcedff3c54a4799b75b81c71a1c94aa909de6123bd5959f3f82febf610dfcc4"
if ($PredecessorEngineeringReceiptSha256 -notmatch '^[0-9a-f]{64}$' -or
    $PredecessorEngineeringReceiptSha256 -ne $FrozenR6SuccessorReceiptSha256) {
    throw "predecessor engineering receipt hash does not match the frozen R6 successor authority"
}
$ObservedPredecessorReceiptSha256 = (
    Get-FileHash -Algorithm SHA256 -LiteralPath $PredecessorEngineeringReceipt
).Hash.ToLowerInvariant()
if ($ObservedPredecessorReceiptSha256 -ne $PredecessorEngineeringReceiptSha256) {
    throw "predecessor engineering receipt SHA-256 drift"
}
$PredecessorReceipt = Get-Content -LiteralPath $PredecessorEngineeringReceipt -Raw | ConvertFrom-Json
if ($PredecessorReceipt.schema_version -ne "cn_phase3cm_r6_successor_engineering_receipt_v1" -or
    $PredecessorReceipt.status -ne "CN_PHASE3CM_R6_SUCCESSOR_ENGINEERING_PASS") {
    throw "R6 successor predecessor engineering receipt is not PASS"
}
if ($PredecessorReceipt.data_role -ne "development_train_only" -or
    $PredecessorReceipt.promotion -ne "FORBIDDEN" -or
    [int]$PredecessorReceipt.access.validation_reads -ne 0 -or
    [int]$PredecessorReceipt.access.holdout_reads -ne 0 -or
    [int]$PredecessorReceipt.access.forward_2026_reads -ne 0) {
    throw "R6 successor predecessor data-access contract drift"
}
if ([int]$PredecessorReceipt.pair_count -ne 64 -or
    [int]$PredecessorReceipt.backend_pair_counts.active_bar -ne 36 -or
    [int]$PredecessorReceipt.backend_pair_counts.stock_session -ne 28 -or
    $PredecessorReceipt.research_parity.status -ne "BYTE_IDENTICAL_PAIR_ANALYSIS" -or
    $PredecessorReceipt.research_parity.pair_csv_sha256 -ne "886aa3b9dcf16001cf1ca7b574d7193e1f96dc18f49d52d5c512d4ac8ba5d413") {
    throw "R6 successor predecessor research-identity drift"
}
if ($PredecessorReceipt.active_backend.parallelism_status -ne "PARALLELISM_ENGAGED" -or
    $PredecessorReceipt.stock_session_backend.parallelism_status -ne "PARALLELISM_ENGAGED" -or
    [int]$PredecessorReceipt.stock_session_backend.compute_threads -ne 2) {
    throw "R6 successor predecessor engineering gates drift"
}
if ($PredecessorReceipt.supersession.original_r6_status -ne "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL" -or
    [bool]$PredecessorReceipt.supersession.research_rows_changed) {
    throw "original R6 FAIL/successor supersession semantics drift"
}

function Confirm-CapacityReceipt {
    param(
        [string]$Receipt,
        [string]$Backend,
        [string]$CandidateTable,
        [string]$ExecutionPlan,
        [string]$FieldRoot,
        [string]$LabelRoot
    )
    $Validator = Join-Path $RepoRoot "scripts\preflight_cn_phase3cm_dag_cache.py"
    $Output = @(& $PythonExe $Validator `
        --validate-receipt $Receipt `
        --candidate-table $CandidateTable `
        --execution-plan $ExecutionPlan `
        --backend $Backend `
        --field-sidecar-root $FieldRoot `
        --label-sidecar-root $LabelRoot `
        --repo-root $RepoRoot `
        --source-closure-manifest $SourceClosureManifest `
        --expected-repo-sha ([string]$Contract.repo_sha))
    if ($LASTEXITCODE -ne 0) {
        throw "Phase E capacity receipt validation failed for $Backend"
    }
    if ($Output.Count -eq 0) {
        throw "Phase E capacity receipt validator returned no result for $Backend"
    }
    $Validation = $Output[-1] | ConvertFrom-Json
    if ($Validation.status -ne "CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH") {
        throw "Phase E capacity receipt validator status drift for $Backend"
    }
    return $Validation
}

# This hard gate runs before any output directory or heavy process is created.
$ActiveCapacityValidation = Confirm-CapacityReceipt `
    -Receipt $ActiveCapacityReceipt `
    -Backend "active_bar" `
    -CandidateTable $ActiveCandidateTable `
    -ExecutionPlan $ActiveExecutionPlanPath `
    -FieldRoot $ActiveFieldRoot `
    -LabelRoot $ActiveLabelRoot
$SessionCapacityValidation = Confirm-CapacityReceipt `
    -Receipt $SessionCapacityReceipt `
    -Backend "stock_session" `
    -CandidateTable $SessionCandidateTable `
    -ExecutionPlan $SessionExecutionPlanPath `
    -FieldRoot $SessionFieldRoot `
    -LabelRoot $SessionLabelRoot
$ActiveMaxBlockRows = [int64]$ActiveCapacityValidation.max_block_rows
$SessionMaxBlockRows = [int64]$SessionCapacityValidation.max_block_rows
$ActiveCapacityReceiptHash = [string]$ActiveCapacityValidation.receipt_hash
$SessionCapacityReceiptHash = [string]$SessionCapacityValidation.receipt_hash
if ([string]$ActiveCapacityValidation.source_closure_manifest_hash -ne
    [string]$SessionCapacityValidation.source_closure_manifest_hash) {
    throw "capacity receipts do not bind the same source closure manifest"
}
if ([string]$ActiveCapacityValidation.execution_plan_hash -ne [string]$Contract.plans.active_bar.execution_plan_hash) {
    throw "active_bar capacity receipt plan hash drifts from combined contract"
}
if ([string]$SessionCapacityValidation.execution_plan_hash -ne [string]$Contract.plans.stock_session.execution_plan_hash) {
    throw "stock_session capacity receipt plan hash drifts from combined contract"
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $ActiveExecutionPlanPath).Hash.ToLowerInvariant() -ne [string]$Contract.plans.active_bar.sha256) {
    throw "active_bar execution plan file hash drifts from combined contract"
}
if ((Get-FileHash -Algorithm SHA256 -LiteralPath $SessionExecutionPlanPath).Hash.ToLowerInvariant() -ne [string]$Contract.plans.stock_session.sha256) {
    throw "stock_session execution plan file hash drifts from combined contract"
}

$RunRoot = Join-Path $RunParentRoot $OutputName
if ((Test-Path $RunRoot) -and -not $Resume) {
    throw "Phase E output already exists; refuse to overwrite or adapt: $RunRoot"
}
if ($Resume -and -not (Test-Path $RunRoot)) {
    throw "Phase E resume requested without an existing output root: $RunRoot"
}
New-Item -ItemType Directory -Force -Path $RunRoot | Out-Null

function New-BackendArguments {
    param(
        [string]$Backend,
        [int]$PairCount,
        [string]$CandidateTable,
        [string]$FieldRoot,
        [string]$LabelRoot,
        [string]$OutputRoot,
        [string]$ExecutionPlan,
        [int]$BlockSessions,
        [int]$PairBatchSize,
        [int]$ComputeThreads,
        [int64]$MaxBlockRows,
        [string]$CapacityReceiptHash
    )
    $Arguments = @(
        "scripts\run_cn_phase3cm_streaming_qualification.py",
        "--backend", $Backend,
        "--phase", "E",
        "--pair-count", [string]$PairCount,
        "--candidate-table", $CandidateTable,
        "--binding", $Binding,
        "--split-manifest", $SplitManifest,
        "--artifact-root", $CandidateRoot,
        "--field-sidecar-root", $FieldRoot,
        "--label-sidecar-root", $LabelRoot,
        "--output-root", $OutputRoot,
        "--execution-plan", $ExecutionPlan,
        "--block-sessions", [string]$BlockSessions,
        "--pair-batch-size", [string]$PairBatchSize,
        "--compute-threads", [string]$ComputeThreads,
        "--max-block-rows", [string]$MaxBlockRows,
        "--capacity-receipt-hash", $CapacityReceiptHash
    )
    if ($Resume) { $Arguments += "--resume" }
    return $Arguments
}

$ActiveRoot = Join-Path $RunRoot "active_bar"
$SessionRoot = Join-Path $RunRoot "stock_session"
New-Item -ItemType Directory -Force -Path $ActiveRoot,$SessionRoot | Out-Null
$ActiveComputeThreads = [int]$ActivePlan.compute_threads
$SessionComputeThreads = [int]$SessionPlan.compute_threads
if ($ActiveComputeThreads + $SessionComputeThreads -ne [int]$Contract.global_active_native_compute_threads) {
    throw "combined native thread total drifts from backend plans"
}
$ActivePairBatchSize = [int](($ActivePlan.pair_batches | ForEach-Object { @($_).Count } | Measure-Object -Maximum).Maximum)
$SessionPairBatchSize = [int](($SessionPlan.pair_batches | ForEach-Object { @($_).Count } | Measure-Object -Maximum).Maximum)
$ActiveArgs = New-BackendArguments `
    -Backend "active_bar" `
    -PairCount $ActivePairCount `
    -CandidateTable $ActiveCandidateTable `
    -FieldRoot $ActiveFieldRoot `
    -LabelRoot $ActiveLabelRoot `
    -OutputRoot $ActiveRoot `
    -ExecutionPlan $ActiveExecutionPlanPath `
    -BlockSessions ([int]$ActivePlan.block_size) `
    -PairBatchSize $ActivePairBatchSize `
    -ComputeThreads $ActiveComputeThreads `
    -MaxBlockRows $ActiveMaxBlockRows `
    -CapacityReceiptHash $ActiveCapacityReceiptHash
$SessionArgs = New-BackendArguments `
    -Backend "stock_session" `
    -PairCount $SessionPairCount `
    -CandidateTable $SessionCandidateTable `
    -FieldRoot $SessionFieldRoot `
    -LabelRoot $SessionLabelRoot `
    -OutputRoot $SessionRoot `
    -ExecutionPlan $SessionExecutionPlanPath `
    -BlockSessions ([int]$SessionPlan.block_size) `
    -PairBatchSize $SessionPairBatchSize `
    -ComputeThreads $SessionComputeThreads `
    -MaxBlockRows $SessionMaxBlockRows `
    -CapacityReceiptHash $SessionCapacityReceiptHash

$ActiveCommandPath = Join-Path $ActiveRoot "CN_BACKEND_COMMAND.json"
$SessionCommandPath = Join-Path $SessionRoot "CN_BACKEND_COMMAND.json"
$ActiveExitReceiptPath = Join-Path $ActiveRoot "CN_BACKEND_EXIT_RECEIPT.json"
$SessionExitReceiptPath = Join-Path $SessionRoot "CN_BACKEND_EXIT_RECEIPT.json"
function New-BackendThreadEnvironment([int]$ComputeThreads) {
    return [ordered]@{
    NUMBA_NUM_THREADS = [string]$ComputeThreads
    ARROW_NUM_THREADS = $env:ARROW_NUM_THREADS
    OMP_NUM_THREADS = $env:OMP_NUM_THREADS
    MKL_NUM_THREADS = $env:MKL_NUM_THREADS
    OPENBLAS_NUM_THREADS = $env:OPENBLAS_NUM_THREADS
    NUMEXPR_MAX_THREADS = $env:NUMEXPR_MAX_THREADS
    POLARS_MAX_THREADS = $env:POLARS_MAX_THREADS
    }
}
$ActiveThreadEnvironment = New-BackendThreadEnvironment $ActiveComputeThreads
$SessionThreadEnvironment = New-BackendThreadEnvironment $SessionComputeThreads
Write-CnAtomicJson -Path $ActiveCommandPath -Payload ([ordered]@{ schema_version = "cn_phase3cm_backend_command_v1"; backend = "active_bar"; arguments = $ActiveArgs; thread_environment = $ActiveThreadEnvironment })
Write-CnAtomicJson -Path $SessionCommandPath -Payload ([ordered]@{ schema_version = "cn_phase3cm_backend_command_v1"; backend = "stock_session"; arguments = $SessionArgs; thread_environment = $SessionThreadEnvironment })
$Wrapper = Join-Path $RepoRoot "scripts\invoke_cn_phase3cm_backend_with_exit_receipt.ps1"
$PowerShellExe = (Get-Command powershell.exe).Source
$RssTimelinePath = Join-Path $RunRoot "CN_GLOBAL_PROCESS_TREE_RSS_TIMELINE.csv"
"sampled_at,active_process_ids,active_rss_bytes,session_process_ids,session_rss_bytes,global_rss_bytes" | Set-Content -LiteralPath $RssTimelinePath -Encoding UTF8

$Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$Active = Start-Process -FilePath $PowerShellExe -ArgumentList @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
    "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
    "-ArgumentFile", $ActiveCommandPath, "-ExitReceipt", $ActiveExitReceiptPath
) -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput (Join-Path $ActiveRoot "stdout.log") `
    -RedirectStandardError (Join-Path $ActiveRoot "stderr.log") `
    -WindowStyle Hidden -PassThru
$Session = Start-Process -FilePath $PowerShellExe -ArgumentList @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
    "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
    "-ArgumentFile", $SessionCommandPath, "-ExitReceipt", $SessionExitReceiptPath
) -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput (Join-Path $SessionRoot "stdout.log") `
    -RedirectStandardError (Join-Path $SessionRoot "stderr.log") `
    -WindowStyle Hidden -PassThru
$Processes = @($Active, $Session)
$GlobalPeakRss = [int64]0
$GlobalHardRss = [int64]$ActivePlan.global_rss_hard_bytes
$GlobalGateFailure = $false
$RssSampleCount = 0
$ActivePeakRss = [int64]0
$SessionPeakRss = [int64]0
$Roots = @{ active_bar = $Active.Id; stock_session = $Session.Id }

while ($true) {
    $Running = @()
    foreach ($Process in $Processes) {
        $Process.Refresh()
        if (-not $Process.HasExited) {
            $Running += $Process
        }
    }
    $Snapshot = Get-CnProcessTreeRssSnapshot -Roots $Roots
    Add-CnRssTimelineSample -Path $RssTimelinePath -Snapshot $Snapshot
    $RssSampleCount += 1
    $CurrentGlobalRss = [int64]$Snapshot.total_rss_bytes
    $ActivePeakRss = [Math]::Max($ActivePeakRss, [int64]$Snapshot.groups["active_bar"].rss_bytes)
    $SessionPeakRss = [Math]::Max($SessionPeakRss, [int64]$Snapshot.groups["stock_session"].rss_bytes)
    if ($CurrentGlobalRss -gt $GlobalPeakRss) { $GlobalPeakRss = $CurrentGlobalRss }
    if ($CurrentGlobalRss -ge $GlobalHardRss) {
        $GlobalGateFailure = $true
        Stop-CnProcessTrees -Roots $Roots
        break
    }
    if ($Running.Count -eq 0) { break }
    Start-Sleep -Seconds 2
}
$Stopwatch.Stop()
foreach ($Process in $Processes) { $Process.WaitForExit(); $Process.Refresh() }
$ActiveExit = if (Test-Path $ActiveExitReceiptPath) { Get-Content $ActiveExitReceiptPath -Raw | ConvertFrom-Json } else { $null }
$SessionExit = if (Test-Path $SessionExitReceiptPath) { Get-Content $SessionExitReceiptPath -Raw | ConvertFrom-Json } else { $null }
$ActiveExitCode = if ($null -ne $ActiveExit) { [int]$ActiveExit.exit_code } else { $null }
$SessionExitCode = if ($null -ne $SessionExit) { [int]$SessionExit.exit_code } else { $null }

$ActiveResultPath = Join-Path $ActiveRoot "CN_STREAMING_BACKEND_RESULT.json"
$SessionResultPath = Join-Path $SessionRoot "CN_STREAMING_BACKEND_RESULT.json"
$ActiveResult = if (Test-Path $ActiveResultPath) { Get-Content $ActiveResultPath -Raw | ConvertFrom-Json } else { $null }
$SessionResult = if (Test-Path $SessionResultPath) { Get-Content $SessionResultPath -Raw | ConvertFrom-Json } else { $null }
$WallGatePass = ($WallSecondsHardMax -le 0.0 -or $Stopwatch.Elapsed.TotalSeconds -le $WallSecondsHardMax)
$Pass = (
    -not $GlobalGateFailure -and
    $ActiveExitCode -eq 0 -and
    $SessionExitCode -eq 0 -and
    $null -ne $ActiveResult -and
    $null -ne $SessionResult -and
    $ActiveResult.status -eq "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED" -and
    $SessionResult.status -eq "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED" -and
    [int]$ActiveResult.coordinate_rows_retained -eq 0 -and
    [int]$SessionResult.coordinate_rows_retained -eq 0 -and
    $ActiveResult.parallelism_status -eq "PARALLELISM_ENGAGED" -and
    $SessionResult.parallelism_status -eq "PARALLELISM_ENGAGED" -and
    [int]$ActiveResult.validation_reads -eq 0 -and
    [int]$ActiveResult.holdout_reads -eq 0 -and
    [int]$ActiveResult.forward_2026_reads -eq 0 -and
    [int]$SessionResult.validation_reads -eq 0 -and
    [int]$SessionResult.holdout_reads -eq 0 -and
    [int]$SessionResult.forward_2026_reads -eq 0 -and
    $ActiveResult.block_row_guard_status -eq "BLOCK_ROW_GUARD_PASS" -and
    $SessionResult.block_row_guard_status -eq "BLOCK_ROW_GUARD_PASS" -and
    [string]$ActiveResult.capacity_receipt_hash -eq $ActiveCapacityReceiptHash -and
    [string]$SessionResult.capacity_receipt_hash -eq $SessionCapacityReceiptHash -and
    [int64]$ActiveResult.max_block_rows_contract -eq $ActiveMaxBlockRows -and
    [int64]$SessionResult.max_block_rows_contract -eq $SessionMaxBlockRows -and
    [int64]$ActiveResult.max_observed_block_rows -le $ActiveMaxBlockRows -and
    [int64]$SessionResult.max_observed_block_rows -le $SessionMaxBlockRows -and
    $WallGatePass -and
    $GlobalPeakRss -lt $GlobalHardRss
)
$Receipt = [ordered]@{
    schema_version = "cn_phase3cm_phase_e_combined_execution_receipt_v1"
    status = if ($Pass) { "CN_PHASE3CM_PHASE_E_STRICT_WAVE_PASS" } elseif ($GlobalGateFailure) { "CN_PHASE3CM_PHASE_E_GLOBAL_RSS_GATE_FAIL" } elseif (-not $WallGatePass) { "CN_PHASE3CM_PHASE_E_WALL_GATE_FAIL" } else { "CN_PHASE3CM_PHASE_E_STRICT_WAVE_FAIL" }
    combined_contract = $CombinedContract
    combined_contract_repo_sha = [string]$Contract.repo_sha
    active_exit_code = $ActiveExitCode
    session_exit_code = $SessionExitCode
    wall_seconds = $Stopwatch.Elapsed.TotalSeconds
    wall_seconds_hard_max = if ($WallSecondsHardMax -gt 0.0) { $WallSecondsHardMax } else { $null }
    wall_gate_enforced = $WallSecondsHardMax -gt 0.0
    active_pair_count = $ActivePairCount
    session_pair_count = $SessionPairCount
    total_pair_count = $TotalPairCount
    global_peak_rss_bytes = $GlobalPeakRss
    active_peak_process_tree_rss_bytes = $ActivePeakRss
    session_peak_process_tree_rss_bytes = $SessionPeakRss
    global_hard_rss_bytes = $GlobalHardRss
    rss_monitor = "PROCESS_TREE_RSS"
    rss_sample_count = $RssSampleCount
    rss_timeline = $RssTimelinePath
    rss_timeline_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $RssTimelinePath).Hash.ToLowerInvariant()
    heavy_processes = 2
    compute_threads_per_process = if ($ActiveComputeThreads -eq $SessionComputeThreads) { $ActiveComputeThreads } else { $null }
    compute_threads_by_backend = @{ active_bar = $ActiveComputeThreads; stock_session = $SessionComputeThreads }
    global_active_native_compute_threads = $ActiveComputeThreads + $SessionComputeThreads
    active_result = $ActiveResultPath
    session_result = $SessionResultPath
    active_capacity_receipt = $ActiveCapacityReceipt
    session_capacity_receipt = $SessionCapacityReceipt
    active_capacity_receipt_hash = $ActiveCapacityReceiptHash
    session_capacity_receipt_hash = $SessionCapacityReceiptHash
    predecessor_engineering_receipt = $PredecessorEngineeringReceipt
    predecessor_engineering_receipt_sha256 = $ObservedPredecessorReceiptSha256
    predecessor_engineering_status = [string]$PredecessorReceipt.status
    original_r6_status = [string]$PredecessorReceipt.supersession.original_r6_status
    predecessor_research_pair_csv_sha256 = [string]$PredecessorReceipt.research_parity.pair_csv_sha256
    source_closure_manifest = $SourceClosureManifest
    source_closure_manifest_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $SourceClosureManifest).Hash.ToLowerInvariant()
    source_closure_manifest_hash = [string]$ActiveCapacityValidation.source_closure_manifest_hash
    active_max_block_rows = $ActiveMaxBlockRows
    session_max_block_rows = $SessionMaxBlockRows
    active_exit_receipt = $ActiveExitReceiptPath
    session_exit_receipt = $SessionExitReceiptPath
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    promotion = "FORBIDDEN"
    strict_stage_a = "NOT_AUTHORIZED"
}
$ReceiptPath = Join-Path $RunRoot "CN_PHASE_E_COMBINED_EXECUTION_RECEIPT.json"
$Temporary = $ReceiptPath + ".tmp"
$Receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Temporary -Encoding UTF8
Move-Item -LiteralPath $Temporary -Destination $ReceiptPath -Force
$Receipt | ConvertTo-Json -Compress
if (-not $Pass) { throw "Phase E strict wave failed closed" }
