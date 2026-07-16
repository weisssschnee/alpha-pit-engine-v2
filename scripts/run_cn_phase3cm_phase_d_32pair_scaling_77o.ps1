param(
    [string]$OutputName = "phase_d_scale_32pairs_t3_b4",
    [int]$ComputeThreads = 3,
    [int]$BlockSessions = 10,
    [int]$PairBatchSize = 4,
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
if ($ComputeThreads -ne 3) { throw "qualified Phase D compute threads are frozen at 3" }
if ($BlockSessions -ne 10) { throw "qualified Phase D block size is frozen at 10 sessions" }
if ($PairBatchSize -ne 4) { throw "qualified Phase D pair batch size is frozen at 4" }
if (2 * $ComputeThreads -gt 24) { throw "global native compute thread budget exceeds 24" }

$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = [string]$ComputeThreads
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = "1"

$RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716"
$CandidateRoot = Join-Path $RepoRoot "runtime\cn_compositional_nline_large_search_20260715"
$SplitManifest = Join-Path $RepoRoot "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$Binding = Join-Path $RuntimeRoot "CN_STREAMING_REPAIR_FROZEN_INPUT_BINDING.json"
$RunRoot = Join-Path $RuntimeRoot $OutputName
if (Test-Path $RunRoot) { throw "Phase D 32-pair output already exists: $RunRoot" }
New-Item -ItemType Directory -Force -Path $RunRoot | Out-Null

function New-BackendArguments {
    param(
        [string]$Backend,
        [int]$PairCount,
        [string]$CandidateTable,
        [string]$FieldRoot,
        [string]$LabelRoot,
        [string]$OutputRoot
    )
    return @(
        "scripts\run_cn_phase3cm_streaming_qualification.py",
        "--backend", $Backend,
        "--phase", "D",
        "--pair-count", [string]$PairCount,
        "--candidate-table", $CandidateTable,
        "--binding", $Binding,
        "--split-manifest", $SplitManifest,
        "--artifact-root", $CandidateRoot,
        "--field-sidecar-root", $FieldRoot,
        "--label-sidecar-root", $LabelRoot,
        "--output-root", $OutputRoot,
        "--block-sessions", [string]$BlockSessions,
        "--pair-batch-size", [string]$PairBatchSize,
        "--compute-threads", [string]$ComputeThreads
    )
}

$ActiveRoot = Join-Path $RunRoot "active_bar"
$SessionRoot = Join-Path $RunRoot "stock_session"
New-Item -ItemType Directory -Force -Path $ActiveRoot,$SessionRoot | Out-Null
$ActiveArgs = New-BackendArguments `
    -Backend "active_bar" `
    -PairCount 18 `
    -CandidateTable (Join-Path $CandidateRoot "preflight_active_candidates.csv") `
    -FieldRoot (Join-Path $RuntimeRoot "time_major_train_v2") `
    -LabelRoot (Join-Path $RuntimeRoot "time_major_train_v3_labels") `
    -OutputRoot $ActiveRoot
$SessionArgs = New-BackendArguments `
    -Backend "stock_session" `
    -PairCount 14 `
    -CandidateTable (Join-Path $CandidateRoot "preflight_session_candidates.csv") `
    -FieldRoot (Join-Path $RuntimeRoot "session_time_major_train_v2") `
    -LabelRoot (Join-Path $RuntimeRoot "session_time_major_train_v3_labels") `
    -OutputRoot $SessionRoot

$Stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
$Active = Start-Process -FilePath $PythonExe -ArgumentList $ActiveArgs -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput (Join-Path $ActiveRoot "stdout.log") `
    -RedirectStandardError (Join-Path $ActiveRoot "stderr.log") `
    -WindowStyle Hidden -PassThru
$Session = Start-Process -FilePath $PythonExe -ArgumentList $SessionArgs -WorkingDirectory $RepoRoot `
    -RedirectStandardOutput (Join-Path $SessionRoot "stdout.log") `
    -RedirectStandardError (Join-Path $SessionRoot "stderr.log") `
    -WindowStyle Hidden -PassThru
$Processes = @($Active, $Session)
$GlobalPeakRss = [int64]0
$GlobalHardRss = [int64](60 * 1024 * 1024 * 1024)
$GlobalGateFailure = $false

while ($true) {
    $Running = @()
    $CurrentGlobalRss = [int64]0
    foreach ($Process in $Processes) {
        $Process.Refresh()
        if (-not $Process.HasExited) {
            $Running += $Process
            $CurrentGlobalRss += [int64]$Process.WorkingSet64
        }
    }
    if ($CurrentGlobalRss -gt $GlobalPeakRss) { $GlobalPeakRss = $CurrentGlobalRss }
    if ($CurrentGlobalRss -ge $GlobalHardRss) {
        $GlobalGateFailure = $true
        foreach ($Process in $Running) { Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue }
        break
    }
    if ($Running.Count -eq 0) { break }
    Start-Sleep -Seconds 2
}
$Stopwatch.Stop()
foreach ($Process in $Processes) { $Process.WaitForExit(); $Process.Refresh() }

$ActiveResultPath = Join-Path $ActiveRoot "CN_STREAMING_BACKEND_RESULT.json"
$SessionResultPath = Join-Path $SessionRoot "CN_STREAMING_BACKEND_RESULT.json"
$ActiveResult = if (Test-Path $ActiveResultPath) { Get-Content $ActiveResultPath -Raw | ConvertFrom-Json } else { $null }
$SessionResult = if (Test-Path $SessionResultPath) { Get-Content $SessionResultPath -Raw | ConvertFrom-Json } else { $null }
$Pass = (
    -not $GlobalGateFailure -and
    $Active.ExitCode -eq 0 -and
    $Session.ExitCode -eq 0 -and
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
    $GlobalPeakRss -lt $GlobalHardRss
)
$Receipt = [ordered]@{
    schema_version = "cn_phase3cm_phase_d_32pair_scaling_receipt_v1"
    status = if ($Pass) { "CN_PHASE3CM_PHASE_D_32PAIR_SCALING_PASS" } elseif ($GlobalGateFailure) { "CN_PHASE3CM_PHASE_D_GLOBAL_RSS_GATE_FAIL" } else { "CN_PHASE3CM_PHASE_D_32PAIR_SCALING_FAIL" }
    active_exit_code = $Active.ExitCode
    session_exit_code = $Session.ExitCode
    wall_seconds = $Stopwatch.Elapsed.TotalSeconds
    global_peak_rss_bytes = $GlobalPeakRss
    global_hard_rss_bytes = $GlobalHardRss
    heavy_processes = 2
    compute_threads_per_process = $ComputeThreads
    global_active_native_compute_threads = 2 * $ComputeThreads
    active_result = $ActiveResultPath
    session_result = $SessionResultPath
    active_execution_plan = Join-Path $ActiveRoot "CN_FROZEN_EXECUTION_PLAN.json"
    session_execution_plan = Join-Path $SessionRoot "CN_FROZEN_EXECUTION_PLAN.json"
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    promotion = "FORBIDDEN"
    strict_stage_a = "NOT_AUTHORIZED"
}
$ReceiptPath = Join-Path $RunRoot "CN_PHASE_D_32PAIR_SCALING_RECEIPT.json"
$Temporary = $ReceiptPath + ".tmp"
$Receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Temporary -Encoding UTF8
Move-Item -LiteralPath $Temporary -Destination $ReceiptPath -Force
$Receipt | ConvertTo-Json -Compress
if (-not $Pass) { throw "Phase D 32-pair scaling failed closed" }
