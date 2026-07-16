param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("active_bar", "stock_session")]
    [string]$Backend,
    [Parameter(Mandatory = $true)]
    [ValidateSet("C", "D", "E")]
    [string]$Phase,
    [Parameter(Mandatory = $true)]
    [int]$PairCount,
    [Parameter(Mandatory = $true)]
    [string]$OutputName,
    [string]$ExecutionPlan = "",
    [int]$ComputeThreads = 16,
    [int]$BlockSessions = 5,
    [int]$PairBatchSize = 4,
    [int]$StopAfterBlocks = 0,
    [switch]$Resume,
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
if ($ComputeThreads -lt 1 -or $ComputeThreads -gt 24) { throw "ComputeThreads must be in [1,24]" }
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
if ($Backend -eq "active_bar") {
    $CandidateTable = Join-Path $CandidateRoot "preflight_active_candidates.csv"
    $FieldRoot = Join-Path $RuntimeRoot "time_major_v1"
    $LabelRoot = Join-Path $RuntimeRoot "time_major_v1_labels"
}
else {
    $CandidateTable = Join-Path $CandidateRoot "preflight_session_candidates.csv"
    $FieldRoot = Join-Path $RuntimeRoot "session_time_major_v1"
    $LabelRoot = Join-Path $RuntimeRoot "session_time_major_v1_labels"
}
$Arguments = @(
    "scripts\run_cn_phase3cm_streaming_qualification.py",
    "--backend", $Backend,
    "--phase", $Phase,
    "--pair-count", [string]$PairCount,
    "--candidate-table", $CandidateTable,
    "--binding", (Join-Path $RuntimeRoot "CN_STREAMING_REPAIR_FROZEN_INPUT_BINDING.json"),
    "--artifact-root", $CandidateRoot,
    "--field-sidecar-root", $FieldRoot,
    "--label-sidecar-root", $LabelRoot,
    "--output-root", (Join-Path $RuntimeRoot $OutputName),
    "--block-sessions", [string]$BlockSessions,
    "--pair-batch-size", [string]$PairBatchSize,
    "--compute-threads", [string]$ComputeThreads
)
if ($StopAfterBlocks -gt 0) { $Arguments += @("--stop-after-blocks", [string]$StopAfterBlocks) }
if ($Phase -eq "E") {
    if (-not $ExecutionPlan) { throw "Phase E requires -ExecutionPlan" }
    $Arguments += @("--execution-plan", $ExecutionPlan)
}
if ($Resume) { $Arguments += "--resume" }

Push-Location $RepoRoot
try {
    & $PythonExe @Arguments
    if ($LASTEXITCODE -ne 0) { throw "streaming backend failed: $LASTEXITCODE" }
}
finally {
    Pop-Location
}
