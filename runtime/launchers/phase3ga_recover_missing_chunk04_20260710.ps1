param(
  [string]$Repo = "D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260710_phase3ga_semantic_efficiency_v2",
  [string]$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
  [string]$ShardRoot = "D:\ChengboRemote\data\phase3fix_true1min_2024_2025_sidecar_augmented_shards_20260709",
  [string]$SourceRunRoot = "D:\ChengboRemote\runtime\phase3fix_repaired_2y_train75_large_search_20260710_77o_scheduled_w4s12",
  [string]$RecoveryRoot = "D:\ChengboRemote\runtime\phase3ga_recover_missing_chunk04_20260710_77o",
  [string]$CacheRoot = "D:\ChengboRemote\cache\phase3ga_chunk04_recovery_bounded",
  [string]$SplitManifest = "D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260710_phase3ga_semantic_efficiency_v2\runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv",
  [string]$UnifiedRegistry = "D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260710_phase3ga_semantic_efficiency_v2\reports\cn_unified_capability_discovery_20260714\completed_f8169e1\registry\unified_capability_registry.json",
  [string]$CandidateReceiptTable = "D:\ChengboRemote\runtime\phase3fix_repaired_2y_train75_large_search_20260710_77o_scheduled_w4s12\candidate_submission_receipts.jsonl",
  [string]$DataReleaseHash = "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827",
  [int]$Workers = 4
)

$ErrorActionPreference = "Stop"
$CandidateTable = Join-Path $SourceRunRoot "phase3cp_real_cm_candidate_audit_semantic.csv"
foreach ($path in @($Repo, $Python, $ShardRoot, $CandidateTable, $SplitManifest, $UnifiedRegistry, $CandidateReceiptTable)) {
  if (-not (Test-Path -LiteralPath $path)) { throw "required path missing: $path" }
}
if ($Workers -lt 1 -or $Workers -gt 6) { throw "Workers must be between 1 and 6" }

Set-Location -LiteralPath $Repo
$env:PYTHONPATH = "$Repo\src;$Repo"
$env:NUMEXPR_MAX_THREADS = "2"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
New-Item -ItemType Directory -Force -Path $RecoveryRoot, $CacheRoot | Out-Null

$Candidates = @(Import-Csv -LiteralPath $CandidateTable)
if ($Candidates.Count -ne 384) { throw "expected 384 candidates, found $($Candidates.Count)" }
$processes = @()
$workerDirs = @()
for ($worker = 0; $worker -lt $Workers; $worker++) {
  $workerId = $worker + 1
  $workerRoot = Join-Path $RecoveryRoot ("candidate_worker_{0:d2}" -f $workerId)
  $workerReport = Join-Path $RecoveryRoot ("reports\candidate_worker_{0:d2}" -f $workerId)
  New-Item -ItemType Directory -Force -Path $workerRoot, $workerReport | Out-Null
  $workerTable = Join-Path $workerRoot "candidate_chunk.csv"
  $selected = for ($index = $worker; $index -lt $Candidates.Count; $index += $Workers) { $Candidates[$index] }
  $selected | Export-Csv -LiteralPath $workerTable -NoTypeInformation -Encoding UTF8
  $stdout = Join-Path $workerRoot "stdout.log"
  $stderr = Join-Path $workerRoot "stderr.log"
  $argsList = @(
    "-m", "our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit",
    "--candidate-audit", $workerTable,
    "--shard-root", $ShardRoot,
    "--output-root", $workerRoot,
    "--report-root", $workerReport,
    "--candidate-limit", "$($selected.Count)",
    "--max-shards", "12",
    "--shard-indices", "3,7,11",
    "--sample-trade-times-per-shard", "256",
    "--event-aware-sample-times",
    "--event-sample-trade-times-per-shard", "384",
    "--horizons", "1,5,10,15",
    "--train-fraction", "0.75",
    "--validation-fraction", "0.15",
    "--split-manifest", $SplitManifest,
    "--candidate-receipt-table", $CandidateReceiptTable,
    "--unified-registry", $UnifiedRegistry,
    "--data-release-hash", $DataReleaseHash,
    "--min-obs-per-time", "20",
    "--cost-bps", "5",
    "--top-quantile", "0.2",
    "--portfolio-mode", "long_only_top",
    "--rank-ic-loss-weight", "6.0",
    "--rank-ic-component-cap", "0.35",
    "--regime-stability-weight", "0.06",
    "--regime-component-cap", "0.08",
    "--operator-cache-max-entries", "192",
    "--operator-cache-max-mb", "1280",
    "--feature-matrix-cache-max-windows", "3",
    "--feature-matrix-cache-max-mb", "2560",
    "--persistent-cache-root", $CacheRoot,
    "--persistent-cache-mode", "readwrite",
    "--persistent-cache-min-free-gb", "220",
    "--persistent-cache-max-gb", "120",
    "--persistent-cache-ttl-days", "7",
    "--checkpoint-every-candidates", "8",
    "--checkpoint-bootstrap-iterations", "128",
    "--fast-mode",
    "--disable-schema-gate",
    "--write-reward-atoms"
  )
  $process = Start-Process -FilePath $Python -ArgumentList $argsList -RedirectStandardOutput $stdout -RedirectStandardError $stderr -PassThru -WindowStyle Hidden
  $processes += $process
  $workerDirs += $workerRoot
}

[ordered]@{
  phase = "Phase3GA"
  purpose = "recover old run missing shard chunk 04 without replaying completed chunks 01-03"
  source_run_root = $SourceRunRoot
  missing_shard_indices = "3,7,11"
  candidate_count = $Candidates.Count
  workers = $Workers
  repo = $Repo
  cache_root = $CacheRoot
  worker_pids = @($processes | ForEach-Object { $_.Id })
  started_at = (Get-Date).ToString("o")
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $RecoveryRoot "phase3ga_chunk04_recovery_manifest.json") -Encoding UTF8

$processes | Wait-Process
$failed = @($processes | Where-Object { $_.ExitCode -ne 0 })
if ($failed.Count) {
  $ids = ($failed | ForEach-Object { "$($_.Id):$($_.ExitCode)" }) -join ","
  throw "Phase3GA chunk04 recovery worker failure: $ids"
}

$ExactOutput = Join-Path $SourceRunRoot "phase3cm_train_reward_exact_recovered_phase3ga"
$mergeArgs = @(
  (Join-Path $Repo "scripts\recover_phase3cm_exact_reward_atoms.py"),
  "--candidate-table", $CandidateTable,
  "--output-root", $ExactOutput,
  "--expected-shard-count", "12",
  "--horizons", "1,5,10,15",
  "--train-fraction", "0.75",
  "--validation-fraction", "0.15",
  "--split-manifest", $SplitManifest,
  "--candidate-receipt-table", $CandidateReceiptTable,
  "--unified-registry", $UnifiedRegistry,
  "--data-release-hash", $DataReleaseHash,
  "--chunk-dir", (Join-Path $SourceRunRoot "phase3cm_train_reward_shard_chunks\shard_chunk_01"),
  "--chunk-dir", (Join-Path $SourceRunRoot "phase3cm_train_reward_shard_chunks\shard_chunk_02"),
  "--chunk-dir", (Join-Path $SourceRunRoot "phase3cm_train_reward_shard_chunks\shard_chunk_03")
)
foreach ($workerDir in $workerDirs) { $mergeArgs += @("--chunk-dir", $workerDir) }
& $Python @mergeArgs
if ($LASTEXITCODE -ne 0) { throw "exact reward-atom merge failed" }

[ordered]@{
  phase = "Phase3GA"
  decision = "PHASE3GA_MISSING_CHUNK04_RECOVERED_AND_EXACT_12_SHARD_MERGE_READY"
  exact_output = $ExactOutput
  completed_at = (Get-Date).ToString("o")
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $RecoveryRoot "phase3ga_chunk04_recovery_completion.json") -Encoding UTF8
