param(
  [string]$Repo = "D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260710_phase3ga_semantic_efficiency_v2",
  [string]$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe",
  [string]$ShardRoot = "D:\ChengboRemote\data\phase3fix_true1min_2024_2025_sidecar_augmented_shards_20260709",
  [string]$CoRoot = "D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill\runtime\phase3er2_direct_cm_parallel_20260706_77o",
  [string]$RunRoot = "D:\ChengboRemote\runtime\phase3ga_semantic_efficiency_v2_20260710_77o",
  [string]$ReportRoot = "D:\ChengboRemote\runtime\reports\phase3ga_semantic_efficiency_v2_20260710_77o",
  [string]$PersistentCacheRoot = "D:\ChengboRemote\cache\phase3ga_2024_2025_cm_bounded_v2",
  [int]$GenerationBudget = 24576,
  [int]$RescheduleBudget = 24576,
  [int]$CaTopN = 1536,
  [int]$CmCandidateLimit = 384,
  [int]$CmWorkers = 4
)

$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $Repo
$env:PYTHONPATH = "$Repo\src;$env:PYTHONPATH"
$env:NUMEXPR_MAX_THREADS = "2"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"

$ArmBudget = Join-Path $CoRoot "phase3cp_real_cm_next_arm_budget_table.csv"
$MemoryRoot = Join-Path $Repo "runtime\search_memory"
$ExpectedPanelRel = "phase3aq_wide_true1min\canary\phase3aq_true_1min_formula_canary.parquet"
foreach ($path in @($Python, $Repo, $ShardRoot, $ArmBudget)) {
  if (-not (Test-Path -LiteralPath $path)) { throw "required path missing: $path" }
}
$PanelCount = @(Get-ChildItem -LiteralPath $ShardRoot -Directory -Filter "shard_*" | Where-Object {
  Test-Path -LiteralPath (Join-Path $_.FullName $ExpectedPanelRel)
}).Count
if ($PanelCount -ne 16) { throw "repaired true1min root must contain 16/16 panels; found $PanelCount" }

New-Item -ItemType Directory -Force -Path $RunRoot, $ReportRoot, $PersistentCacheRoot | Out-Null
& $Python -m py_compile `
  "src\our_system_phase2\runtime\phase3cp_real_cm_small_loop.py" `
  "src\our_system_phase2\runtime\phase3cm_train_portfolio_sortino_reward_audit.py" `
  "src\our_system_phase2\services\expression_semantics.py" `
  "src\our_system_phase2\services\signal_vector_semantics.py"
if ($LASTEXITCODE -ne 0) { throw "Phase3GA py_compile failed" }

$manifest = [ordered]@{
  phase = "Phase3GA"
  machine = $env:COMPUTERNAME
  repo = $Repo
  shard_root = $ShardRoot
  data_contract = "true1min 2024-2025 train/search; 2026 separate forward OOS; old 1D forbidden"
  panel_count = $PanelCount
  generation_budget = $GenerationBudget
  cm_candidate_limit = $CmCandidateLimit
  cm_workers = $CmWorkers
  cm_parallel_axis = "shard"
  semantic_construction_gate = $true
  pre_cm_semantic_only_gate = $true
  signal_equivalence = "rank correlation plus long/low bucket overlap"
  operator_cache_max_mb_per_process = 1536
  feature_matrix_cache_max_mb_per_process = 3072
  checkpoint_bootstrap_iterations = 128
  final_bootstrap_iterations = 600
  bootstrap_engine = "numpy_vectorized_v1"
  validation_usage = "report_only"
  holdout_usage = "report_only"
  year_2026_usage = "forbidden_during_search"
  plate_membership_claim = "none"
  started_at = (Get-Date).ToString("o")
}
$manifest | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $RunRoot "phase3ga_run_manifest.json") -Encoding UTF8

$argsList = @(
  "-m", "our_system_phase2.runtime.phase3cp_real_cm_small_loop",
  "--co-root", $CoRoot,
  "--arm-budget-table", $ArmBudget,
  "--shard-root", $ShardRoot,
  "--output-root", $RunRoot,
  "--report-root", $ReportRoot,
  "--generation-budget", "$GenerationBudget",
  "--shortfall-fill-rounds", "10",
  "--shortfall-oversample-multiplier", "5.5",
  "--ca-top-n", "$CaTopN",
  "--cm-candidate-limit", "$CmCandidateLimit",
  "--cm-selection-mode", "arm_balanced",
  "--cm-max-shards", "12",
  "--cm-sample-trade-times-per-shard", "256",
  "--cm-event-aware-sample-times",
  "--cm-event-sample-trade-times-per-shard", "384",
  "--cm-horizons", "1,5,10,15",
  "--cm-train-fraction", "0.75",
  "--cm-validation-fraction", "0.15",
  "--cm-min-obs-per-time", "20",
  "--cm-cost-bps", "5",
  "--cm-top-quantile", "0.2",
  "--cm-rank-ic-loss-weight", "6.0",
  "--cm-rank-ic-component-cap", "0.35",
  "--cm-regime-stability-weight", "0.06",
  "--cm-regime-component-cap", "0.08",
  "--cm-operator-cache-max-entries", "256",
  "--cm-operator-cache-max-mb", "1536",
  "--cm-feature-matrix-cache-max-windows", "4",
  "--cm-feature-matrix-cache-max-mb", "3072",
  "--cm-persistent-cache-root", $PersistentCacheRoot,
  "--cm-persistent-cache-mode", "readwrite",
  "--cm-persistent-cache-min-free-gb", "220",
  "--cm-persistent-cache-max-gb", "180",
  "--cm-persistent-cache-ttl-days", "7",
  "--cm-checkpoint-every-candidates", "8",
  "--cm-checkpoint-bootstrap-iterations", "128",
  "--pre-cm-semantic-gate",
  "--pre-cm-semantic-oversample-multiplier", "2.0",
  "--pre-cm-semantic-max-shards", "1",
  "--pre-cm-semantic-sample-trade-times-per-shard", "32",
  "--pre-cm-semantic-event-sample-trade-times-per-shard", "96",
  "--pre-cm-semantic-operator-cache-max-mb", "768",
  "--pre-cm-semantic-feature-matrix-cache-max-mb", "1536",
  "--cm-workers", "$CmWorkers",
  "--cm-parallel-axis", "shard",
  "--numexpr-threads", "2",
  "--min-clean-feedback", "3",
  "--reschedule-total-budget", "$RescheduleBudget",
  "--memory-root", $MemoryRoot
)

$ConsoleLog = Join-Path $RunRoot "phase3ga_console.log"
& $Python @argsList 2>&1 | Tee-Object -FilePath $ConsoleLog
$exitCode = $LASTEXITCODE
[ordered]@{
  phase = "Phase3GA"
  python_exit_code = $exitCode
  completed_at = (Get-Date).ToString("o")
  console_log = $ConsoleLog
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $RunRoot "phase3ga_exit.json") -Encoding UTF8
if ($exitCode -ne 0) { throw "Phase3GA search failed with exit code $exitCode" }
