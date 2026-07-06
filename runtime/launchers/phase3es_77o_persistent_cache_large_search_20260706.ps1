$ErrorActionPreference = "Stop"

$Repo = "D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill"
$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
$ShardRoot = "D:\ChengboRemote\data\phase3dz_true1min_sidecar_augmented_full16_20260702"
$CoRoot = Join-Path $Repo "runtime\phase3er2_direct_cm_parallel_20260706_77o"
$ArmBudgetTable = Join-Path $CoRoot "phase3cp_real_cm_next_arm_budget_table.csv"
$RunRoot = Join-Path $Repo "runtime\phase3es_persistent_cache_large_search_20260706_77o"
$ReportRoot = Join-Path $Repo "reports\phase3es_persistent_cache_large_search_20260706_77o"
$PersistentCacheRoot = "D:\ChengboRemote\cache\phase3cm_persistent_series_cache"

$MemoryRoot = Join-Path $Repo "runtime\search_memory"
$EpRunRoot = Join-Path $Repo "runtime\phase3ep_medium_search_20260705_77o"
$EqRoot = Join-Path $Repo "runtime\phase3eq_robust_followup_validation_20260706_77o"
$AfterburnerRoot = Join-Path $Repo "runtime\phase3eq_afterburner_deep_validation_20260706_77o"
$ErStoppedRoot = Join-Path $Repo "runtime\phase3er_fresh_large_round1_20260706_77o"
$Er2Root = Join-Path $Repo "runtime\phase3er2_direct_cm_parallel_20260706_77o"

Set-Location -LiteralPath $Repo
$env:PYTHONPATH = "$Repo\src;$env:PYTHONPATH"
$env:NUMEXPR_MAX_THREADS = "2"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"

if (-not (Test-Path -LiteralPath $ShardRoot)) {
  throw "true1min shard root missing: $ShardRoot"
}
if (-not (Test-Path -LiteralPath $ArmBudgetTable)) {
  throw "arm budget table missing: $ArmBudgetTable"
}

New-Item -ItemType Directory -Force -Path $RunRoot, $ReportRoot, $PersistentCacheRoot | Out-Null

& $Python -m py_compile "src\our_system_phase2\runtime\phase3cp_real_cm_small_loop.py" "src\our_system_phase2\runtime\phase3cm_train_portfolio_sortino_reward_audit.py"
if ($LASTEXITCODE -ne 0) {
  throw "py_compile failed"
}

$argsList = @(
  "-m", "our_system_phase2.runtime.phase3cp_real_cm_small_loop",
  "--co-root", $CoRoot,
  "--arm-budget-table", $ArmBudgetTable,
  "--shard-root", $ShardRoot,
  "--output-root", $RunRoot,
  "--report-root", $ReportRoot,
  "--generation-budget", "12288",
  "--shortfall-fill-rounds", "8",
  "--shortfall-oversample-multiplier", "4.5",
  "--ca-top-n", "1024",
  "--cm-candidate-limit", "256",
  "--cm-selection-mode", "arm_balanced",
  "--cm-max-shards", "8",
  "--cm-sample-trade-times-per-shard", "192",
  "--cm-event-aware-sample-times",
  "--cm-event-sample-trade-times-per-shard", "256",
  "--cm-horizons", "1,5,10,15",
  "--cm-train-fraction", "0.60",
  "--cm-validation-fraction", "0.20",
  "--cm-min-obs-per-time", "20",
  "--cm-cost-bps", "5",
  "--cm-top-quantile", "0.2",
  "--cm-rank-ic-loss-weight", "6.0",
  "--cm-rank-ic-component-cap", "0.35",
  "--cm-regime-stability-weight", "0.06",
  "--cm-regime-component-cap", "0.08",
  "--cm-operator-cache-max-entries", "512",
  "--cm-feature-matrix-cache-max-windows", "6",
  "--cm-persistent-cache-root", $PersistentCacheRoot,
  "--cm-persistent-cache-mode", "readwrite",
  "--cm-workers", "8",
  "--cm-parallel-axis", "shard",
  "--numexpr-threads", "2",
  "--min-clean-feedback", "3",
  "--reschedule-total-budget", "12288",
  "--memory-root", $MemoryRoot,
  "--memory-root", $EpRunRoot,
  "--memory-root", $EqRoot,
  "--memory-root", $AfterburnerRoot,
  "--memory-root", $ErStoppedRoot,
  "--memory-root", $Er2Root,
  "--no-pre-cm-semantic-gate"
)

[pscustomobject]@{
  phase = "Phase3ES"
  run_type = "persistent_cache_large_true1min_reward_search"
  repo = $Repo
  shard_root = $ShardRoot
  run_root = $RunRoot
  report_root = $ReportRoot
  co_root = $CoRoot
  arm_budget_table = $ArmBudgetTable
  persistent_cache_root = $PersistentCacheRoot
  memory_roots = @($MemoryRoot, $EpRunRoot, $EqRoot, $AfterburnerRoot, $ErStoppedRoot, $Er2Root)
  generation_budget = 12288
  ca_top_n = 1024
  cm_candidate_limit = 256
  cm_max_shards = 8
  cm_workers = 8
  cm_parallel_axis = "shard"
  cm_horizons = "1,5,10,15"
  pre_cm_semantic_gate = $false
  reward = "train_portfolio_sortino_plus_rankic_loss_plus_low_weight_regime_stability"
  validation_holdout = "report_only"
  old_1d_kline_allowed = $false
  x0_r3_read_only = $true
  started_at = (Get-Date).ToString("o")
} | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $RunRoot "phase3es_run_manifest.json") -Encoding UTF8

& $Python @argsList 2>&1 | Tee-Object -FilePath (Join-Path $RunRoot "phase3es_console.log")
if ($LASTEXITCODE -ne 0) {
  throw "Phase3ES persistent cache large search failed with exit code $LASTEXITCODE"
}

[pscustomobject]@{
  phase = "Phase3ES"
  completed_at = (Get-Date).ToString("o")
  run_root = $RunRoot
  report_root = $ReportRoot
} | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $RunRoot "phase3es_completion.json") -Encoding UTF8
