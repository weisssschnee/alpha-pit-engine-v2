param(
    [string]$RepoRoot = "G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619",
    [string]$PythonExe = "G:\PythonProject\.venv\Scripts\python.exe",
    [string]$ShardRoot = "G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3au_aq_only_true1min_sharded_20260611",
    [switch]$CompanyMode,
    [switch]$RunSearch
)

$ErrorActionPreference = "Stop"

if ($CompanyMode) {
    $RepoRoot = "D:\HermesWorker\workspace\alpha_pit_true1min_engine_20260619"
    $PythonExe = "D:\HermesWorker\workspace\.venv\Scripts\python.exe"
    $ShardRoot = "D:\HermesWorker\workspace\phase3aj_new_data_current\runtime\phase3au_company_full_true1min_sharded_20260611"
}

Set-Location $RepoRoot
$env:PYTHONPATH = "src"

Write-Host "[Phase3CF] prelaunch validation"
& $PythonExe app.py phase3cf-large-search-prelaunch --allow-diagnostic
if ($LASTEXITCODE -ne 0) {
    throw "Phase3CF prelaunch failed with exit code $LASTEXITCODE"
}

if (-not $RunSearch) {
    Write-Host "[Phase3CF] prelaunch complete. Re-run with -RunSearch to launch heavy search."
    exit 0
}

$bsOut = "runtime/phase3cf_bs_adaptive_ucb_cem_20260618"
$bsRep = "reports/phase3cf_bs_adaptive_ucb_cem_20260618"
$btOut = "runtime/phase3cf_bt_ast_fresh_20260618"
$btRep = "reports/phase3cf_bt_ast_fresh_20260618"
$caOut = "runtime/phase3cf_bz_candidate_audit_20260618"
$bzOut = "runtime/phase3cf_bz_fragment_replay_20260618"
$bzRep = "reports/phase3cf_bz_fragment_replay_20260618"

if ($CompanyMode) {
    $maxShards = 16
    $sampleTimes = 96
    $seed = 512
    $each = 512
    $top = 320
    $bzLimit = 128
    $bzSample = 180
}
else {
    $maxShards = 8
    $sampleTimes = 64
    $seed = 256
    $each = 256
    $top = 160
    $bzLimit = 64
    $bzSample = 120
}

Write-Host "[Phase3CF] lane A: BS adaptive UCB/CEM"
& $PythonExe app.py phase3bs-adaptive-ucb-cem-practice --allow-diagnostic -- `
    --shard-root $ShardRoot `
    --memory-root runtime/search_memory `
    --output-root $bsOut `
    --report-root $bsRep `
    --seed-candidates $seed `
    --adaptive-cem-candidates $each `
    --adaptive-hybrid-candidates $each `
    --cem-dominant-ucb-candidates $each `
    --cem-dominant-rx-candidates $each `
    --max-shards $maxShards `
    --sample-trade-times-per-shard $sampleTimes `
    --top-decisions $top `
    --horizons 1,5,15,30 `
    --min-obs-per-time 20 `
    --seed-exploration 0.35 `
    --learning-rate 0.20 `
    --entropy-floor 0.18 `
    --min-feedback-eligible 32
if ($LASTEXITCODE -ne 0) {
    throw "Phase3CF BS lane failed with exit code $LASTEXITCODE"
}

Write-Host "[Phase3CF] lane B: BT AST fresh"
& $PythonExe app.py phase3bt-ast-algorithm-bakeoff --allow-diagnostic -- `
    --shard-root $ShardRoot `
    --memory-root runtime/search_memory `
    --output-root $btOut `
    --report-root $btRep `
    --max-shards $maxShards `
    --sample-trade-times-per-shard $sampleTimes `
    --seed-candidates $seed `
    --cem-candidates $each `
    --hybrid-candidates $each `
    --dominant-candidates $each `
    --fresh-hybrid-candidates $each `
    --top-decisions $top `
    --horizons 1,5,15,30 `
    --min-obs-per-time 20 `
    --seed-exploration 0.45 `
    --learning-rate 0.20 `
    --entropy-floor 0.20 `
    --min-feedback-eligible 32
if ($LASTEXITCODE -ne 0) {
    throw "Phase3CF BT lane failed with exit code $LASTEXITCODE"
}

Write-Host "[Phase3CF] lane C: CA bridge to BZ input"
& $PythonExe app.py phase3ca-build-bz-candidate-audit --allow-diagnostic -- `
    --source-root $bsRep `
    --source-root $btRep `
    --output-root $caOut `
    --top-n $bzLimit
if ($LASTEXITCODE -ne 0) {
    throw "Phase3CF CA bridge failed with exit code $LASTEXITCODE"
}

Write-Host "[Phase3CF] lane D: BZ fragment replay"
& $PythonExe app.py phase3bz-fragment-replay-audit --allow-diagnostic -- `
    --bx-audit "$caOut\phase3ca_bz_candidate_audit.csv" `
    --shard-root $ShardRoot `
    --output-root $bzOut `
    --report-root $bzRep `
    --candidate-limit $bzLimit `
    --max-shards $maxShards `
    --sample-trade-times-per-shard $bzSample `
    --horizons 1,5,15,30 `
    --min-obs-per-time 20 `
    --cost-bps 5 `
    --numexpr-threads 8
if ($LASTEXITCODE -ne 0) {
    throw "Phase3CF BZ replay failed with exit code $LASTEXITCODE"
}

Write-Host "[Phase3CF] complete. Inspect $bzRep"
