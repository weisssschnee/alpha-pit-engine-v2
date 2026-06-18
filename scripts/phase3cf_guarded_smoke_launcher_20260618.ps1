param(
    [string]$RepoRoot = "G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619",
    [string]$PythonExe = "G:\PythonProject\.venv\Scripts\python.exe",
    [string]$ShardRoot = "G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3au_aq_only_true1min_sharded_20260611",
    [switch]$CompanyMode
)

$ErrorActionPreference = "Stop"

if ($CompanyMode) {
    $RepoRoot = "D:\HermesWorker\workspace\alpha_pit_true1min_engine_20260619"
    $PythonExe = "D:\HermesWorker\workspace\.venv\Scripts\python.exe"
    $ShardRoot = "D:\HermesWorker\workspace\phase3aj_new_data_current\runtime\phase3au_company_full_true1min_sharded_20260611"
}

Set-Location $RepoRoot
$env:PYTHONPATH = "src"
$env:NUMEXPR_MAX_THREADS = "8"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"

$bsOut = "runtime/phase3cf_guarded_smoke_bs_20260618"
$bsRep = "reports/phase3cf_guarded_smoke_bs_20260618"
$btOut = "runtime/phase3cf_guarded_smoke_bt_20260618"
$btRep = "reports/phase3cf_guarded_smoke_bt_20260618"
$caOut = "runtime/phase3cf_guarded_smoke_bz_candidate_audit_20260618"

if ($CompanyMode) {
    $maxShards = 8
    $sampleTimes = 96
    $seed = 192
    $each = 192
    $top = 128
}
else {
    $maxShards = 6
    $sampleTimes = 72
    $seed = 128
    $each = 128
    $top = 96
}

Write-Host "[Phase3CF Guarded Smoke] prelaunch validation"
& $PythonExe app.py phase3cf-large-search-prelaunch --allow-diagnostic
if ($LASTEXITCODE -ne 0) { throw "prelaunch failed: $LASTEXITCODE" }

Write-Host "[Phase3CF Guarded Smoke] BS guarded adaptive UCB/CEM"
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
    --seed-exploration 0.90 `
    --learning-rate 0.20 `
    --entropy-floor 0.08 `
    --min-feedback-eligible 32
if ($LASTEXITCODE -ne 0) { throw "BS guarded smoke failed: $LASTEXITCODE" }

Write-Host "[Phase3CF Guarded Smoke] BT guarded AST fresh"
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
    --seed-exploration 0.92 `
    --learning-rate 0.20 `
    --entropy-floor 0.08 `
    --min-feedback-eligible 32
if ($LASTEXITCODE -ne 0) { throw "BT guarded smoke failed: $LASTEXITCODE" }

Write-Host "[Phase3CF Guarded Smoke] CA bridge with hard filters"
& $PythonExe app.py phase3ca-build-bz-candidate-audit --allow-diagnostic -- `
    --source-root $bsRep `
    --source-root $btRep `
    --output-root $caOut `
    --top-n 64
if ($LASTEXITCODE -ne 0) { throw "CA guarded bridge failed: $LASTEXITCODE" }

Write-Host "[Phase3CF Guarded Smoke] complete. Inspect $caOut"
