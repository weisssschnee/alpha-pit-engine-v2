param(
  [string]$RepoRoot = "D:\ChengboRemote\workspace\cn_searchcore_v2_stage1_20260824"
)

$ErrorActionPreference = "Stop"
$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
$Git = "D:\ChengboRemote\tools\PortableGit\cmd\git.exe"
$RepoRoot = (Resolve-Path -LiteralPath $RepoRoot).Path
$env:PYTHONPATH = Join-Path $RepoRoot "src"

$StageDAuth = Join-Path $RepoRoot "runtime\run_plans\cn_program_stage_d_primitive_confirmation_authorization_v1.json"
$Prefreeze = Join-Path $RepoRoot "runtime\run_plans\cn_search_core_v2_stage1_prefreeze_20260824.json"
$SupplyAudit = Join-Path $RepoRoot "runtime\run_plans\cn_search_core_v2_state_jump_real_supply_audit_20260824.json"
$ResourceCanary = Join-Path $RepoRoot "runtime\run_plans\cn_search_core_v2_stage1_official_resource_canary_20260824.json"
$Authorization = Join-Path $RepoRoot "runtime\run_plans\cn_search_core_v2_stage1_authorization_v1.json"

$SourceFreeze = "D:\ChengboRemote\runtime\cn_program_optimizer_tournament_batch_feasibility_retry_20260815_11c5d08\prefinancial_freeze_stage01"
$ExecutionContract = "D:\ChengboRemote\runtime\cn_finalist_strict_train_replay_continuous_dual_32_20260802_2340_023363c\prepared\replay_then_oos_execution_contract.json"
$TrainFieldRoot = "D:\ChengboRemote\runtime\cn_search_engine_v2_canary_replacement_20260812_1b91c88\prefinancial_freeze\materialized_session_sidecar"
$TrainPriceRoot = "D:\ChengboRemote\runtime\cn_finalist_strict_train_replay_continuous_dual_32_20260802_2340_023363c\sidecars\train_session_fields"
$Registry = "D:\ChengboRemote\workspace\alpha_pit_search_v2_1b91c88_20260812\runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json"
$Capacity = "D:\ChengboRemote\workspace\alpha_pit_search_v2_1b91c88_20260812\runtime\run_plans\cn_alpha_node_resource_profiles_v1.json"

if (-not (Test-Path -LiteralPath $Python)) { throw "alpha311 Python missing: $Python" }
if (-not (Test-Path -LiteralPath $Git)) { throw "PortableGit missing: $Git" }
foreach ($path in @($StageDAuth,$Prefreeze,$SourceFreeze,$ExecutionContract,$TrainFieldRoot,$TrainPriceRoot,$Registry,$Capacity)) {
  if (-not (Test-Path -LiteralPath $path)) { throw "required frozen authority missing: $path" }
}

$StageD = Get-Content -Raw -LiteralPath $StageDAuth | ConvertFrom-Json
$PriorRelative = [string]$StageD.source_prior_exact.relative_path
if ([string]::IsNullOrWhiteSpace($PriorRelative)) { throw "Stage-D source prior binding missing" }
$PriorExact = Join-Path $RepoRoot $PriorRelative
if (-not (Test-Path -LiteralPath $PriorExact)) { throw "prior exact freeze missing: $PriorExact" }

$SafeRepo = $RepoRoot -replace '\\','/'
$RepoSha = (& $Git -c "safe.directory=$SafeRepo" -C $RepoRoot rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $RepoSha -notmatch '^[0-9a-f]{40}$') { throw "unable to bind repo SHA" }

Write-Host "[1/4] real component-pool state-jump supply audit (zero financial)"
& $Python (Join-Path $RepoRoot "scripts\audit_cn_search_core_v2_state_jump_supply.py") `
  --repo-root $RepoRoot `
  --source-freeze-root $SourceFreeze `
  --registry $Registry `
  --stage1-prefreeze $Prefreeze `
  --probes-per-template 96 `
  --output $SupplyAudit
if ($LASTEXITCODE -ne 0) { throw "state-jump real-supply audit failed" }

Write-Host "[2/4] SEARCH_DUAL_24 official resource canary (zero candidate evaluation)"
& $Python (Join-Path $RepoRoot "scripts\run_cn_search_core_v2_stage1_resource_canary.py") `
  --source-stage-d-authorization $StageDAuth `
  --stage1-prefreeze $Prefreeze `
  --state-jump-supply-audit $SupplyAudit `
  --source-freeze-root $SourceFreeze `
  --prior-exact-freeze $PriorExact `
  --execution-contract $ExecutionContract `
  --train-field-root $TrainFieldRoot `
  --train-price-root $TrainPriceRoot `
  --registry $Registry `
  --node-resource-capacity $Capacity `
  --repo-sha $RepoSha `
  --output $ResourceCanary
if ($LASTEXITCODE -ne 0) { throw "official resource canary failed" }

Write-Host "[3/4] build final campaign authorization"
& $Python (Join-Path $RepoRoot "scripts\build_cn_search_core_v2_stage1_authorization_v1.py") `
  --repo-root $RepoRoot `
  --official-resource-canary $ResourceCanary `
  --state-jump-supply-audit $SupplyAudit `
  --output $Authorization
if ($LASTEXITCODE -ne 0) { throw "authorization build failed" }

Write-Host "[4/4] verify authorization binding"
& $Python -c "from pathlib import Path; from our_system_phase2.runtime.cn_search_core_v2_stage1_v1 import verify_authorization; p=verify_authorization(Path(r'$Authorization'), repo_root=Path(r'$RepoRoot')); print(p['status']); print(p['authorization_payload_sha256'])"
if ($LASTEXITCODE -ne 0) { throw "authorization verification failed" }

$Supply = Get-Content -Raw -LiteralPath $SupplyAudit | ConvertFrom-Json
$Canary = Get-Content -Raw -LiteralPath $ResourceCanary | ConvertFrom-Json
$Auth = Get-Content -Raw -LiteralPath $Authorization | ConvertFrom-Json
[ordered]@{
  status = "SEARCH_CORE_V2_STAGE1_PREFINANCIAL_READY"
  repo_sha = $RepoSha
  supply_audit = [string]$Supply.audit_payload_sha256
  generated_total = [int]$Supply.generator.generated_total
  resource_canary = [string]$Canary.official_canary_payload_sha256
  field_column_count = [int]$Canary.field_column_count
  authorization = [string]$Auth.authorization_payload_sha256
  candidate_evaluation_executed = $false
} | ConvertTo-Json -Depth 4
