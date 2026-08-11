param(
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$QualificationRoot,
    [Parameter(Mandatory = $true)][string]$SearchPreflightRoot,
    [Parameter(Mandatory = $true)][string]$ClassificationRoot,
    [Parameter(Mandatory = $true)][string]$ProjectControlTargetRunId,
    [Parameter(Mandatory = $true)][string]$ProjectControlExpiresAt,
    [Parameter(Mandatory = $true)][string]$ParentProjectControlRunId,
    [Parameter(Mandatory = $true)][string]$ParentTargetCampaignId,
    [Parameter(Mandatory = $true)][string]$ParentTargetRunId,
    [string]$ClosedSearchRoot = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_continuous_dual_' +
        '20260803_0120_1f8bdb3_768'
    ),
    [string]$BaseHistoryRoot = (
        'D:\ChengboRemote\runtime\' +
        'cn_continuous_search_finalist_dual_history_after_1152_' +
        '20260803_023363c'
    ),
    [string]$StrictReplayRoot = (
        'D:\ChengboRemote\runtime\' +
        'cn_finalist_strict_train_replay_continuous_dual_32_' +
        '20260802_2340_023363c'
    ),
    [string]$WinnerGuide = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_prep_20260730\' +
        'winner_structural_guide.json'
    ),
    [string]$NodeResourceStateRoot = (
        'D:\ChengboRemote\runtime\node_resource_governor'
    )
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Invoke-CheckedPython {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    & $script:Python @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Python command failed with exit code $LASTEXITCODE"
    }
}

function Assert-ZeroDataReads {
    param([object]$Payload, [string]$Label)
    foreach ($field in @(
        'financial_reads',
        'validation_reads',
        'holdout_reads',
        'forward_2026_reads',
        'new_financial_data_reads',
        'new_validation_data_reads'
    )) {
        $property = $Payload.PSObject.Properties[$field]
        if ($null -ne $property -and [int64]$property.Value -ne 0) {
            throw "$Label has nonzero $field"
        }
    }
}

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw 'preparation is authorized only on DESKTOP-77OPJ6F'
}
$script:Python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$repo = (Resolve-Path -LiteralPath $RepoRoot).Path
$deployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$closedSearch = (Resolve-Path -LiteralPath $ClosedSearchRoot).Path
$baseHistory = (Resolve-Path -LiteralPath $BaseHistoryRoot).Path
$strictReplay = (Resolve-Path -LiteralPath $StrictReplayRoot).Path
$winnerGuide = (Resolve-Path -LiteralPath $WinnerGuide).Path
$qualification = [IO.Path]::GetFullPath($QualificationRoot)
$searchPreflight = [IO.Path]::GetFullPath($SearchPreflightRoot)
$classification = [IO.Path]::GetFullPath($ClassificationRoot)
foreach ($root in @($qualification, $searchPreflight, $classification)) {
    if (Test-Path -LiteralPath $root) {
        throw "preparation output root must be fresh: $root"
    }
}
if (-not $repo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $repo"
}
if (-not $searchPreflight.StartsWith(
    'D:\ChengboRemote\runtime\cn_winner_guided_large_search_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected search preflight root: $searchPreflight"
}
$gitSafeDirectory = "safe.directory=$($repo.Replace('\', '/'))"
$head = (& $git -c $gitSafeDirectory -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -ne $RepoSha) {
    throw "repo SHA mismatch: expected=$RepoSha observed=$head"
}
$dirty = @(& $git -c $gitSafeDirectory -C $repo status --porcelain)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw 'deployed workspace must be clean'
}
$deploymentPayload = Get-Content -LiteralPath $deployment -Raw |
    ConvertFrom-Json
if (
    [string]$deploymentPayload.repo_sha -ne $RepoSha -or
    [string]$deploymentPayload.workspace -ne $repo
) {
    throw 'deployment manifest binding drift'
}

$env:PYTHONPATH = Join-Path $repo 'src'
$env:PYTHONUTF8 = '1'
$capacity = Join-Path $repo (
    'runtime\run_plans\cn_alpha_node_resource_profiles_v1.json'
)
$contract = Join-Path $repo (
    'runtime\run_plans\cn_terminal_liquidity_search_continuity_v1_contract.json'
)
$sourceAuthorization = Join-Path $repo (
    'runtime\run_plans\cn_winner_guided_continuation_search_v1_authorization.json'
)
foreach ($path in @(
    $script:Python,
    $git,
    $capacity,
    $contract,
    $sourceAuthorization,
    (Join-Path $closedSearch 'checkpoints\checkpoint_001\batch_manifest.json'),
    (Join-Path $strictReplay 'replay\REPLAY_COMPLETE.json'),
    (Join-Path $strictReplay 'oos\OOS_COMPLETE.json')
)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "required authority missing: $path"
    }
}

$resourceBefore = Invoke-CheckedPython (
    Join-Path $repo 'scripts\manage_cn_node_resource_lease.py'
) status --state-root $NodeResourceStateRoot --capacity-manifest $capacity |
    Select-Object -Last 1 | ConvertFrom-Json
if (
    [int]$resourceBefore.active_lease_count -ne 0 -or
    [int]$resourceBefore.active_cpu_threads -ne 0
) {
    throw 'shared resource state is not empty before preparation'
}

New-Item -ItemType Directory -Path $qualification | Out-Null
$historyRoot = Join-Path $qualification 'history_after_768'
Invoke-CheckedPython (
    Join-Path $repo 'scripts\build_cn_verified_partial_campaign_history.py'
) --campaign-root $closedSearch `
  --base-candidate-archive (Join-Path $baseHistory 'candidate_exact_archive.parquet') `
  --base-behavior-archive (Join-Path $baseHistory 'behavior_archive.parquet') `
  --winner-structural-guide $winnerGuide `
  --output-root $historyRoot `
  --checkpoint-count 1 `
  --expected-route 'SLOW_TEMPORAL_CHANGE=736' `
  --expected-route 'FIRSTN_PATH=32' `
  --expected-route 'SLOW_CROSS_SECTIONAL_LEVEL=0' `
  --expected-route 'MARKET_REGIME_CONDITION=0' `
  --expected-route 'DISCLOSURE_EVENT=0'

Invoke-CheckedPython (
    Join-Path $repo 'scripts\build_cn_terminal_liquidity_oos_classification.py'
) --candidate-replay-results (Join-Path $strictReplay (
    'replay\candidate_replay_results.parquet'
)) --pair-replay-results (Join-Path $strictReplay (
    'replay\pair_replay_results.parquet'
)) --oos-pair-results (Join-Path $strictReplay (
    'oos\replay_then_oos_pair_results.parquet'
)) --finalist-pairs (Join-Path $strictReplay (
    'prepared\finalist_pairs.parquet'
)) --output-root $classification
$classificationClosure = Get-Content -LiteralPath (
    Join-Path $classification 'CLASSIFICATION_COMPLETE.json'
) -Raw | ConvertFrom-Json
if ([string]$classificationClosure.status -ne 'CLASSIFICATION_CLOSED_IMMUTABLE') {
    throw 'terminal-liquidity classification did not close'
}
Assert-ZeroDataReads $classificationClosure 'terminal classification'

$candidateArchive = Join-Path $historyRoot 'candidate_exact_archive.parquet'
$behaviorArchive = Join-Path $historyRoot 'behavior_archive.parquet'
$historyManifest = Join-Path $historyRoot 'history_manifest.json'
$generatedRoot = Join-Path $repo 'runtime\run_plans\generated'
New-Item -ItemType Directory -Force -Path $generatedRoot | Out-Null
$searchAuthorization = Join-Path $generatedRoot (
    'cn_terminal_liquidity_search_continuity_authorization_' +
    $RepoSha.Substring(0, 7) + '.json'
)
if (Test-Path -LiteralPath $searchAuthorization) {
    throw "generated authorization already exists: $searchAuthorization"
}
Invoke-CheckedPython (
    Join-Path $repo 'scripts\freeze_cn_shared_control_dual_search_authorization.py'
) --source-authorization $sourceAuthorization `
  --dual-lane-contract $contract `
  --capacity-manifest $capacity `
  --candidate-archive $candidateArchive `
  --behavior-archive $behaviorArchive `
  --history-manifest $historyManifest `
  --output $searchAuthorization

$futureAuthorization = Join-Path $searchPreflight (
    'qualification_authorization.json'
)
$projectControlRequest = Join-Path $generatedRoot (
    'cn_terminal_liquidity_project_control_request_' +
    $RepoSha.Substring(0, 7) + '.json'
)
Invoke-CheckedPython (
    Join-Path $repo 'scripts\build_cn_project_control_execution_request.py'
) --output $projectControlRequest `
  --action 'SUCCESSOR_CAMPAIGN' `
  --target-campaign-id 'cn-large-tpe-search-campaign' `
  --target-run-id $ProjectControlTargetRunId `
  --target-output-root $searchPreflight `
  --repo-sha $RepoSha `
  --expires-at $ProjectControlExpiresAt `
  --campaign-authorization-path $futureAuthorization `
  --preflight-authorization-source $searchAuthorization `
  --parent-project-control-run-id $ParentProjectControlRunId `
  --parent-target-campaign-id $ParentTargetCampaignId `
  --parent-target-run-id $ParentTargetRunId
$boundaryReceipt = [ordered]@{
    schema_version = 'cn_project_control_external_boundary_v1'
    status = 'AWAITING_EXTERNAL_PROJECT_CONTROL_ADMISSION'
    requested_action = 'SUCCESSOR_CAMPAIGN'
    target_run_id = $ProjectControlTargetRunId
    target_output_root = $searchPreflight
    source_campaign_authorization = $searchAuthorization
    future_campaign_authorization = $futureAuthorization
    execution_request = $projectControlRequest
    execution_request_sha256 = (
        Get-FileHash -LiteralPath $projectControlRequest -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    canonical_launcher = 'scripts\run_cn_winner_guided_large_search_77o.ps1'
    next_step = 'OBTAIN_EXTERNAL_ADMISSION_THEN_CALL_CANONICAL_LAUNCHER'
    financial_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
}
$boundaryReceiptPath = Join-Path $qualification (
    'project_control_external_boundary.json'
)
$boundaryReceipt | ConvertTo-Json -Depth 6 |
    Set-Content -LiteralPath $boundaryReceiptPath -Encoding UTF8
Write-Output "PROJECT_CONTROL_REQUEST=$projectControlRequest"
Write-Output "PROJECT_CONTROL_BOUNDARY=$boundaryReceiptPath"
return
