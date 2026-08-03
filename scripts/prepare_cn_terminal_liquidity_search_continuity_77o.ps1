param(
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)][string]$DeploymentManifest,
    [Parameter(Mandatory = $true)][string]$QualificationRoot,
    [Parameter(Mandatory = $true)][string]$SearchPreflightRoot,
    [Parameter(Mandatory = $true)][string]$ClassificationRoot,
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

& (Join-Path $repo 'scripts\run_cn_winner_guided_large_search_77o.ps1') `
    -Repo $repo `
    -RepoSha $RepoSha `
    -OutputRoot $searchPreflight `
    -CampaignAuthorization $searchAuthorization `
    -CandidateArchive $candidateArchive `
    -BehaviorArchive $behaviorArchive `
    -WinnerGuide $winnerGuide `
    -HistoryManifest $historyManifest `
    -NodeResourceProfile 'SEARCH_DUAL_24' `
    -NodeResourceCapacity $capacity `
    -NodeResourceStateRoot $NodeResourceStateRoot `
    -PreflightOnly
if ($LASTEXITCODE -ne 0) {
    throw "search zero-financial preflight failed: $LASTEXITCODE"
}
$supplyPath = Join-Path $searchPreflight 'fresh_exact_supply_preflight.json'
$supply = Get-Content -LiteralPath $supplyPath -Raw | ConvertFrom-Json
if ([string]$supply.status -ne 'PASS') {
    throw 'search fresh exact supply is not PASS'
}
Assert-ZeroDataReads $supply 'search supply preflight'
$expectedFresh = @{
    SLOW_TEMPORAL_CHANGE = 177
    SLOW_CROSS_SECTIONAL_LEVEL = 54
    FIRSTN_PATH = 365
}
foreach ($routeId in $expectedFresh.Keys) {
    $property = $supply.route_rows.PSObject.Properties[$routeId]
    if (
        $null -eq $property -or
        [int]$property.Value.fresh_exact -ne [int]$expectedFresh[$routeId]
    ) {
        throw "refreshed exact supply drift: $routeId"
    }
}

$historyReceipt = Get-Content -LiteralPath (
    Join-Path $historyRoot 'partial_history_receipt.json'
) -Raw | ConvertFrom-Json
Assert-ZeroDataReads $historyReceipt 'history refresh'
$receipt = [ordered]@{
    schema_version = 'cn_terminal_liquidity_search_continuity_preflight_v1'
    status = 'PASS_ZERO_FINANCIAL_READY_FOR_DUAL_LAUNCH'
    repo_sha = $RepoSha
    contract = $contract
    contract_sha256 = (
        Get-FileHash -LiteralPath $contract -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    history_manifest = $historyManifest
    history_manifest_sha256 = (
        Get-FileHash -LiteralPath $historyManifest -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    search_authorization = $searchAuthorization
    search_authorization_sha256 = (
        Get-FileHash -LiteralPath $searchAuthorization -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    search_supply_preflight = $supplyPath
    search_supply_preflight_sha256 = (
        Get-FileHash -LiteralPath $supplyPath -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    classification_closure = Join-Path $classification 'CLASSIFICATION_COMPLETE.json'
    classification_closure_sha256 = (
        Get-FileHash -LiteralPath (
            Join-Path $classification 'CLASSIFICATION_COMPLETE.json'
        ) -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    search_formal_asks = 184
    search_route_mix = [ordered]@{
        SLOW_TEMPORAL_CHANGE = 144
        SLOW_CROSS_SECTIONAL_LEVEL = 40
        FIRSTN_PATH = 0
        MARKET_REGIME_CONDITION = 0
        DISCLOSURE_EVENT = 0
    }
    search_threads = 24
    diagnostic_threads = 8
    total_threads = 32
    financial_reads = 0
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    reward_rows_imported = 0
    optimizer_state_imported = $false
    scheduler_state_imported = $false
    classification_feedback_to_search = $false
}
$receiptJson = $receipt | ConvertTo-Json -Depth 8
$receiptHash = [Security.Cryptography.SHA256]::HashData(
    [Text.Encoding]::UTF8.GetBytes($receiptJson)
)
$receipt['receipt_payload_sha256'] = [Convert]::ToHexString(
    $receiptHash
).ToLowerInvariant()
$receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (
    Join-Path $qualification 'qualification_receipt.json'
) -Encoding UTF8
$receipt | ConvertTo-Json -Depth 8
