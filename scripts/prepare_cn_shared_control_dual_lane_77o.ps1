param(
    [Parameter(Mandatory = $true)]
    [string]$RepoRoot,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$DeploymentManifest,
    [Parameter(Mandatory = $true)]
    [string]$SessionAuthorityManifest,
    [Parameter(Mandatory = $true)]
    [string]$QualificationRoot,
    [Parameter(Mandatory = $true)]
    [string]$SearchPreflightRoot,
    [string]$PartialSearchRoot = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_continuation_20260731_1025_' +
        '69e33b6_12288'
    ),
    [string]$BaseCandidateArchive = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_prep_20260730\' +
        'candidate_exact_archive_after_bounded_large.parquet'
    ),
    [string]$BaseBehaviorArchive = (
        'D:\ChengboRemote\runtime\' +
        'cn_hybrid_bounded_large_tranche_20260729_1030_6288d71_6144\' +
        'behavior_archive.parquet'
    ),
    [string]$WinnerGuide = (
        'D:\ChengboRemote\runtime\' +
        'cn_winner_guided_large_search_prep_20260730\' +
        'winner_structural_guide.json'
    ),
    [string]$ValidationSourceCampaign = (
        'D:\ChengboRemote\runtime\' +
        'cn_hybrid_bounded_large_tranche_20260729_1030_6288d71_6144'
    ),
    [string]$PriorReviewCohort = (
        'D:\ChengboRemote\runtime\' +
        'cn_productive_keep_review_64_20260729_2318_76503e0'
    ),
    [string]$MinuteReleaseRoot = (
        'D:\ChengboRemote\data\' +
        'cn_true1min_development_only_release_v1_20260712_77o'
    ),
    [string]$SplitManifest = (
        'D:\ChengboRemote\workspace\' +
        'cn_phase3cm_1024_sidecar_closure_0aba8c5\runtime\run_plans\' +
        'phase3ga_true1min_2024_2025_global_split_manifest.csv'
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

function Assert-ZeroSealedReads {
    param([object]$Payload, [string]$Label)
    foreach ($field in @(
        'financial_reads',
        'validation_reads',
        'holdout_reads',
        'forward_2026_reads'
    )) {
        $property = $Payload.PSObject.Properties[$field]
        if ($null -ne $property -and [int64]$property.Value -ne 0) {
            throw "$Label has nonzero $field"
        }
    }
}

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw 'this preparation is authorized only on DESKTOP-77OPJ6F'
}

$script:Python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$git = 'D:\ChengboRemote\tools\PortableGit\cmd\git.exe'
$repo = (Resolve-Path -LiteralPath $RepoRoot).Path
$deployment = (Resolve-Path -LiteralPath $DeploymentManifest).Path
$sessionManifest = (Resolve-Path -LiteralPath $SessionAuthorityManifest).Path
$partialSearch = (Resolve-Path -LiteralPath $PartialSearchRoot).Path
$baseCandidate = (Resolve-Path -LiteralPath $BaseCandidateArchive).Path
$baseBehavior = (Resolve-Path -LiteralPath $BaseBehaviorArchive).Path
$winnerGuide = (Resolve-Path -LiteralPath $WinnerGuide).Path
$validationCampaign = (Resolve-Path -LiteralPath $ValidationSourceCampaign).Path
$priorCohort = (Resolve-Path -LiteralPath $PriorReviewCohort).Path
$minuteRelease = (Resolve-Path -LiteralPath $MinuteReleaseRoot).Path
$split = (Resolve-Path -LiteralPath $SplitManifest).Path
$qualification = [IO.Path]::GetFullPath($QualificationRoot)
$searchPreflight = [IO.Path]::GetFullPath($SearchPreflightRoot)
$env:PYTHONPATH = Join-Path $repo 'src'

if (-not $repo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $repo"
}
if (-not $qualification.StartsWith(
    'D:\ChengboRemote\runtime\cn_shared_control_dual_lane_qualification_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected qualification root: $qualification"
}
if (-not $searchPreflight.StartsWith(
    'D:\ChengboRemote\runtime\cn_winner_guided_large_search_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected search preflight root: $searchPreflight"
}
if ((Test-Path -LiteralPath $qualification) -or
    (Test-Path -LiteralPath $searchPreflight)) {
    throw 'qualification and search preflight roots must both be fresh'
}
if (-not (Test-Path -LiteralPath $script:Python -PathType Leaf) -or
    -not (Test-Path -LiteralPath $git -PathType Leaf)) {
    throw 'official Python or portable Git is missing'
}

$head = (& $git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -ne $RepoSha) {
    throw "repo SHA mismatch: expected=$RepoSha observed=$head"
}
$deploymentPayload = Get-Content -LiteralPath $deployment -Raw |
    ConvertFrom-Json
$workspaceProperty = $deploymentPayload.PSObject.Properties['workspace']
$shaProperty = $deploymentPayload.PSObject.Properties['repo_sha']
if ($null -eq $shaProperty) {
    $shaProperty = $deploymentPayload.PSObject.Properties['git_sha']
}
if ($null -ne $workspaceProperty -and
    [IO.Path]::GetFullPath([string]$workspaceProperty.Value) -ne $repo) {
    throw 'deployment workspace binding drift'
}
if ($null -ne $shaProperty -and [string]$shaProperty.Value -ne $RepoSha) {
    throw 'deployment repo SHA binding drift'
}

$capacity = Join-Path $repo (
    'runtime\run_plans\cn_alpha_node_resource_profiles_v1.json'
)
$contract = Join-Path $repo (
    'runtime\run_plans\cn_shared_control_dual_lane_alpha_v1_contract.json'
)
$sourceAuthorization = Join-Path $repo (
    'runtime\run_plans\' +
    'cn_winner_guided_continuation_search_v1_authorization.json'
)
$registry = Join-Path $repo (
    'runtime\field_registry\cn_unified_capability_registry_v3_20260717\' +
    'unified_capability_registry.json'
)
foreach ($required in @(
    $capacity,
    $contract,
    $sourceAuthorization,
    $registry,
    $sessionManifest,
    $deployment
)) {
    if (-not (Test-Path -LiteralPath $required -PathType Leaf)) {
        throw "required authority missing: $required"
    }
}

$resourceBefore = Invoke-CheckedPython (
    Join-Path $repo 'scripts\manage_cn_node_resource_lease.py'
) status --state-root $NodeResourceStateRoot --capacity-manifest $capacity |
    Select-Object -Last 1 | ConvertFrom-Json
if ([int]$resourceBefore.active_lease_count -ne 0 -or
    [int]$resourceBefore.active_cpu_threads -ne 0) {
    throw 'shared resource state is not empty before qualification'
}

New-Item -ItemType Directory -Path $qualification | Out-Null
$historyRoot = Join-Path $qualification 'history'
Invoke-CheckedPython (
    Join-Path $repo 'scripts\build_cn_verified_partial_campaign_history.py'
) --campaign-root $partialSearch `
  --base-candidate-archive $baseCandidate `
  --base-behavior-archive $baseBehavior `
  --winner-structural-guide $winnerGuide `
  --output-root $historyRoot `
  --checkpoint-count 3 `
  --expected-route 'SLOW_TEMPORAL_CHANGE=1504' `
  --expected-route 'FIRSTN_PATH=32' `
  --expected-route 'SLOW_CROSS_SECTIONAL_LEVEL=0' `
  --expected-route 'MARKET_REGIME_CONDITION=0' `
  --expected-route 'DISCLOSURE_EVENT=0'

$candidateArchive = Join-Path $historyRoot 'candidate_exact_archive.parquet'
$behaviorArchive = Join-Path $historyRoot 'behavior_archive.parquet'
$historyManifest = Join-Path $historyRoot 'history_manifest.json'
$generatedAuthorityRoot = Join-Path $repo 'runtime\run_plans\generated'
New-Item -ItemType Directory -Force -Path $generatedAuthorityRoot | Out-Null
$searchAuthorization = Join-Path $generatedAuthorityRoot (
    'cn_shared_control_dual_search_authorization_' + $RepoSha.Substring(0, 7) +
    '.json'
)
if (Test-Path -LiteralPath $searchAuthorization) {
    throw "generated search authorization already exists: $searchAuthorization"
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
Assert-ZeroSealedReads $supply 'search supply preflight'

$capability = Join-Path $qualification 'stock_session_capability.json'
$splitHash = (Get-FileHash -LiteralPath $split -Algorithm SHA256).Hash.ToLowerInvariant()
Invoke-CheckedPython (
    Join-Path $repo 'scripts\build_cn_stock_session_field_capability.py'
) --source-root $minuteRelease `
  --candidate-table (Join-Path $validationCampaign 'candidate_ledger.parquet') `
  --registry $registry `
  --split-manifest $split `
  --split-manifest-hash $splitHash `
  --output $capability `
  --max-shards 16
$capabilityPayload = Get-Content -LiteralPath $capability -Raw |
    ConvertFrom-Json
if ([string]$capabilityPayload.status -ne 'ZERO_FINANCIAL_CAPABILITY_CLOSED') {
    throw 'execution-clock capability did not close'
}
Assert-ZeroSealedReads $capabilityPayload 'execution capability'

$cohort = Join-Path $qualification 'new_keep_review_32'
Invoke-CheckedPython (
    Join-Path $repo 'scripts\freeze_cn_productive_keep_review_cohort.py'
) --campaign-root $validationCampaign `
  --output-root $cohort `
  --cohort-pairs 32 `
  --exclude-cohort-root $priorCohort `
  --execution-capability-manifest $capability
$cohortVerification = Join-Path $qualification (
    'new_keep_review_32_verification.json'
)
Invoke-CheckedPython (
    Join-Path $repo 'scripts\verify_cn_productive_keep_review_cohort.py'
) --campaign-root $validationCampaign `
  --selection-root $cohort `
  --receipt $cohortVerification
$cohortManifestPath = Join-Path $cohort 'keep_review_manifest.json'
$cohortManifest = Get-Content -LiteralPath $cohortManifestPath -Raw |
    ConvertFrom-Json
$cohortSummaryPath = Join-Path $cohort 'keep_review_summary.json'
$cohortSummary = Get-Content -LiteralPath $cohortSummaryPath -Raw |
    ConvertFrom-Json
if ([int]$cohortSummary.selected_pairs -ne 32 -or
    [int]$cohortSummary.selected_candidate_members -ne 64) {
    throw 'new validation cohort is not exactly 32 pairs / 64 members'
}
Assert-ZeroSealedReads $cohortManifest 'new cohort manifest'
Assert-ZeroSealedReads $cohortSummary 'new cohort summary'

$authority = Join-Path $qualification 'finalist_input_authority_32'
Invoke-CheckedPython (
    Join-Path $repo 'scripts\freeze_cn_finalist_input_authority.py'
) --repo-root $repo `
  --cohort-root $cohort `
  --release-root $minuteRelease `
  --output-root $authority `
  --session-authority-manifest $sessionManifest `
  --repo-sha $RepoSha
$authorityVerification = Join-Path $qualification (
    'finalist_input_authority_32_verification'
)
Invoke-CheckedPython (
    Join-Path $repo 'scripts\verify_cn_finalist_input_authority.py'
) --output-root $authority --verification-root $authorityVerification
$authorityManifestPath = Join-Path $authority (
    'finalist_input_authority_manifest.json'
)
$authorityManifest = Get-Content -LiteralPath $authorityManifestPath -Raw |
    ConvertFrom-Json
if ([string]$authorityManifest.status -ne 'FINALIST_INPUT_AUTHORITY_READY') {
    throw 'new finalist input authority is not READY'
}
Assert-ZeroSealedReads $authorityManifest 'finalist input authority'

$resourceAfter = Invoke-CheckedPython (
    Join-Path $repo 'scripts\manage_cn_node_resource_lease.py'
) status --state-root $NodeResourceStateRoot --capacity-manifest $capacity |
    Select-Object -Last 1 | ConvertFrom-Json
if ([int]$resourceAfter.active_lease_count -ne 0 -or
    [int]$resourceAfter.active_cpu_threads -ne 0) {
    throw 'shared resource state is not empty after qualification'
}

$closure = [ordered]@{
    schema_version = 'cn_shared_control_dual_lane_qualification_closure_v1'
    status = 'ZERO_FINANCIAL_DUAL_LANE_QUALIFICATION_PASS'
    repo_sha = $RepoSha
    deployment_manifest = $deployment
    deployment_manifest_sha256 = (
        Get-FileHash -LiteralPath $deployment -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    dual_lane_contract = $contract
    dual_lane_contract_sha256 = (
        Get-FileHash -LiteralPath $contract -Algorithm SHA256
    ).Hash.ToLowerInvariant()
    history_receipt = Join-Path $historyRoot 'partial_history_receipt.json'
    search_authorization = $searchAuthorization
    search_preflight_root = $searchPreflight
    search_supply_preflight = $supplyPath
    execution_capability_manifest = $capability
    new_cohort_root = $cohort
    new_cohort_manifest = $cohortManifestPath
    new_cohort_verification = $cohortVerification
    finalist_input_authority_root = $authority
    finalist_input_authority_manifest = $authorityManifestPath
    finalist_input_authority_verification = $authorityVerification
    search_profile = 'SEARCH_DUAL_24'
    validation_profile = 'VALIDATION_DUAL_8'
    total_cpu_threads = 32
    search_formal_asks = 9216
    validation_pairs = 32
    reward_rows_imported = 0
    optimizer_state_imported = $false
    scheduler_state_imported = $false
    financial_reads = 0
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    resource_active_leases_after = [int]$resourceAfter.active_lease_count
    resource_active_cpu_threads_after = [int]$resourceAfter.active_cpu_threads
    execution_authorized = $true
}
$closurePath = Join-Path $qualification 'qualification_closure.json'
$closure | ConvertTo-Json -Depth 6 |
    Set-Content -LiteralPath $closurePath -Encoding UTF8
$closureHash = (
    Get-FileHash -LiteralPath $closurePath -Algorithm SHA256
).Hash.ToLowerInvariant()
Write-Output "QUALIFICATION_CLOSURE=$closurePath"
Write-Output "QUALIFICATION_CLOSURE_SHA256=$closureHash"
