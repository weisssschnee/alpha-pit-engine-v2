param(
    [Parameter(Mandatory = $true)][string]$FrozenBinding,
    [Parameter(Mandatory = $true)][string]$ExpectedFrozenBindingSha256,
    [Parameter(Mandatory = $true)][string]$CombinedContract,
    [Parameter(Mandatory = $true)][string]$ExpectedCombinedContractSha256,
    [Parameter(Mandatory = $true)][string]$SidecarClosure,
    [Parameter(Mandatory = $true)][string]$ExpectedSidecarClosureSha256,
    [Parameter(Mandatory = $true)][string]$ResourceContract,
    [Parameter(Mandatory = $true)][string]$ExpectedResourceContractSha256,
    [Parameter(Mandatory = $true)][string]$HistoricalSubsetReceipt,
    [Parameter(Mandatory = $true)][string]$ExpectedHistoricalSubsetReceiptSha256,
    [Parameter(Mandatory = $true)][string]$ReadjudicationManifest,
    [Parameter(Mandatory = $true)][string]$ExpectedReadjudicationManifestSha256,
    [Parameter(Mandatory = $true)][string]$ActiveCapacityReceipt,
    [Parameter(Mandatory = $true)][string]$SessionCapacityReceipt,
    [Parameter(Mandatory = $true)][string]$SourceClosureManifest,
    [Parameter(Mandatory = $true)][string]$ExpectedSourceClosureManifestSha256,
    [Parameter(Mandatory = $true)][string]$ExpectedRepoSha,
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][string]$RunParentRoot,
    [string]$OutputName = "cn_phase3cm_1024_wave",
    [switch]$Resume,
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ExpectedActivePairs = 584
$ExpectedSessionPairs = 440
$ExpectedTotalPairs = 1024
$ExpectedCandidateMembers = 2048
$FrozenPortfolioSourceSha256 = "5b665d33d74eb4b5f451f06b4ad7e4b352f779c3e2e4836bb2d76950c413d110"

function Get-CnSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
}

function Resolve-CnPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Base,
        [switch]$Directory
    )
    $Candidate = if ([IO.Path]::IsPathRooted($Path)) { $Path } else { Join-Path $Base $Path }
    if ($Directory) {
        if (-not (Test-Path -LiteralPath $Candidate -PathType Container)) {
            throw "required directory is missing: $Candidate"
        }
    }
    elseif (-not (Test-Path -LiteralPath $Candidate -PathType Leaf)) {
        throw "required file is missing: $Candidate"
    }
    return (Resolve-Path -LiteralPath $Candidate).Path
}

function Read-CnBoundJson {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$ExpectedSha256,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Base
    )
    if ($ExpectedSha256 -notmatch '^[0-9a-f]{64}$') {
        throw "$Label expected SHA-256 is not exact lowercase hex"
    }
    $Resolved = Resolve-CnPath -Path $Path -Base $Base
    $Observed = Get-CnSha256 -Path $Resolved
    if ($Observed -ne $ExpectedSha256) { throw "$Label SHA-256 drift" }
    return [pscustomobject]@{
        path = $Resolved
        sha256 = $Observed
        payload = Get-Content -LiteralPath $Resolved -Raw | ConvertFrom-Json
    }
}

function Test-CnZeroAccessEvidence {
    param([object]$Payload)
    if ($null -eq $Payload) { return $false }
    foreach ($Name in @("validation_reads", "holdout_reads", "forward_2026_reads")) {
        $Property = $Payload.PSObject.Properties[$Name]
        if ($null -eq $Property -or $null -eq $Property.Value -or [int64]$Property.Value -ne 0) {
            return $false
        }
    }
    return $true
}

function Test-CnSequenceEqual {
    param([object[]]$Left, [object[]]$Right)
    if ($Left.Count -ne $Right.Count) { return $false }
    for ($Index = 0; $Index -lt $Left.Count; $Index += 1) {
        if ([string]$Left[$Index] -ne [string]$Right[$Index]) { return $false }
    }
    return $true
}

function Confirm-CnFileBinding {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)][string]$Sha256,
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string]$Base
    )
    if ($Sha256 -notmatch '^[0-9a-f]{64}$') { throw "$Label SHA-256 is not exact" }
    $Resolved = Resolve-CnPath -Path $Path -Base $Base
    if ((Get-CnSha256 -Path $Resolved) -ne $Sha256) { throw "$Label SHA-256 drift" }
    return $Resolved
}

function Get-CnPairIds {
    param([object]$Plan)
    return @($Plan.pair_batches | ForEach-Object { @($_) } | ForEach-Object { [string]$_ })
}

function Confirm-CnCapacityReceipt {
    param(
        [Parameter(Mandatory = $true)][string]$Receipt,
        [Parameter(Mandatory = $true)][string]$Backend,
        [Parameter(Mandatory = $true)][string]$CandidateTable,
        [Parameter(Mandatory = $true)][string]$ExecutionPlan,
        [Parameter(Mandatory = $true)][string]$FieldRoot,
        [Parameter(Mandatory = $true)][string]$LabelRoot,
        [Parameter(Mandatory = $true)][string]$Repo,
        [Parameter(Mandatory = $true)][string]$Closure,
        [Parameter(Mandatory = $true)][string]$RepoSha,
        [Parameter(Mandatory = $true)][string]$Python
    )
    $Validator = Resolve-CnPath -Path "scripts\preflight_cn_phase3cm_dag_cache.py" -Base $Repo
    $Output = @(& $Python $Validator `
        --validate-receipt $Receipt `
        --candidate-table $CandidateTable `
        --execution-plan $ExecutionPlan `
        --backend $Backend `
        --field-sidecar-root $FieldRoot `
        --label-sidecar-root $LabelRoot `
        --repo-root $Repo `
        --source-closure-manifest $Closure `
        --expected-repo-sha $RepoSha)
    if ($LASTEXITCODE -ne 0 -or $Output.Count -eq 0) {
        throw "1024 capacity receipt launch validation failed for $Backend"
    }
    $Validation = $Output[-1] | ConvertFrom-Json
    if ($Validation.status -ne "CN_PHASE3CM_DAG_CACHE_RECEIPT_VALIDATED_FOR_LAUNCH" -or
        [int64]$Validation.max_block_rows -le 0 -or
        [string]$Validation.receipt_hash -notmatch '^[0-9a-f]{64}$') {
        throw "1024 capacity receipt launch validation drift for $Backend"
    }
    return $Validation
}

$RepoRoot = Resolve-CnPath -Path $RepoRoot -Base (Get-Location).Path -Directory
$RunParentRoot = Resolve-CnPath -Path $RunParentRoot -Base $RepoRoot -Directory
$PythonExe = Resolve-CnPath -Path $PythonExe -Base $RepoRoot
if ($ExpectedRepoSha -notmatch '^[0-9a-f]{40}$') {
    throw "ExpectedRepoSha must be an exact lowercase 40-character Git SHA"
}

$BindingEvidence = Read-CnBoundJson `
    -Path $FrozenBinding -ExpectedSha256 $ExpectedFrozenBindingSha256 `
    -Label "1024 frozen binding" -Base $RepoRoot
$FrozenBinding = $BindingEvidence.path
$Binding = $BindingEvidence.payload
if ($Binding.status -ne "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND" -or
    [string]$Binding.source_closure_sha -ne $ExpectedRepoSha -or
    [string]$Binding.data_role -ne "development_train_only" -or
    [int]$Binding.pair_count -ne $ExpectedTotalPairs -or
    [int]$Binding.candidate_member_count -ne $ExpectedCandidateMembers -or
    [int]$Binding.clock_counts.active_bar -ne $ExpectedActivePairs -or
    [int]$Binding.clock_counts.stock_session -ne $ExpectedSessionPairs -or
    [int]$Binding.sealed_reads.validation -ne 0 -or
    [int]$Binding.sealed_reads.holdout -ne 0 -or
    [int]$Binding.sealed_reads.forward_2026 -ne 0 -or
    $Binding.promotion -ne "FORBIDDEN" -or
    $Binding.cross_sprint_memory -ne "FORBIDDEN" -or
    $Binding.strict_stage_a -ne "NOT_AUTHORIZED") {
    throw "1024 frozen binding contract drift"
}

$CombinedEvidence = Read-CnBoundJson `
    -Path $CombinedContract -ExpectedSha256 $ExpectedCombinedContractSha256 `
    -Label "1024 combined execution contract" -Base $RepoRoot
$CombinedContract = $CombinedEvidence.path
$Contract = $CombinedEvidence.payload
if ($Contract.schema_version -ne "cn_phase3cm_phase_e_combined_execution_contract_v1" -or
    $Contract.status -ne "CN_PHASE3CM_PHASE_E_EXECUTION_PLANS_FROZEN" -or
    [string]$Contract.repo_sha -ne $ExpectedRepoSha -or
    [string]$Contract.input_binding_hash -ne [string]$Binding.binding_hash -or
    [string]$Contract.input_binding_sha256 -ne $BindingEvidence.sha256 -or
    [int]$Contract.heavy_processes -ne 2 -or
    [int]$Contract.plans.active_bar.pair_count -ne $ExpectedActivePairs -or
    [int]$Contract.plans.stock_session.pair_count -ne $ExpectedSessionPairs -or
    [int]$Contract.global_active_native_compute_threads -le 0 -or
    [int]$Contract.global_active_native_compute_threads -gt 24 -or
    [string]$Contract.data_role -ne "development_train_only" -or
    -not (Test-CnZeroAccessEvidence -Payload $Contract) -or
    $Contract.promotion -ne "FORBIDDEN" -or
    $Contract.strict_stage_a -ne "NOT_AUTHORIZED" -or
    $Contract.adaptation -ne "FORBIDDEN" -or
    $Contract.resource_gate_action -ne "FAIL_CLOSED_NO_PLAN_CHANGE") {
    throw "1024 combined execution contract drift"
}

$ActiveExecutionPlanPath = Confirm-CnFileBinding `
    -Path ([string]$Contract.plans.active_bar.path) `
    -Sha256 ([string]$Contract.plans.active_bar.sha256) `
    -Label "active 1024 execution plan" -Base $RepoRoot
$SessionExecutionPlanPath = Confirm-CnFileBinding `
    -Path ([string]$Contract.plans.stock_session.path) `
    -Sha256 ([string]$Contract.plans.stock_session.sha256) `
    -Label "session 1024 execution plan" -Base $RepoRoot
$ActivePlan = Get-Content -LiteralPath $ActiveExecutionPlanPath -Raw | ConvertFrom-Json
$SessionPlan = Get-Content -LiteralPath $SessionExecutionPlanPath -Raw | ConvertFrom-Json
$ActivePairIds = Get-CnPairIds -Plan $ActivePlan
$SessionPairIds = Get-CnPairIds -Plan $SessionPlan
$BoundActivePairIds = @(
    $Binding.pairs |
        Where-Object { $_.clock_namespace -eq "active_bar" } |
        ForEach-Object { [string]$_.pair_id } |
        Sort-Object
)
$BoundSessionPairIds = @(
    $Binding.pairs |
        Where-Object { $_.clock_namespace -eq "stock_session" } |
        ForEach-Object { [string]$_.pair_id } |
        Sort-Object
)
if ($ActivePlan.schema_version -ne "cn_phase3cm_frozen_execution_plan_v1" -or
    $SessionPlan.schema_version -ne "cn_phase3cm_frozen_execution_plan_v1" -or
    $ActivePlan.phase -ne "E" -or $SessionPlan.phase -ne "E" -or
    [string]$ActivePlan.execution_plan_hash -ne [string]$Contract.plans.active_bar.execution_plan_hash -or
    [string]$SessionPlan.execution_plan_hash -ne [string]$Contract.plans.stock_session.execution_plan_hash -or
    $ActivePairIds.Count -ne $ExpectedActivePairs -or
    $SessionPairIds.Count -ne $ExpectedSessionPairs -or
    @($ActivePairIds | Select-Object -Unique).Count -ne $ExpectedActivePairs -or
    @($SessionPairIds | Select-Object -Unique).Count -ne $ExpectedSessionPairs -or
    @(Compare-Object @($ActivePairIds | Sort-Object) $BoundActivePairIds).Count -ne 0 -or
    @(Compare-Object @($SessionPairIds | Sort-Object) $BoundSessionPairIds).Count -ne 0 -or
    [int]$ActivePlan.heavy_processes -ne 2 -or
    [int]$SessionPlan.heavy_processes -ne 2 -or
    [string]$ActivePlan.primary_thread_pool -ne "numba" -or
    [string]$SessionPlan.primary_thread_pool -ne "numba" -or
    [int]$ActivePlan.checkpoint_every_blocks -le 0 -or
    [int]$SessionPlan.checkpoint_every_blocks -le 0 -or
    [int]$ActivePlan.compute_threads + [int]$SessionPlan.compute_threads -ne
        [int]$Contract.global_active_native_compute_threads) {
    throw "1024 frozen execution plan identity, checkpoint, or thread drift"
}
foreach ($Plan in @($ActivePlan, $SessionPlan)) {
    if ([string]$Plan.thread_environment.NUMBA_NUM_THREADS -ne [string]$Plan.compute_threads -or
        [string]$Plan.thread_environment.ARROW_NUM_THREADS -ne "1" -or
        [string]$Plan.thread_environment.OMP_NUM_THREADS -ne "1" -or
        [string]$Plan.thread_environment.MKL_NUM_THREADS -ne "1" -or
        [string]$Plan.thread_environment.OPENBLAS_NUM_THREADS -ne "1" -or
        [string]$Plan.thread_environment.NUMEXPR_MAX_THREADS -ne "1" -or
        [string]$Plan.thread_environment.POLARS_MAX_THREADS -ne "1" -or
        [int64]$Plan.rss_soft_bytes -le 0 -or
        [int64]$Plan.rss_soft_bytes -ge [int64]$Plan.rss_hard_bytes) {
        throw "1024 frozen execution plan native-pool or RSS ordering drift"
    }
}

$SidecarEvidence = Read-CnBoundJson `
    -Path $SidecarClosure -ExpectedSha256 $ExpectedSidecarClosureSha256 `
    -Label "1024 sidecar closure" -Base $RepoRoot
$SidecarClosure = $SidecarEvidence.path
$Sidecars = $SidecarEvidence.payload
if ($Sidecars.schema_version -ne "cn_phase3cm_1024_sidecar_closure_v1" -or
    $Sidecars.status -ne "CN_PHASE3CM_1024_SIDECAR_CLOSURE_PASS" -or
    $Sidecars.data_role -ne "development_train_only" -or
    [int]$Sidecars.pair_counts.active_bar -ne $ExpectedActivePairs -or
    [int]$Sidecars.pair_counts.stock_session -ne $ExpectedSessionPairs -or
    [int]$Sidecars.pair_counts.total -ne $ExpectedTotalPairs -or
    [int]$Sidecars.candidate_member_counts.active_bar -ne (2 * $ExpectedActivePairs) -or
    [int]$Sidecars.candidate_member_counts.stock_session -ne (2 * $ExpectedSessionPairs) -or
    $Sidecars.coverage_audit.all_required_fields_present -ne $true -or
    -not (Test-CnZeroAccessEvidence -Payload $Sidecars) -or
    $Sidecars.promotion -ne "FORBIDDEN" -or
    $Sidecars.strict_stage_a -ne "NOT_AUTHORIZED" -or
    $Sidecars.formal_evaluator_authority -ne "UNCHANGED" -or
    $Sidecars.streaming_backend_authority -ne "EXPERIMENTAL_BACKEND") {
    throw "1024 sidecar closure drift"
}
$ActiveCandidateTable = Confirm-CnFileBinding `
    -Path ([string]$Sidecars.candidate_tables.active_bar.path) `
    -Sha256 ([string]$Sidecars.candidate_tables.active_bar.sha256) `
    -Label "active 1024 candidate table" -Base $RepoRoot
$SessionCandidateTable = Confirm-CnFileBinding `
    -Path ([string]$Sidecars.candidate_tables.stock_session.path) `
    -Sha256 ([string]$Sidecars.candidate_tables.stock_session.sha256) `
    -Label "session 1024 candidate table" -Base $RepoRoot
$CandidateRoot = (Split-Path -Parent $ActiveCandidateTable)
if (-not [string]::Equals(
    $CandidateRoot,
    (Split-Path -Parent $SessionCandidateTable),
    [StringComparison]::OrdinalIgnoreCase
)) { throw "1024 candidate tables do not share one frozen artifact root" }
$ActiveFieldRoot = Resolve-CnPath -Path ([string]$Sidecars.active_sidecar.root) -Base $RepoRoot -Directory
$SessionFieldRoot = Resolve-CnPath -Path ([string]$Sidecars.session_sidecar.root) -Base $RepoRoot -Directory
$ActiveLabelRoot = Resolve-CnPath `
    -Path ([string]$Sidecars.coverage_audit.backends.active.label_root) -Base $RepoRoot -Directory
$SessionLabelRoot = Resolve-CnPath `
    -Path ([string]$Sidecars.coverage_audit.backends.session.label_root) -Base $RepoRoot -Directory
$SplitManifest = Confirm-CnFileBinding `
    -Path ([string]$Sidecars.split_manifest.path) `
    -Sha256 ([string]$Sidecars.split_manifest.sha256) `
    -Label "development split manifest" -Base $RepoRoot
if ([string]$Sidecars.split_manifest.sha256 -ne [string]$Binding.split_manifest_hash) {
    throw "sidecar closure and frozen binding split identity drift"
}

$SubsetEvidence = Read-CnBoundJson `
    -Path $HistoricalSubsetReceipt -ExpectedSha256 $ExpectedHistoricalSubsetReceiptSha256 `
    -Label "historical 256 semantic subset receipt" -Base $RepoRoot
$HistoricalSubsetReceipt = $SubsetEvidence.path
$Subset = $SubsetEvidence.payload
if ($Subset.schema_version -ne "cn_phase3cm_historical_256_semantic_subset_receipt_v2" -or
    $Subset.status -ne "CN_PHASE3CM_HISTORICAL_256_SEMANTIC_SUBSET_PASS" -or
    [int]$Subset.historical_pair_count -ne 256 -or
    [int]$Subset.freeze_pair_count -ne $ExpectedTotalPairs -or
    [int]$Subset.historical_candidate_count -ne 512 -or
    [int]$Subset.freeze_candidate_count -ne $ExpectedCandidateMembers -or
    $Subset.pair_identity_subset_exact -ne $true -or
    $Subset.candidate_identity_subset_exact -ne $true -or
    $Subset.pair_semantics_exact -ne $true -or
    $Subset.candidate_semantics_exact -ne $true -or
    @($Subset.missing_pair_ids).Count -ne 0 -or
    @($Subset.missing_candidate_ids).Count -ne 0 -or
    @($Subset.pair_semantic_mismatch_ids).Count -ne 0 -or
    @($Subset.candidate_semantic_mismatch_ids).Count -ne 0) {
    throw "historical 256 semantic subset gate is not exact PASS"
}

$ReadjudicationEvidence = Read-CnBoundJson `
    -Path $ReadjudicationManifest `
    -ExpectedSha256 $ExpectedReadjudicationManifestSha256 `
    -Label "146 readjudication manifest" -Base $RepoRoot
$ReadjudicationManifest = $ReadjudicationEvidence.path
$Readjudication = $ReadjudicationEvidence.payload
$ExpectedCheckpointCategories = @("temporal", "state", "support", "portfolio", "reducer")
if ($Readjudication.schema_version -ne "cn_phase3cm_current_kernel_146_readjudication_manifest_v1" -or
    $Readjudication.status -ne "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_READJUDICATED_PASS" -or
    $Readjudication.identity_coverage_exact -ne $true -or
    -not (Test-CnSequenceEqual -Left @($Readjudication.checkpoint_categories) -Right $ExpectedCheckpointCategories) -or
    $Readjudication.next_decision -ne "FREEZE_1024_RESOURCE_AND_EXECUTION_CONTRACT" -or
    -not (Test-CnZeroAccessEvidence -Payload $Readjudication.boundaries) -or
    $Readjudication.boundaries.promotion -ne "FORBIDDEN" -or
    $Readjudication.boundaries.strict_stage_a -ne "NOT_AUTHORIZED") {
    throw "146 readjudication manifest drift"
}
$ReadjudicatedReceiptPath = Confirm-CnFileBinding `
    -Path ([string]$Readjudication.readjudicated_parity_receipt) `
    -Sha256 ([string]$Readjudication.readjudicated_parity_receipt_sha256) `
    -Label "146 readjudicated parity receipt" -Base (Split-Path -Parent $ReadjudicationManifest)
$ReadjudicatedReceipt = Get-Content -LiteralPath $ReadjudicatedReceiptPath -Raw | ConvertFrom-Json
if ($ReadjudicatedReceipt.schema_version -ne "cn_phase3cm_current_kernel_146_parity_final_v1" -or
    $ReadjudicatedReceipt.status -ne "CN_PHASE3CM_CURRENT_KERNEL_146_PARITY_PASS" -or
    $ReadjudicatedReceipt.identity_coverage_exact -ne $true -or
    $ReadjudicatedReceipt.kernel_state -ne "PARTIALLY_QUALIFIED" -or
    $ReadjudicatedReceipt.backend_authority -ne "EXPERIMENTAL_BACKEND" -or
    $ReadjudicatedReceipt.formal_evaluator_authority -ne "UNCHANGED" -or
    $ReadjudicatedReceipt.performance_threshold_gate -ne "NOT_USED" -or
    $ReadjudicatedReceipt.two_x_speedup_required -ne $false -or
    -not (Test-CnZeroAccessEvidence -Payload $ReadjudicatedReceipt.boundaries) -or
    $ReadjudicatedReceipt.boundaries.promotion -ne "FORBIDDEN" -or
    $ReadjudicatedReceipt.boundaries.strict_stage_a -ne "NOT_AUTHORIZED") {
    throw "146 readjudicated parity receipt is not exact semantic PASS"
}

$ResourceEvidence = Read-CnBoundJson `
    -Path $ResourceContract -ExpectedSha256 $ExpectedResourceContractSha256 `
    -Label "1024 resource contract" -Base $RepoRoot
$ResourceContract = $ResourceEvidence.path
$Resources = $ResourceEvidence.payload
$WallSecondsHardMax = [double]$Resources.wall_seconds_hard_max
$GlobalHardRss = [int64]$Resources.global_rss_hard_bytes
$ActiveHardRss = [int64]$Resources.rss_hard_bytes_by_backend.active_bar
$SessionHardRss = [int64]$Resources.rss_hard_bytes_by_backend.stock_session
if ($Resources.schema_version -ne "cn_phase3cm_1024_resource_contract_v1" -or
    $Resources.status -ne "CN_PHASE3CM_1024_RESOURCE_CONTRACT_PASS" -or
    [string]$Resources.repo_sha -ne $ExpectedRepoSha -or
    [int]$Resources.total_pair_count -ne $ExpectedTotalPairs -or
    [int]$Resources.backend_pair_counts.active_bar -ne $ExpectedActivePairs -or
    [int]$Resources.backend_pair_counts.stock_session -ne $ExpectedSessionPairs -or
    [int]$Resources.heavy_processes -ne 2 -or
    [int]$Resources.global_active_native_compute_threads -ne
        [int]$Contract.global_active_native_compute_threads -or
    [int]$Resources.global_active_native_compute_threads -gt 24 -or
    $ActiveHardRss -le 0 -or $SessionHardRss -le 0 -or
    $GlobalHardRss -lt $ActiveHardRss -or $GlobalHardRss -lt $SessionHardRss -or
    $WallSecondsHardMax -le 0.0 -or
    [double]$Resources.host_hours_projected -le 0.0 -or
    [double]$Resources.host_hours_projected -gt [double]$Resources.host_hours_hard_max -or
    [double]$Resources.host_hours_hard_max -ne 12.0 -or
    $WallSecondsHardMax -gt (3600.0 * [double]$Resources.host_hours_hard_max) -or
    [string]$Resources.speedup_threshold -ne "NONE" -or
    [string]$Resources.data_role -ne "development_train_only" -or
    -not (Test-CnZeroAccessEvidence -Payload $Resources) -or
    $Resources.promotion -ne "FORBIDDEN" -or
    $Resources.strict_stage_a -ne "NOT_AUTHORIZED" -or
    $Resources.formal_evaluator_authority -ne "UNCHANGED" -or
    $Resources.streaming_backend_authority -ne "EXPERIMENTAL_BACKEND" -or
    $Resources.kernel_state -ne "PARTIALLY_QUALIFIED" -or
    [int64]$ActivePlan.rss_hard_bytes -ne $ActiveHardRss -or
    [int64]$SessionPlan.rss_hard_bytes -ne $SessionHardRss -or
    [int64]$ActivePlan.global_rss_hard_bytes -ne $GlobalHardRss -or
    [int64]$SessionPlan.global_rss_hard_bytes -ne $GlobalHardRss) {
    throw "1024 absolute resource contract drift"
}

$SourceClosureEvidence = Read-CnBoundJson `
    -Path $SourceClosureManifest `
    -ExpectedSha256 $ExpectedSourceClosureManifestSha256 `
    -Label "1024 source closure manifest" -Base $RepoRoot
$SourceClosureManifest = $SourceClosureEvidence.path
$SourceClosureValidator = Resolve-CnPath `
    -Path "scripts\validate_cn_phase3cm_source_closure.py" -Base $RepoRoot
$SourceClosureOutput = @(& $PythonExe $SourceClosureValidator `
    --manifest $SourceClosureManifest `
    --repo-root $RepoRoot `
    --expected-repo-sha $ExpectedRepoSha)
if ($LASTEXITCODE -ne 0 -or $SourceClosureOutput.Count -eq 0) {
    throw "1024 exact source closure validation failed"
}
$SourceClosureValidation = $SourceClosureOutput[-1] | ConvertFrom-Json
if ([string]$SourceClosureValidation.repo_sha -ne $ExpectedRepoSha) {
    throw "1024 source closure repo SHA drift"
}
$PortfolioSource = Resolve-CnPath `
    -Path "src\our_system_phase2\services\phase3cm_streaming_portfolio.py" -Base $RepoRoot
if ((Get-CnSha256 -Path $PortfolioSource) -ne $FrozenPortfolioSourceSha256) {
    throw "frozen PARTIALLY_QUALIFIED portfolio kernel source drift"
}

$ActiveCapacityReceipt = Resolve-CnPath -Path $ActiveCapacityReceipt -Base $RepoRoot
$SessionCapacityReceipt = Resolve-CnPath -Path $SessionCapacityReceipt -Base $RepoRoot
$ActiveCapacityValidation = Confirm-CnCapacityReceipt `
    -Receipt $ActiveCapacityReceipt -Backend "active_bar" `
    -CandidateTable $ActiveCandidateTable -ExecutionPlan $ActiveExecutionPlanPath `
    -FieldRoot $ActiveFieldRoot -LabelRoot $ActiveLabelRoot `
    -Repo $RepoRoot -Closure $SourceClosureManifest -RepoSha $ExpectedRepoSha -Python $PythonExe
$SessionCapacityValidation = Confirm-CnCapacityReceipt `
    -Receipt $SessionCapacityReceipt -Backend "stock_session" `
    -CandidateTable $SessionCandidateTable -ExecutionPlan $SessionExecutionPlanPath `
    -FieldRoot $SessionFieldRoot -LabelRoot $SessionLabelRoot `
    -Repo $RepoRoot -Closure $SourceClosureManifest -RepoSha $ExpectedRepoSha -Python $PythonExe
if ([string]$ActiveCapacityValidation.execution_plan_hash -ne [string]$ActivePlan.execution_plan_hash -or
    [string]$SessionCapacityValidation.execution_plan_hash -ne [string]$SessionPlan.execution_plan_hash -or
    [string]$ActiveCapacityValidation.dag_plan_hash -ne [string]$Contract.plans.active_bar.dag_plan_hash -or
    [string]$SessionCapacityValidation.dag_plan_hash -ne [string]$Contract.plans.stock_session.dag_plan_hash -or
    [string]$ActiveCapacityValidation.source_closure_manifest_sha256 -ne $SourceClosureEvidence.sha256 -or
    [string]$SessionCapacityValidation.source_closure_manifest_sha256 -ne $SourceClosureEvidence.sha256 -or
    [string]$ActiveCapacityValidation.source_closure_manifest_hash -ne
        [string]$SessionCapacityValidation.source_closure_manifest_hash) {
    throw "1024 capacity receipts do not bind the frozen plans and exact source closure"
}

# No output directory or heavy process is created before every frozen gate above passes.
$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = "1"
. (Join-Path $RepoRoot "scripts\cn_phase3cm_process_tree_monitor.ps1")

$RunRoot = Join-Path $RunParentRoot $OutputName
if ((Test-Path -LiteralPath $RunRoot) -and -not $Resume) {
    throw "1024 output already exists; refuse overwrite or implicit adaptation: $RunRoot"
}
if ($Resume -and -not (Test-Path -LiteralPath $RunRoot -PathType Container)) {
    throw "1024 resume requested without an existing output root: $RunRoot"
}
$ActiveRoot = Join-Path $RunRoot "active_bar"
$SessionRoot = Join-Path $RunRoot "stock_session"
if ($Resume) {
    foreach ($BackendCheckpoint in @(
        (Join-Path $ActiveRoot "checkpoints\active_bar\CN_STREAMING_CHECKPOINT_MANIFEST.json"),
        (Join-Path $SessionRoot "checkpoints\stock_session\CN_STREAMING_CHECKPOINT_MANIFEST.json")
    )) {
        if (-not (Test-Path -LiteralPath $BackendCheckpoint -PathType Leaf)) {
            throw "1024 resume requires complete periodic checkpoint manifest: $BackendCheckpoint"
        }
    }
}
New-Item -ItemType Directory -Force -Path $ActiveRoot,$SessionRoot | Out-Null

function New-CnBackendArguments {
    param(
        [string]$Backend,
        [int]$PairCount,
        [string]$CandidateTable,
        [string]$FieldRoot,
        [string]$LabelRoot,
        [string]$OutputRoot,
        [string]$ExecutionPlan,
        [object]$Plan,
        [object]$CapacityValidation
    )
    $PairBatchSize = [int](($Plan.pair_batches | ForEach-Object { @($_).Count } |
        Measure-Object -Maximum).Maximum)
    $Arguments = @(
        "scripts\run_cn_phase3cm_streaming_qualification.py",
        "--backend", $Backend,
        "--phase", "E",
        "--pair-count", [string]$PairCount,
        "--candidate-table", $CandidateTable,
        "--binding", $FrozenBinding,
        "--split-manifest", $SplitManifest,
        "--artifact-root", $CandidateRoot,
        "--field-sidecar-root", $FieldRoot,
        "--label-sidecar-root", $LabelRoot,
        "--output-root", $OutputRoot,
        "--execution-plan", $ExecutionPlan,
        "--block-sessions", [string]$Plan.block_size,
        "--pair-batch-size", [string]$PairBatchSize,
        "--compute-threads", [string]$Plan.compute_threads,
        "--max-block-rows", [string]$CapacityValidation.max_block_rows,
        "--capacity-receipt-hash", [string]$CapacityValidation.receipt_hash
    )
    if ($Resume) { $Arguments += "--resume" }
    return $Arguments
}

function New-CnBackendThreadEnvironment {
    param([object]$Plan)
    $Environment = [ordered]@{}
    foreach ($Property in $Plan.thread_environment.PSObject.Properties) {
        $Environment[[string]$Property.Name] = [string]$Property.Value
    }
    return $Environment
}

$ActiveArgs = New-CnBackendArguments `
    -Backend "active_bar" -PairCount $ExpectedActivePairs `
    -CandidateTable $ActiveCandidateTable -FieldRoot $ActiveFieldRoot `
    -LabelRoot $ActiveLabelRoot -OutputRoot $ActiveRoot `
    -ExecutionPlan $ActiveExecutionPlanPath -Plan $ActivePlan `
    -CapacityValidation $ActiveCapacityValidation
$SessionArgs = New-CnBackendArguments `
    -Backend "stock_session" -PairCount $ExpectedSessionPairs `
    -CandidateTable $SessionCandidateTable -FieldRoot $SessionFieldRoot `
    -LabelRoot $SessionLabelRoot -OutputRoot $SessionRoot `
    -ExecutionPlan $SessionExecutionPlanPath -Plan $SessionPlan `
    -CapacityValidation $SessionCapacityValidation

$ActiveCommandPath = Join-Path $ActiveRoot "CN_BACKEND_COMMAND.json"
$SessionCommandPath = Join-Path $SessionRoot "CN_BACKEND_COMMAND.json"
$ActiveExitReceiptPath = Join-Path $ActiveRoot "CN_BACKEND_EXIT_RECEIPT.json"
$SessionExitReceiptPath = Join-Path $SessionRoot "CN_BACKEND_EXIT_RECEIPT.json"
$ActiveResultPath = Join-Path $ActiveRoot "CN_STREAMING_BACKEND_RESULT.json"
$SessionResultPath = Join-Path $SessionRoot "CN_STREAMING_BACKEND_RESULT.json"
if ($Resume) {
    foreach ($StaleFinal in @(
        $ActiveExitReceiptPath, $SessionExitReceiptPath, $ActiveResultPath, $SessionResultPath
    )) {
        if (Test-Path -LiteralPath $StaleFinal -PathType Leaf) {
            Remove-Item -LiteralPath $StaleFinal -Force
        }
    }
}
Write-CnAtomicJson -Path $ActiveCommandPath -Payload ([ordered]@{
    schema_version = "cn_phase3cm_backend_command_v1"
    backend = "active_bar"
    arguments = $ActiveArgs
    thread_environment = New-CnBackendThreadEnvironment -Plan $ActivePlan
})
Write-CnAtomicJson -Path $SessionCommandPath -Payload ([ordered]@{
    schema_version = "cn_phase3cm_backend_command_v1"
    backend = "stock_session"
    arguments = $SessionArgs
    thread_environment = New-CnBackendThreadEnvironment -Plan $SessionPlan
})
$ActiveCommandSha256 = Get-CnSha256 -Path $ActiveCommandPath
$SessionCommandSha256 = Get-CnSha256 -Path $SessionCommandPath

$Wrapper = Resolve-CnPath `
    -Path "scripts\invoke_cn_phase3cm_backend_with_exit_receipt.ps1" -Base $RepoRoot
$PowerShellExe = (Get-Command powershell.exe).Source
$RssTimelinePath = Join-Path $RunRoot "CN_GLOBAL_PROCESS_TREE_RSS_TIMELINE.csv"
"sampled_at,active_process_ids,active_rss_bytes,session_process_ids,session_rss_bytes,global_rss_bytes" |
    Set-Content -LiteralPath $RssTimelinePath -Encoding UTF8
$Processes = @()
$Roots = @{}
$GlobalPeakRss = [int64]0
$ActivePeakRss = [int64]0
$SessionPeakRss = [int64]0
$GlobalGateFailure = $false
$ActiveRssGateFailure = $false
$SessionRssGateFailure = $false
$WallGateFailure = $false
$LaunchFailure = $null
$RssSampleCount = 0
$Stopwatch = [Diagnostics.Stopwatch]::StartNew()
try {
    $Active = Start-Process -FilePath $PowerShellExe -ArgumentList @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
        "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
        "-ArgumentFile", $ActiveCommandPath, "-ExitReceipt", $ActiveExitReceiptPath
    ) -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput (Join-Path $ActiveRoot "stdout.log") `
        -RedirectStandardError (Join-Path $ActiveRoot "stderr.log") `
        -WindowStyle Hidden -PassThru
    $Processes += $Active
    $Roots["active_bar"] = $Active.Id
    $Session = Start-Process -FilePath $PowerShellExe -ArgumentList @(
        "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
        "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
        "-ArgumentFile", $SessionCommandPath, "-ExitReceipt", $SessionExitReceiptPath
    ) -WorkingDirectory $RepoRoot `
        -RedirectStandardOutput (Join-Path $SessionRoot "stdout.log") `
        -RedirectStandardError (Join-Path $SessionRoot "stderr.log") `
        -WindowStyle Hidden -PassThru
    $Processes += $Session
    $Roots["stock_session"] = $Session.Id

    while ($true) {
        $Running = @()
        foreach ($Process in $Processes) {
            $Process.Refresh()
            if (-not $Process.HasExited) { $Running += $Process }
        }
        $Snapshot = Get-CnProcessTreeRssSnapshot -Roots $Roots
        Add-CnRssTimelineSample -Path $RssTimelinePath -Snapshot $Snapshot
        $RssSampleCount += 1
        $CurrentGlobalRss = [int64]$Snapshot.total_rss_bytes
        $GlobalPeakRss = [Math]::Max($GlobalPeakRss, $CurrentGlobalRss)
        $ActivePeakRss = [Math]::Max(
            $ActivePeakRss, [int64]$Snapshot.groups["active_bar"].rss_bytes
        )
        $SessionPeakRss = [Math]::Max(
            $SessionPeakRss, [int64]$Snapshot.groups["stock_session"].rss_bytes
        )
        if ([int64]$Snapshot.groups["active_bar"].rss_bytes -ge $ActiveHardRss) {
            $ActiveRssGateFailure = $true
            Stop-CnProcessTrees -Roots $Roots
            break
        }
        if ([int64]$Snapshot.groups["stock_session"].rss_bytes -ge $SessionHardRss) {
            $SessionRssGateFailure = $true
            Stop-CnProcessTrees -Roots $Roots
            break
        }
        if ($CurrentGlobalRss -ge $GlobalHardRss) {
            $GlobalGateFailure = $true
            Stop-CnProcessTrees -Roots $Roots
            break
        }
        if ($Stopwatch.Elapsed.TotalSeconds -ge $WallSecondsHardMax) {
            $WallGateFailure = $true
            Stop-CnProcessTrees -Roots $Roots
            break
        }
        if ($Running.Count -eq 0) { break }
        Start-Sleep -Seconds 2
    }
}
catch {
    $LaunchFailure = $_.Exception.Message
    if ($Roots.Count -gt 0) { Stop-CnProcessTrees -Roots $Roots }
}
finally {
    foreach ($Process in $Processes) {
        try { $Process.WaitForExit(); $Process.Refresh() } catch { }
    }
    $Stopwatch.Stop()
}

function Get-CnBackendEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$Backend,
        [Parameter(Mandatory = $true)][int]$PairCount,
        [Parameter(Mandatory = $true)][string]$ExitReceiptPath,
        [Parameter(Mandatory = $true)][string]$ResultPath,
        [Parameter(Mandatory = $true)][string]$CommandSha256,
        [Parameter(Mandatory = $true)][object]$Plan,
        [Parameter(Mandatory = $true)][object]$CapacityValidation,
        [Parameter(Mandatory = $true)][string]$ExpectedDagPlanHash,
        [Parameter(Mandatory = $true)][int64]$RssHardBytes,
        [Parameter(Mandatory = $true)][object[]]$ExpectedPairIds,
        [object]$Process
    )
    $ExitReceipt = if (Test-Path -LiteralPath $ExitReceiptPath -PathType Leaf) {
        Get-Content -LiteralPath $ExitReceiptPath -Raw | ConvertFrom-Json
    } else { $null }
    $Result = if (Test-Path -LiteralPath $ResultPath -PathType Leaf) {
        Get-Content -LiteralPath $ResultPath -Raw | ConvertFrom-Json
    } else { $null }
    $ActualExitCode = if ($null -ne $Process -and $Process.HasExited) {
        [int]$Process.ExitCode
    } else { -1 }
    $ObservedPairIds = if ($null -ne $Result) {
        @($Result.pair_results | ForEach-Object { [string]$_.pair_id } | Sort-Object)
    } else { @() }
    $ExpectedSorted = @($ExpectedPairIds | ForEach-Object { [string]$_ } | Sort-Object)
    $PairIdentityPass = (
        $ObservedPairIds.Count -eq $ExpectedSorted.Count -and
        @(Compare-Object $ExpectedSorted $ObservedPairIds).Count -eq 0
    )
    $CheckpointManifest = Join-Path (
        Split-Path -Parent $ResultPath
    ) "checkpoints\$Backend\CN_STREAMING_CHECKPOINT_MANIFEST.json"
    $Pass = (
        $null -ne $ExitReceipt -and
        $ExitReceipt.schema_version -eq "cn_phase3cm_backend_exit_receipt_v1" -and
        $ExitReceipt.status -eq "CN_PHASE3CM_BACKEND_PROCESS_COMPLETED" -and
        [int]$ExitReceipt.exit_code -eq 0 -and
        $ActualExitCode -eq 0 -and
        [string]$ExitReceipt.argument_file_sha256 -eq $CommandSha256 -and
        $null -ne $Result -and
        $Result.schema_version -eq "cn_phase3cm_streaming_backend_result_v1" -and
        $Result.status -eq "CN_PHASE3CM_STREAMING_BACKEND_COMPLETED" -and
        $Result.backend -eq $Backend -and
        $Result.phase -eq "E" -and
        [int]$Result.pair_count -eq $PairCount -and
        [int]$Result.candidate_count -eq (2 * $PairCount) -and
        [string]$Result.execution_plan_hash -eq [string]$Plan.execution_plan_hash -and
        [string]$Result.dag_plan_hash -eq $ExpectedDagPlanHash -and
        [string]$CapacityValidation.dag_plan_hash -eq $ExpectedDagPlanHash -and
        [string]$Result.capacity_receipt_hash -eq [string]$CapacityValidation.receipt_hash -and
        [string]$Result.input_binding_hash -eq [string]$Binding.binding_hash -and
        [string]$Result.split_manifest_hash -eq [string]$Binding.split_manifest_hash -and
        $Result.block_row_guard_status -eq "BLOCK_ROW_GUARD_PASS" -and
        [int64]$Result.max_block_rows_contract -eq [int64]$CapacityValidation.max_block_rows -and
        [int64]$Result.max_observed_block_rows -le [int64]$CapacityValidation.max_block_rows -and
        [int64]$Result.peak_rss_bytes -lt $RssHardBytes -and
        [int]$Result.coordinate_rows_retained -eq 0 -and
        $Result.parallelism_status -eq "PARALLELISM_ENGAGED" -and
        -not ($Result.compute_phase_parallelism.PSObject.Properties.Value |
            Where-Object { $_.parallelism_status -eq "PARALLELISM_NOT_ENGAGED" }) -and
        (Test-CnZeroAccessEvidence -Payload $Result) -and
        $Result.promotion -eq "FORBIDDEN" -and
        $Result.strict_stage_a -eq "NOT_AUTHORIZED" -and
        $PairIdentityPass -and
        (Test-Path -LiteralPath $CheckpointManifest -PathType Leaf)
    )
    return [ordered]@{
        backend = $Backend
        pass = $Pass
        actual_exit_code = $ActualExitCode
        exit_receipt = $ExitReceiptPath
        exit_receipt_sha256 = if ($null -ne $ExitReceipt) { Get-CnSha256 -Path $ExitReceiptPath } else { $null }
        backend_result = $ResultPath
        backend_result_sha256 = if ($null -ne $Result) { Get-CnSha256 -Path $ResultPath } else { $null }
        pair_count = $PairCount
        pair_identity_pass = $PairIdentityPass
        checkpoint_manifest = $CheckpointManifest
        checkpoint_manifest_sha256 = if (Test-Path -LiteralPath $CheckpointManifest -PathType Leaf) {
            Get-CnSha256 -Path $CheckpointManifest
        } else { $null }
        execution_plan_hash = [string]$Plan.execution_plan_hash
        dag_plan_hash = $ExpectedDagPlanHash
        capacity_receipt_hash = [string]$CapacityValidation.receipt_hash
        peak_rss_bytes = if ($null -ne $Result) { [int64]$Result.peak_rss_bytes } else { $null }
        rss_hard_bytes = $RssHardBytes
        access_evidence_complete = if ($null -ne $Result) {
            Test-CnZeroAccessEvidence -Payload $Result
        } else { $false }
    }
}

$ActiveProcess = if ($Processes.Count -ge 1) { $Processes[0] } else { $null }
$SessionProcess = if ($Processes.Count -ge 2) { $Processes[1] } else { $null }
$ActiveEvidence = Get-CnBackendEvidence `
    -Backend "active_bar" -PairCount $ExpectedActivePairs `
    -ExitReceiptPath $ActiveExitReceiptPath -ResultPath $ActiveResultPath `
    -CommandSha256 $ActiveCommandSha256 -Plan $ActivePlan `
    -CapacityValidation $ActiveCapacityValidation `
    -ExpectedDagPlanHash ([string]$Contract.plans.active_bar.dag_plan_hash) `
    -RssHardBytes $ActiveHardRss `
    -ExpectedPairIds $ActivePairIds -Process $ActiveProcess
$SessionEvidence = Get-CnBackendEvidence `
    -Backend "stock_session" -PairCount $ExpectedSessionPairs `
    -ExitReceiptPath $SessionExitReceiptPath -ResultPath $SessionResultPath `
    -CommandSha256 $SessionCommandSha256 -Plan $SessionPlan `
    -CapacityValidation $SessionCapacityValidation `
    -ExpectedDagPlanHash ([string]$Contract.plans.stock_session.dag_plan_hash) `
    -RssHardBytes $SessionHardRss `
    -ExpectedPairIds $SessionPairIds -Process $SessionProcess
$WallGatePass = (-not $WallGateFailure -and $Stopwatch.Elapsed.TotalSeconds -le $WallSecondsHardMax)
$Pass = (
    $ActiveEvidence.pass -and $SessionEvidence.pass -and
    -not $ActiveRssGateFailure -and -not $SessionRssGateFailure -and
    -not $GlobalGateFailure -and $WallGatePass -and
    $ActivePeakRss -lt $ActiveHardRss -and
    $SessionPeakRss -lt $SessionHardRss -and
    $GlobalPeakRss -lt $GlobalHardRss -and $null -eq $LaunchFailure
)
$Receipt = [ordered]@{
    schema_version = "cn_phase3cm_1024_wave_execution_receipt_v1"
    status = if ($Pass) {
        "CN_PHASE3CM_1024_WAVE_PASS"
    } elseif ($GlobalGateFailure) {
        "CN_PHASE3CM_1024_WAVE_GLOBAL_RSS_FAIL_CLOSED"
    } elseif ($ActiveRssGateFailure -or $SessionRssGateFailure) {
        "CN_PHASE3CM_1024_WAVE_BACKEND_RSS_FAIL_CLOSED"
    } elseif (-not $WallGatePass) {
        "CN_PHASE3CM_1024_WAVE_WALL_FAIL_CLOSED"
    } elseif ($null -ne $LaunchFailure) {
        "CN_PHASE3CM_1024_WAVE_LAUNCH_FAIL_CLOSED"
    } else {
        "CN_PHASE3CM_1024_WAVE_FAIL_CLOSED"
    }
    repo_sha = $ExpectedRepoSha
    frozen_binding = @{ path = $FrozenBinding; sha256 = $BindingEvidence.sha256; binding_hash = [string]$Binding.binding_hash }
    combined_contract = @{ path = $CombinedContract; sha256 = $CombinedEvidence.sha256 }
    sidecar_closure = @{ path = $SidecarClosure; sha256 = $SidecarEvidence.sha256; closure_hash = [string]$Sidecars.closure_hash }
    resource_contract = @{ path = $ResourceContract; sha256 = $ResourceEvidence.sha256 }
    historical_subset_receipt = @{ path = $HistoricalSubsetReceipt; sha256 = $SubsetEvidence.sha256; status = [string]$Subset.status }
    readjudication_manifest = @{ path = $ReadjudicationManifest; sha256 = $ReadjudicationEvidence.sha256; status = [string]$Readjudication.status }
    source_closure = @{
        path = $SourceClosureManifest
        sha256 = $SourceClosureEvidence.sha256
        manifest_hash = [string]$SourceClosureValidation.manifest_hash
        source_closure_hash = [string]$SourceClosureValidation.source_closure_hash
    }
    pair_counts = @{ active_bar = $ExpectedActivePairs; stock_session = $ExpectedSessionPairs; total = $ExpectedTotalPairs }
    candidate_member_count = $ExpectedCandidateMembers
    heavy_processes = 2
    compute_threads_by_backend = @{
        active_bar = [int]$ActivePlan.compute_threads
        stock_session = [int]$SessionPlan.compute_threads
    }
    global_active_native_compute_threads = [int]$Contract.global_active_native_compute_threads
    wall_seconds = $Stopwatch.Elapsed.TotalSeconds
    wall_seconds_hard_max = $WallSecondsHardMax
    wall_gate_pass = $WallGatePass
    host_hours_actual = $Stopwatch.Elapsed.TotalHours
    host_hours_hard_max = [double]$Resources.host_hours_hard_max
    global_peak_rss_bytes = $GlobalPeakRss
    global_rss_hard_bytes = $GlobalHardRss
    global_rss_gate_pass = (-not $GlobalGateFailure -and $GlobalPeakRss -lt $GlobalHardRss)
    active_peak_process_tree_rss_bytes = $ActivePeakRss
    session_peak_process_tree_rss_bytes = $SessionPeakRss
    active_rss_hard_bytes = $ActiveHardRss
    session_rss_hard_bytes = $SessionHardRss
    active_rss_gate_pass = (-not $ActiveRssGateFailure -and $ActivePeakRss -lt $ActiveHardRss)
    session_rss_gate_pass = (-not $SessionRssGateFailure -and $SessionPeakRss -lt $SessionHardRss)
    rss_sample_count = $RssSampleCount
    rss_timeline = $RssTimelinePath
    rss_timeline_sha256 = Get-CnSha256 -Path $RssTimelinePath
    launch_failure = $LaunchFailure
    resume = [bool]$Resume
    periodic_checkpoint_resume = "FROZEN_PLAN_CADENCE_AND_COMPLETE_PAYLOAD"
    active_backend = $ActiveEvidence
    stock_session_backend = $SessionEvidence
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    promotion = "FORBIDDEN"
    strict_stage_a = "NOT_AUTHORIZED"
    formal_evaluator_authority = "UNCHANGED"
    streaming_backend_authority = "EXPERIMENTAL_BACKEND"
    kernel_state = "PARTIALLY_QUALIFIED"
    cross_epoch_adaptive_memory = "FORBIDDEN"
    speedup_threshold = "NONE"
    performance_threshold_gate = "NOT_USED"
}
$ReceiptPath = Join-Path $RunRoot "CN_PHASE3CM_1024_WAVE_EXECUTION_RECEIPT.json"
Write-CnAtomicJson -Path $ReceiptPath -Payload $Receipt
Write-Output ($Receipt | ConvertTo-Json -Depth 12 -Compress)
if (-not $Pass) { exit 1 }
