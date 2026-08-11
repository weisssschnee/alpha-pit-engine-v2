param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [ValidateSet('LAUNCH_HIGH_COST_CAMPAIGN', 'RETRY', 'RECOVERY')]
    [string]$RequestedAction,
    [Parameter(Mandatory = $true)]
    [string]$TargetRunId,
    [Parameter(Mandatory = $true)]
    [string]$ProjectControlAdmission,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{64}$')]
    [string]$ProjectControlAdmissionSha256,
    [Parameter(Mandatory = $true)]
    [string]$PhaseBFreezeRoot,
    [Parameter(Mandatory = $true)]
    [string]$ExecutionContract,
    [Parameter(Mandatory = $true)]
    [string]$TrainFieldRoot,
    [Parameter(Mandatory = $true)]
    [string]$TrainPriceRoot,
    [Parameter(Mandatory = $true)]
    [string]$Registry,
    [Parameter(Mandatory = $true)]
    [string]$AcceptedFieldManifest,
    [Parameter(Mandatory = $true)]
    [string]$NodeResourceCapacity,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [string]$CampaignAuthorization = (
        'runtime\run_plans\cn_search_engine_v2_prospective_512_canary_v1.json'
    ),
    [string]$RootFinalizationRecoveryFromRepoSha = '',
    [string]$RootFinalizationIncident = '',
    [string]$RootFinalizationDeploymentManifest = '',
    [string]$CheckpointRecoveryFromRepoSha = '',
    [string]$CheckpointRecoveryIncident = '',
    [string]$CheckpointRecoveryDiagnosticAudit = '',
    [string]$CheckpointRecoveryDeploymentManifest = '',
    [int]$ExecutorWorkers = 10
)

$ErrorActionPreference = 'Stop'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$resolvedRepo = [IO.Path]::GetFullPath($Repo)
$resolvedOutput = [IO.Path]::GetFullPath($OutputRoot)
$resolvedAdmission = [IO.Path]::GetFullPath($ProjectControlAdmission)
$resolvedAuthorization = if ([IO.Path]::IsPathRooted($CampaignAuthorization)) {
    [IO.Path]::GetFullPath($CampaignAuthorization)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $CampaignAuthorization))
}

if ($env:COMPUTERNAME -ne 'DESKTOP-77OPJ6F') {
    throw "unauthorized host: $($env:COMPUTERNAME)"
}
if (-not $resolvedRepo.StartsWith(
    'D:\ChengboRemote\workspace\',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected repo path: $resolvedRepo"
}
if (-not $resolvedOutput.StartsWith(
    'D:\ChengboRemote\runtime\cn_search_engine_v2_canary_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected Search V2 output root: $resolvedOutput"
}
if ($RequestedAction -eq 'RECOVERY') {
    if (-not (Test-Path -LiteralPath $resolvedOutput -PathType Container)) {
        throw "Search V2 recovery requires the original output root"
    }
} elseif (Test-Path -LiteralPath $resolvedOutput) {
    throw "Search V2 launch/retry output root must be absent: $resolvedOutput"
}

$head = (& git -C $resolvedRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -ne $RepoSha) {
    throw "Search V2 executing checkout SHA drift: expected=$RepoSha actual=$head"
}
$dirty = @(& git -C $resolvedRepo status --porcelain --untracked-files=normal)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw "Search V2 executing checkout must be clean"
}

$requiredPaths = @(
    $python,
    $resolvedAdmission,
    $resolvedAuthorization,
    [IO.Path]::GetFullPath($PhaseBFreezeRoot),
    [IO.Path]::GetFullPath($ExecutionContract),
    [IO.Path]::GetFullPath($TrainFieldRoot),
    [IO.Path]::GetFullPath($TrainPriceRoot),
    [IO.Path]::GetFullPath($Registry),
    [IO.Path]::GetFullPath($AcceptedFieldManifest),
    [IO.Path]::GetFullPath($NodeResourceCapacity),
    (Join-Path $resolvedRepo 'app.py')
)
foreach ($path in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required Search V2 input missing: $path"
    }
}

$routeArgs = @(
    (Join-Path $resolvedRepo 'app.py'),
    'cn-joint-program-search-v2-canary',
    '--requested-action', $RequestedAction,
    '--target-run-id', $TargetRunId,
    '--project-control-admission', $resolvedAdmission,
    '--project-control-admission-sha256', $ProjectControlAdmissionSha256,
    '--',
    '--campaign-authorization', $resolvedAuthorization,
    '--phase-b-freeze-root', [IO.Path]::GetFullPath($PhaseBFreezeRoot),
    '--execution-contract', [IO.Path]::GetFullPath($ExecutionContract),
    '--train-field-root', [IO.Path]::GetFullPath($TrainFieldRoot),
    '--train-price-root', [IO.Path]::GetFullPath($TrainPriceRoot),
    '--registry', [IO.Path]::GetFullPath($Registry),
    '--accepted-field-manifest', [IO.Path]::GetFullPath($AcceptedFieldManifest),
    '--node-resource-capacity', [IO.Path]::GetFullPath($NodeResourceCapacity),
    '--output-root', $resolvedOutput,
    '--executor-workers', $ExecutorWorkers
)

foreach ($binding in @(
    @('--root-finalization-recovery-from-repo-sha', $RootFinalizationRecoveryFromRepoSha),
    @('--checkpoint-recovery-from-repo-sha', $CheckpointRecoveryFromRepoSha)
)) {
    if ($binding[1]) {
        if ($binding[1] -notmatch '^[0-9a-f]{40}$') {
            throw "recovery repository SHA must be full length: $($binding[1])"
        }
        $routeArgs += $binding[0]
        $routeArgs += $binding[1]
    }
}
foreach ($binding in @(
    @('--root-finalization-incident', $RootFinalizationIncident),
    @('--root-finalization-deployment-manifest', $RootFinalizationDeploymentManifest),
    @('--checkpoint-recovery-incident', $CheckpointRecoveryIncident),
    @('--checkpoint-recovery-diagnostic-audit', $CheckpointRecoveryDiagnosticAudit),
    @('--checkpoint-recovery-deployment-manifest', $CheckpointRecoveryDeploymentManifest)
)) {
    if ($binding[1]) {
        $resolvedBinding = [IO.Path]::GetFullPath($binding[1])
        if (-not (Test-Path -LiteralPath $resolvedBinding)) {
            throw "Search V2 recovery binding missing: $resolvedBinding"
        }
        $routeArgs += $binding[0]
        $routeArgs += $resolvedBinding
    }
}

$env:PYTHONPATH = (Join-Path $resolvedRepo 'src')
& $python @routeArgs
if ($LASTEXITCODE -ne 0) {
    throw "Search V2 runner failed with exit code $LASTEXITCODE"
}
