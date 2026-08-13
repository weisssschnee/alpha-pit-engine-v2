param(
    [Parameter(Mandatory = $true)]
    [string]$Repo,
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[0-9a-f]{40}$')]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [ValidateSet('LAUNCH_HIGH_COST_CAMPAIGN', 'RETRY')]
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
    [string]$BarSourceRoot,
    [Parameter(Mandatory = $true)]
    [string]$NodeResourceCapacity,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [string]$CampaignAuthorization = (
        'runtime\run_plans\cn_program_optimizer_tournament_v1.json'
    ),
    [string]$InformationMetrics = (
        'runtime\cn_full_field_information_research_v1_20260717\capability_information_metrics.json'
    ),
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
$resolvedInformationMetrics = if ([IO.Path]::IsPathRooted($InformationMetrics)) {
    [IO.Path]::GetFullPath($InformationMetrics)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $InformationMetrics))
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
    'D:\ChengboRemote\runtime\cn_program_optimizer_tournament_',
    [StringComparison]::OrdinalIgnoreCase
)) {
    throw "unexpected Program tournament output root: $resolvedOutput"
}
if (Test-Path -LiteralPath $resolvedOutput) {
    throw "Program tournament launch/retry output root must be absent: $resolvedOutput"
}

$head = (& git -C $resolvedRepo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0 -or $head -ne $RepoSha) {
    throw "Program tournament checkout SHA drift: expected=$RepoSha actual=$head"
}
$dirty = @(& git -C $resolvedRepo status --porcelain --untracked-files=normal)
if ($LASTEXITCODE -ne 0 -or $dirty.Count -ne 0) {
    throw "Program tournament executing checkout must be clean"
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
    $resolvedInformationMetrics,
    [IO.Path]::GetFullPath($BarSourceRoot),
    [IO.Path]::GetFullPath($NodeResourceCapacity),
    (Join-Path $resolvedRepo 'app.py')
)
foreach ($path in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required Program tournament input missing: $path"
    }
}

$env:PYTHONPATH = (Join-Path $resolvedRepo 'src')
Push-Location $resolvedRepo
try {
    & $python -c @'
import app
import our_system_phase2.runtime.cn_program_optimizer_tournament_v1
import scripts.run_cn_program_optimizer_tournament_v1
'@
    $importExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
if ($importExitCode -ne 0) {
    throw "Program tournament execution-surface import failed with exit code $importExitCode"
}

$routeArgs = @(
    (Join-Path $resolvedRepo 'app.py'),
    'cn-program-optimizer-tournament-v1',
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
    '--information-metrics', $resolvedInformationMetrics,
    '--bar-source-root', [IO.Path]::GetFullPath($BarSourceRoot),
    '--node-resource-capacity', [IO.Path]::GetFullPath($NodeResourceCapacity),
    '--output-root', $resolvedOutput,
    '--executor-workers', $ExecutorWorkers
)

& $python @routeArgs
if ($LASTEXITCODE -ne 0) {
    throw "Program tournament runner failed with exit code $LASTEXITCODE"
}
