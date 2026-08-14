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
    [string]$ExecutionContract,
    [Parameter(Mandatory = $true)]
    [string]$TrainPriceRoot,
    [Parameter(Mandatory = $true)]
    [string]$OutputRoot,
    [string]$CampaignAuthorization = (
        'runtime\run_plans\cn_program_optimizer_tournament_v1.json'
    ),
    [string]$SourceBinding = (
        'runtime\run_plans\cn_program_optimizer_tournament_source_binding_v1.json'
    ),
    [int]$ExecutorWorkers = 10,
    [ValidateSet(4, 6, 8)]
    [int]$CheckpointWorkerCap = 8,
    [string]$CheckpointRecoveryFromRepoSha = '',
    [string]$CheckpointRecoveryIncident = '',
    [string]$CheckpointRecoveryDiagnosticAudit = '',
    [string]$CheckpointRecoveryDeploymentManifest = ''
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
$resolvedSourceBinding = if ([IO.Path]::IsPathRooted($SourceBinding)) {
    [IO.Path]::GetFullPath($SourceBinding)
} else {
    [IO.Path]::GetFullPath((Join-Path $resolvedRepo $SourceBinding))
}
$terminalRoot = Join-Path (
    'D:\ChengboRemote\runtime\cn_program_optimizer_tournament_terminal_logs'
) ("{0}_{1}" -f $TargetRunId, $ProjectControlAdmissionSha256.Substring(0, 12))

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
if ($RequestedAction -eq 'RECOVERY') {
    if (-not (Test-Path -LiteralPath $resolvedOutput -PathType Container)) {
        throw "Program tournament recovery output root must exist: $resolvedOutput"
    }
} elseif (Test-Path -LiteralPath $resolvedOutput) {
    throw "Program tournament launch/retry output root must be absent: $resolvedOutput"
}

$resolvedRecoveryIncident = $null
$resolvedRecoveryDiagnostic = $null
$resolvedRecoveryDeployment = $null
if ($RequestedAction -eq 'RECOVERY') {
    if ($CheckpointRecoveryFromRepoSha -notmatch '^[0-9a-f]{40}$') {
        throw 'Program tournament recovery requires a full checkpoint-builder SHA'
    }
    if (
        [string]::IsNullOrWhiteSpace($CheckpointRecoveryIncident) -or
        [string]::IsNullOrWhiteSpace($CheckpointRecoveryDiagnosticAudit) -or
        [string]::IsNullOrWhiteSpace($CheckpointRecoveryDeploymentManifest)
    ) {
        throw 'Program tournament recovery binding is incomplete'
    }
    $resolvedRecoveryIncident = [IO.Path]::GetFullPath($CheckpointRecoveryIncident)
    $resolvedRecoveryDiagnostic = [IO.Path]::GetFullPath(
        $CheckpointRecoveryDiagnosticAudit
    )
    $resolvedRecoveryDeployment = [IO.Path]::GetFullPath(
        $CheckpointRecoveryDeploymentManifest
    )
} elseif (
    -not [string]::IsNullOrWhiteSpace($CheckpointRecoveryFromRepoSha) -or
    -not [string]::IsNullOrWhiteSpace($CheckpointRecoveryIncident) -or
    -not [string]::IsNullOrWhiteSpace($CheckpointRecoveryDiagnosticAudit) -or
    -not [string]::IsNullOrWhiteSpace($CheckpointRecoveryDeploymentManifest)
) {
    throw 'checkpoint recovery binding is valid only for RECOVERY'
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
    $resolvedSourceBinding,
    [IO.Path]::GetFullPath($ExecutionContract),
    [IO.Path]::GetFullPath($TrainPriceRoot),
    (Join-Path $resolvedRepo 'app.py'),
    (Join-Path $resolvedRepo (
        'scripts\check_cn_program_optimizer_execution_node_cleanliness_v1.py'
    ))
)
if ($RequestedAction -eq 'RECOVERY') {
    $requiredPaths += @(
        $resolvedRecoveryIncident,
        $resolvedRecoveryDiagnostic,
        $resolvedRecoveryDeployment
    )
}
foreach ($path in $requiredPaths) {
    if (-not (Test-Path -LiteralPath $path)) {
        throw "required Program tournament input missing: $path"
    }
}

$env:PYTHONPATH = (Join-Path $resolvedRepo 'src')
& $python (
    Join-Path $resolvedRepo (
        'scripts\check_cn_program_optimizer_tournament_source_binding_v1.py'
    )
) --source-binding $resolvedSourceBinding
if ($LASTEXITCODE -ne 0) {
    throw "Program tournament exact source authority preflight failed with exit code $LASTEXITCODE"
}

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

New-Item -ItemType Directory -Path $terminalRoot -Force | Out-Null
$cleanlinessPath = Join-Path $terminalRoot 'execution_node_cleanliness.json'
& $python (
    Join-Path $resolvedRepo (
        'scripts\check_cn_program_optimizer_execution_node_cleanliness_v1.py'
    )
) 1> $cleanlinessPath
$cleanlinessExitCode = $LASTEXITCODE
if (Test-Path -LiteralPath $cleanlinessPath) {
    [Console]::Out.Write((Get-Content -LiteralPath $cleanlinessPath -Raw))
}
if ($cleanlinessExitCode -ne 0) {
    throw "Program tournament execution-node cleanliness failed with exit code $cleanlinessExitCode"
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
    '--source-binding', $resolvedSourceBinding,
    '--execution-contract', [IO.Path]::GetFullPath($ExecutionContract),
    '--train-price-root', [IO.Path]::GetFullPath($TrainPriceRoot),
    '--output-root', $resolvedOutput,
    '--executor-workers', $ExecutorWorkers,
    '--checkpoint-worker-cap', $CheckpointWorkerCap
)
if ($RequestedAction -eq 'RECOVERY') {
    $routeArgs += @(
        '--checkpoint-recovery-from-repo-sha', $CheckpointRecoveryFromRepoSha,
        '--checkpoint-recovery-incident', $resolvedRecoveryIncident,
        '--checkpoint-recovery-diagnostic-audit', $resolvedRecoveryDiagnostic,
        '--checkpoint-recovery-deployment-manifest', $resolvedRecoveryDeployment
    )
}

$stdoutPath = Join-Path $terminalRoot 'runner.stdout.log'
$stderrPath = Join-Path $terminalRoot 'runner.stderr.log'
$receiptPath = Join-Path $terminalRoot 'terminal_receipt.json'
$ErrorActionPreference = 'Continue'
& $python @routeArgs 1> $stdoutPath 2> $stderrPath
$runnerExitCode = $LASTEXITCODE
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $stdoutPath) {
    [Console]::Out.Write((Get-Content -LiteralPath $stdoutPath -Raw))
}
if (Test-Path -LiteralPath $stderrPath) {
    [Console]::Error.Write((Get-Content -LiteralPath $stderrPath -Raw))
}
[ordered]@{
    schema_version = 'cn_program_optimizer_tournament_terminal_receipt_v1'
    target_run_id = $TargetRunId
    stdout_path = $stdoutPath
    stderr_path = $stderrPath
    exit_code = $runnerExitCode
    complete_python_traceback_preserved_in_stderr = $true
} | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $receiptPath -Encoding UTF8
if ($runnerExitCode -ne 0) {
    throw "Program tournament runner failed with exit code $runnerExitCode; complete stdout/stderr retained under $terminalRoot"
}
