param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("active_bar", "stock_session")]
    [string]$Backend,
    [Parameter(Mandatory = $true)]
    [ValidateSet("C", "D", "E")]
    [string]$Phase,
    [Parameter(Mandatory = $true)]
    [int]$PairCount,
    [Parameter(Mandatory = $true)]
    [string]$OutputName,
    [string]$ExecutionPlan = "",
    [int]$ComputeThreads = 16,
    [int]$BlockSessions = 5,
    [int]$PairBatchSize = 4,
    [int]$StopAfterBlocks = 0,
    [switch]$Resume,
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git"
)

$RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716"
$LogRoot = Join-Path $RuntimeRoot $OutputName
New-Item -ItemType Directory -Force -Path $LogRoot | Out-Null
$arguments = @(
    "-NoProfile", "-ExecutionPolicy", "Bypass",
    "-File", (Join-Path $RepoRoot "scripts\run_cn_phase3cm_streaming_backend_77o.ps1"),
    "-Backend", $Backend,
    "-Phase", $Phase,
    "-PairCount", [string]$PairCount,
    "-OutputName", $OutputName,
    "-ComputeThreads", [string]$ComputeThreads,
    "-BlockSessions", [string]$BlockSessions,
    "-PairBatchSize", [string]$PairBatchSize
)
if ($StopAfterBlocks -gt 0) { $arguments += @("-StopAfterBlocks", [string]$StopAfterBlocks) }
if ($ExecutionPlan) { $arguments += @("-ExecutionPlan", $ExecutionPlan) }
if ($Resume) { $arguments += "-Resume" }
$process = Start-Process powershell.exe `
    -WindowStyle Hidden `
    -PassThru `
    -ArgumentList $arguments `
    -RedirectStandardOutput (Join-Path $LogRoot "launcher.stdout.log") `
    -RedirectStandardError (Join-Path $LogRoot "launcher.stderr.log")
Set-Content -LiteralPath (Join-Path $LogRoot "launcher.pid") -Value ([string]$process.Id)
Write-Output $process.Id
