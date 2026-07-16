param(
    [string]$OutputName = "process_tree_monitor_synthetic",
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
. (Join-Path $RepoRoot "scripts\cn_phase3cm_process_tree_monitor.ps1")
$Root = Join-Path $RepoRoot ("runtime\cn_phase3cm_streaming_repair_20260716\" + $OutputName)
if (Test-Path $Root) { throw "synthetic monitor output already exists: $Root" }
New-Item -ItemType Directory -Force -Path $Root | Out-Null
$ArgumentFile = Join-Path $Root "command.json"
$ExitReceipt = Join-Path $Root "exit_receipt.json"
$ResultPath = Join-Path $Root "result.json"
$Command = [ordered]@{
    schema_version = "cn_phase3cm_backend_command_v1"
    backend = "synthetic"
    arguments = @("-c", "import time; payload=bytearray(128*1024*1024); time.sleep(4)")
}
Write-CnAtomicJson -Path $ArgumentFile -Payload $Command
$Wrapper = Join-Path $RepoRoot "scripts\invoke_cn_phase3cm_backend_with_exit_receipt.ps1"
$Process = Start-Process -FilePath "powershell.exe" -ArgumentList @(
    "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", $Wrapper,
    "-PythonExe", $PythonExe, "-RepoRoot", $RepoRoot,
    "-ArgumentFile", $ArgumentFile, "-ExitReceipt", $ExitReceipt
) -WindowStyle Hidden -PassThru
$Peak = [int64]0
$ObservedDescendant = $false
while (-not $Process.HasExited) {
    $Snapshot = Get-CnProcessTreeRssSnapshot -Roots @{ active_bar = $Process.Id }
    $Group = $Snapshot.groups["active_bar"]
    if (@($Group.live_process_ids).Count -gt 1) { $ObservedDescendant = $true }
    if ([int64]$Group.rss_bytes -gt $Peak) { $Peak = [int64]$Group.rss_bytes }
    Start-Sleep -Milliseconds 200
    $Process.Refresh()
}
$Process.WaitForExit()
$Exit = Get-Content -LiteralPath $ExitReceipt -Raw | ConvertFrom-Json
$Pass = (
    [int]$Exit.exit_code -eq 0 -and
    $Exit.status -eq "CN_PHASE3CM_BACKEND_PROCESS_COMPLETED" -and
    $ObservedDescendant -and
    $Peak -ge 100MB
)
$Result = [ordered]@{
    schema_version = "cn_phase3cm_process_tree_monitor_synthetic_v1"
    status = if ($Pass) { "CN_PHASE3CM_PROCESS_TREE_MONITOR_PASS" } else { "CN_PHASE3CM_PROCESS_TREE_MONITOR_FAIL" }
    observed_descendant = $ObservedDescendant
    peak_process_tree_rss_bytes = $Peak
    exit_receipt = $ExitReceipt
    exit_code = [int]$Exit.exit_code
}
Write-CnAtomicJson -Path $ResultPath -Payload $Result
$Result | ConvertTo-Json -Compress
if (-not $Pass) { throw "process-tree monitor synthetic gate failed" }
