param(
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git"
)

$RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716"
New-Item -ItemType Directory -Force -Path $RuntimeRoot | Out-Null
$process = Start-Process powershell.exe `
    -WindowStyle Hidden `
    -PassThru `
    -ArgumentList @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", (Join-Path $RepoRoot "scripts\run_cn_phase3cm_sidecars_77o.ps1")
    ) `
    -RedirectStandardOutput (Join-Path $RuntimeRoot "sidecar_build.stdout.log") `
    -RedirectStandardError (Join-Path $RuntimeRoot "sidecar_build.stderr.log")
Set-Content -LiteralPath (Join-Path $RuntimeRoot "sidecar_build.pid") -Value ([string]$process.Id)
Write-Output $process.Id
