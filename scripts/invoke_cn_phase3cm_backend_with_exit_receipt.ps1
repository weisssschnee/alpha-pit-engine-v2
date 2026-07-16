param(
    [Parameter(Mandatory = $true)][string]$PythonExe,
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][string]$ArgumentFile,
    [Parameter(Mandatory = $true)][string]$ExitReceipt
)

$ErrorActionPreference = "Stop"
$StartedAt = (Get-Date).ToUniversalTime()
$ExitCode = 1
$Status = "CN_PHASE3CM_BACKEND_WRAPPER_FAILED"
$Failure = $null
try {
    $Command = Get-Content -LiteralPath $ArgumentFile -Raw | ConvertFrom-Json
    $Arguments = @($Command.arguments | ForEach-Object { [string]$_ })
    Push-Location $RepoRoot
    try {
        & $PythonExe @Arguments
        $ExitCode = [int]$LASTEXITCODE
    }
    finally {
        Pop-Location
    }
    $Status = if ($ExitCode -eq 0) {
        "CN_PHASE3CM_BACKEND_PROCESS_COMPLETED"
    }
    else {
        "CN_PHASE3CM_BACKEND_PROCESS_FAILED"
    }
}
catch {
    $Failure = $_.Exception.Message
    Write-Error $_
}
finally {
    $Receipt = [ordered]@{
        schema_version = "cn_phase3cm_backend_exit_receipt_v1"
        status = $Status
        exit_code = $ExitCode
        wrapper_process_id = $PID
        python_executable = $PythonExe
        argument_file = $ArgumentFile
        argument_file_sha256 = (Get-FileHash -Algorithm SHA256 -LiteralPath $ArgumentFile).Hash.ToLowerInvariant()
        started_at = $StartedAt.ToString("o")
        finished_at = (Get-Date).ToUniversalTime().ToString("o")
        failure = $Failure
    }
    $Temporary = $ExitReceipt + ".tmp"
    $Receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $Temporary -Encoding UTF8
    Move-Item -LiteralPath $Temporary -Destination $ExitReceipt -Force
}
exit $ExitCode
