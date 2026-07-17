param(
    [Parameter(Mandatory = $true)]
    [string]$RepoSha,
    [Parameter(Mandatory = $true)]
    [string]$StageRoot,
    [string]$OutputRoot = "D:\ChengboRemote\runtime\cn_full_field_information_research_v1_20260717"
)

$ErrorActionPreference = "Stop"
$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
$Script = Join-Path $StageRoot "scripts\run_cn_full_field_information_research.py"
$True1MinManifest = "D:\ChengboRemote\data\cn_true1min_development_only_release_v1_20260712_77o\development_only_release_manifest.json"
$FundamentalManifest = Join-Path $StageRoot "inputs\pit_sidecar_manifest.json"
$SplitManifest = Join-Path $StageRoot "inputs\phase3ga_true1min_2024_2025_global_split_manifest.csv"
$Master = Join-Path $StageRoot "inputs\cn_field_master_registry_v1.json"
$Registry = Join-Path $StageRoot "inputs\unified_capability_registry.json"
$Qualification = Join-Path $StageRoot "inputs\fundamental_field_qualification_matrix.csv"
$ChipCensus = Join-Path $StageRoot "inputs\chip_census"
$BroadEventPack = Join-Path $StageRoot "inputs\DISCOVERY_ENTRY_PACK.json"

if (Test-Path -LiteralPath $OutputRoot) {
    throw "Refusing to overwrite existing final output: $OutputRoot"
}

$env:PYTHONPATH = Join-Path $StageRoot "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "12"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "12"

New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
$stdout = Join-Path $OutputRoot "runner.stdout.log"
$stderr = Join-Path $OutputRoot "runner.stderr.log"
$arguments = @(
    $Script,
    "--master", $Master,
    "--registry", $Registry,
    "--qualification", $Qualification,
    "--true1min-release-manifest", $True1MinManifest,
    "--fundamental-manifest", $FundamentalManifest,
    "--split-manifest", $SplitManifest,
    "--chip-census", $ChipCensus,
    "--broad-event-pack", $BroadEventPack,
    "--output", $OutputRoot,
    "--repo-sha", $RepoSha,
    "--row-groups-per-file", "3",
    "--true1min-sample-modulus", "256",
    "--true1min-field-batch-size", "16",
    "--fundamental-code-count", "384",
    "--fundamental-session-count", "72"
)

@{
    pid = $PID
    repo_sha = $RepoSha
    stage_root = $StageRoot
    output_root = $OutputRoot
    started_at = [DateTimeOffset]::UtcNow.ToString("o")
    thread_contract = @{
        NUMBA_NUM_THREADS = $env:NUMBA_NUM_THREADS
        ARROW_NUM_THREADS = $env:ARROW_NUM_THREADS
        OMP_NUM_THREADS = $env:OMP_NUM_THREADS
        MKL_NUM_THREADS = $env:MKL_NUM_THREADS
        OPENBLAS_NUM_THREADS = $env:OPENBLAS_NUM_THREADS
        NUMEXPR_MAX_THREADS = $env:NUMEXPR_MAX_THREADS
    }
} | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $OutputRoot "launcher_receipt.json") -Encoding UTF8

$exitCode = 1
try {
    $previousErrorActionPreference = $ErrorActionPreference
    # Windows PowerShell 5.1 wraps any native stderr line (including a Python
    # warning) as NativeCommandError when ErrorActionPreference is Stop.
    # Preserve stderr in the run log without terminating a healthy process.
    $ErrorActionPreference = "Continue"
    & $Python @arguments 1> $stdout 2> $stderr
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousErrorActionPreference
}
catch {
    $ErrorActionPreference = "Continue"
    $_ | Out-String | Add-Content -LiteralPath $stderr -Encoding UTF8
    $exitCode = 1
}
finally {
    @{
        pid = $PID
        repo_sha = $RepoSha
        output_root = $OutputRoot
        completed_at = [DateTimeOffset]::UtcNow.ToString("o")
        exit_code = $exitCode
    } | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $OutputRoot "completion_receipt.json") -Encoding UTF8
}
exit $exitCode
