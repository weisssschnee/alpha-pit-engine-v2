param(
    [Parameter(Mandatory = $true)][string]$Workspace,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][string]$SourceRepoSha
)

$ErrorActionPreference = "Stop"
$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "77o Python environment is missing: $Python"
}

$env:PYTHONPATH = Join-Path $Workspace "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"

New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
Set-Location $Workspace
& $Python scripts\run_cn_core_pack_generator_capacity_review.py `
    --output $OutputRoot `
    --contract-output (Join-Path $OutputRoot "cn_core_pack_development_discovery_v1.json") `
    --attempts-per-route 32768 `
    --broad-event-attempts 4096 `
    --seeds "20260718,20260719" `
    --checkpoints "2048,8192,32768" `
    --expected-hostname "DESKTOP-77OPJ6F" `
    --source-repo-sha $SourceRepoSha
if ($LASTEXITCODE -ne 0) {
    throw "Core Pack generator capacity review failed: $LASTEXITCODE"
}
