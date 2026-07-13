param(
    [Parameter(Mandatory = $true)][string]$RepoRoot,
    [Parameter(Mandatory = $true)][string]$DataRoot,
    [Parameter(Mandatory = $true)][string]$ChipRoot,
    [Parameter(Mandatory = $true)][string]$OutputRoot,
    [Parameter(Mandatory = $true)][string]$RepoSha
)

$ErrorActionPreference = "Stop"
$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
Set-Location $RepoRoot
$env:PYTHONPATH = "src;."
$env:NUMEXPR_MAX_THREADS = "12"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"

& $Python -m our_system_phase2.runtime.cn_broad_event_canary `
    --contract (Join-Path $RepoRoot "runtime\run_plans\cn_broad_event_canary_v1.json") `
    --semantic-registry (Join-Path $RepoRoot "runtime\event_registry\cn_broad_event_semantic_registry_v1.json") `
    --data-root $DataRoot `
    --chip-root $ChipRoot `
    --output-root $OutputRoot `
    --repo-sha $RepoSha
if ($LASTEXITCODE -ne 0) {
    throw "Broad Event CANARY failed with exit code $LASTEXITCODE"
}
