param(
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path $RepoRoot "src"
$RuntimeRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716"

Push-Location $RepoRoot
try {
    & $PythonExe scripts\audit_cn_phase3cm_resume_parity.py `
        --uninterrupted-root (Join-Path $RuntimeRoot "phase_c_active1_v3_t3_b10") `
        --resumed-root (Join-Path $RuntimeRoot "phase_d_resume_active1_v3_t3_b10") `
        --output (Join-Path $RuntimeRoot "resume_parity\CN_ACTIVE_RESUME_PARITY.json")
    if ($LASTEXITCODE -ne 0) { throw "resume parity failed: $LASTEXITCODE" }
}
finally {
    Pop-Location
}
