$ErrorActionPreference = "Stop"

& "D:\HermesWorker\workspace\alpha_pit_true1min_engine_20260619\scripts\phase3cf_guarded_smoke_launcher_20260618.ps1" -CompanyMode
if ($LASTEXITCODE -ne 0) {
    throw "Phase3CF guarded company smoke failed: $LASTEXITCODE"
}
