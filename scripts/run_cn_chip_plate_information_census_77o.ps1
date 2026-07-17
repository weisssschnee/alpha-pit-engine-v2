param(
    [string]$RepoSha = "ca91741e842ec77c92d32e3962f2b5aa1fccd6b1",
    [string]$StageRoot = "D:\ChengboRemote\staging\cn_field_information_ca91741",
    [string]$OutputRoot = "D:\ChengboRemote\runtime\cn_chip_plate_information_census_20260717_r2"
)

$ErrorActionPreference = "Stop"
$Python = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
$Manifest = "D:\ChengboRemote\data\chip_pit_v1_20260713\chip_sidecar_manifest_v1.json"
$Script = Join-Path $StageRoot "scripts\run_cn_chip_plate_information_census.py"
$env:PYTHONPATH = Join-Path $StageRoot "src"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "8"
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
$stdout = Join-Path $OutputRoot "runner.stdout.log"
$stderr = Join-Path $OutputRoot "runner.stderr.log"
$arguments = @(
    $Script,
    "--chip-manifest", $Manifest,
    "--output", $OutputRoot,
    "--repo-sha", $RepoSha,
    "--sample-modulus", "16"
)
@{
    pid = $PID
    repo_sha = $RepoSha
    output_root = $OutputRoot
    started_at = [DateTimeOffset]::UtcNow.ToString("o")
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $OutputRoot "launcher_receipt.json") -Encoding UTF8
& $Python @arguments 1> $stdout 2> $stderr
$exitCode = $LASTEXITCODE
@{
    pid = $PID
    repo_sha = $RepoSha
    output_root = $OutputRoot
    completed_at = [DateTimeOffset]::UtcNow.ToString("o")
    exit_code = $exitCode
} | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $OutputRoot "completion_receipt.json") -Encoding UTF8
exit $exitCode
