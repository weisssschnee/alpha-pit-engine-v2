param(
    [string]$RepoRoot = "D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git",
    [string]$PythonExe = "D:\ChengboRemote\venvs\alpha311\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
$env:PYTHONPATH = Join-Path $RepoRoot "src"
$env:NUMBA_NUM_THREADS = "3"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "1"
$env:POLARS_MAX_THREADS = "1"

$OutputRoot = Join-Path $RepoRoot "runtime\cn_phase3cm_streaming_repair_20260716\test_evidence"
New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
$FullXml = Join-Path $OutputRoot "CN_FULL_PYTEST.xml"
$TargetXml = Join-Path $OutputRoot "CN_PHASE3CM_TARGETED_PYTEST.xml"
$FullOut = Join-Path $OutputRoot "CN_FULL_PYTEST.stdout.log"
$FullErr = Join-Path $OutputRoot "CN_FULL_PYTEST.stderr.log"
$TargetOut = Join-Path $OutputRoot "CN_PHASE3CM_TARGETED_PYTEST.stdout.log"
$TargetErr = Join-Path $OutputRoot "CN_PHASE3CM_TARGETED_PYTEST.stderr.log"

Push-Location $RepoRoot
try {
    $Full = Start-Process -FilePath $PythonExe -ArgumentList @(
        "-m", "pytest", "-q", "--junitxml", $FullXml
    ) -WorkingDirectory $RepoRoot -RedirectStandardOutput $FullOut -RedirectStandardError $FullErr `
        -WindowStyle Hidden -PassThru -Wait
    $TargetFiles = @(
        "tests\test_phase3cm_streaming_block_reader.py",
        "tests\test_phase3cm_qualification_orchestrators.py",
        "tests\test_phase3cm_streaming_cache.py",
        "tests\test_phase3cm_streaming_checkpoint.py",
        "tests\test_phase3cm_streaming_dag.py",
        "tests\test_phase3cm_streaming_expression.py",
        "tests\test_phase3cm_streaming_finalizer.py",
        "tests\test_phase3cm_streaming_portfolio.py",
        "tests\test_phase3cm_streaming_reducer.py",
        "tests\test_phase3cm_streaming_resource_contract.py",
        "tests\test_phase3cm_streaming_support.py",
        "tests\test_phase3cm_streaming_telemetry.py",
        "tests\test_phase3cm_time_major_sidecar.py"
    )
    $TargetArguments = @("-m", "pytest", "-q", "--junitxml", $TargetXml)
    $TargetArguments += $TargetFiles
    $Target = Start-Process -FilePath $PythonExe -ArgumentList $TargetArguments `
        -WorkingDirectory $RepoRoot -RedirectStandardOutput $TargetOut -RedirectStandardError $TargetErr `
        -WindowStyle Hidden -PassThru -Wait
}
finally {
    Pop-Location
}

function Read-TestCounts {
    param([string]$Path)
    [xml]$Document = Get-Content -LiteralPath $Path -Raw
    $Suites = @()
    if ($null -ne $Document.testsuites) {
        $Suites = @($Document.testsuites.testsuite)
    }
    elseif ($null -ne $Document.testsuite) {
        $Suites = @($Document.testsuite)
    }
    if ($Suites.Count -eq 0) {
        throw "JUnit report does not contain a testsuite: $Path"
    }
    $Tests = [int](($Suites | Measure-Object -Property tests -Sum).Sum)
    $Failures = [int](($Suites | Measure-Object -Property failures -Sum).Sum)
    $Errors = [int](($Suites | Measure-Object -Property errors -Sum).Sum)
    $Skipped = [int](($Suites | Measure-Object -Property skipped -Sum).Sum)
    return [ordered]@{
        tests = $Tests
        passed = $Tests - $Failures - $Errors - $Skipped
        failed = $Failures + $Errors
        skipped = $Skipped
    }
}

$FullCounts = Read-TestCounts $FullXml
$TargetCounts = Read-TestCounts $TargetXml
$Pass = $Full.ExitCode -eq 0 -and $Target.ExitCode -eq 0 -and $FullCounts.failed -eq 0 -and $TargetCounts.failed -eq 0
$Summary = [ordered]@{
    schema_version = "cn_phase3cm_pytest_summary_v1"
    status = if ($Pass) { "TEST_SUITE_PASS" } else { "TEST_SUITE_FAIL" }
    tests = $FullCounts.tests
    passed = $FullCounts.passed
    failed = $FullCounts.failed
    skipped = $FullCounts.skipped
    targeted_tests = $TargetCounts.tests
    targeted_passed = $TargetCounts.passed
    targeted_failed = $TargetCounts.failed
    full_exit_code = $Full.ExitCode
    targeted_exit_code = $Target.ExitCode
    full_junit = $FullXml
    targeted_junit = $TargetXml
    data_role = "synthetic_and_test_fixtures_only"
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
}
$SummaryPath = Join-Path $OutputRoot "CN_PHASE3CM_PYTEST_SUMMARY.json"
$Temporary = $SummaryPath + ".tmp"
$Summary | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $Temporary -Encoding UTF8
Move-Item -LiteralPath $Temporary -Destination $SummaryPath -Force
$Summary | ConvertTo-Json -Compress
if (-not $Pass) { throw "Phase3CM test suite failed" }
