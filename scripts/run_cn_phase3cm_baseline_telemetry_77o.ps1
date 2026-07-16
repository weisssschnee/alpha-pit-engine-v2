param(
    [Parameter(Mandatory = $true)][string]$Repo,
    [Parameter(Mandatory = $true)][string]$Python,
    [Parameter(Mandatory = $true)][string]$DataRoot,
    [int]$DurationSeconds = 900,
    [int]$SampleSeconds = 15
)

$ErrorActionPreference = "Stop"
if ($DurationSeconds -lt 60 -or $DurationSeconds -gt 900) {
    throw "DurationSeconds must be between 60 and 900"
}
if ($SampleSeconds -lt 10 -or $SampleSeconds -gt 30) {
    throw "SampleSeconds must be between 10 and 30"
}

$env:PYTHONPATH = Join-Path $Repo "src"
$env:NUMBA_NUM_THREADS = "1"
$env:ARROW_NUM_THREADS = "1"
$env:OMP_NUM_THREADS = "1"
$env:MKL_NUM_THREADS = "1"
$env:OPENBLAS_NUM_THREADS = "1"
$env:NUMEXPR_MAX_THREADS = "16"

$runtime = Join-Path $Repo "runtime\cn_phase3cm_streaming_repair_20260716"
$legacyOutput = Join-Path $runtime "phase0_legacy_baseline"
$legacyReport = Join-Path $runtime "phase0_legacy_baseline_report"
New-Item -ItemType Directory -Force -Path $runtime, $legacyOutput, $legacyReport | Out-Null

$arguments = @(
    "-m", "our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit",
    "--candidate-audit", (Join-Path $Repo "runtime\cn_compositional_nline_large_search_20260715\preflight_active_candidates.csv"),
    "--shard-root", $DataRoot,
    "--output-root", $legacyOutput,
    "--report-root", $legacyReport,
    "--candidate-limit", "1",
    "--max-shards", "16",
    "--sample-trade-times-per-shard", "0",
    "--no-event-aware-sample-times",
    "--horizons", "1,5,15,30",
    "--split-manifest", (Join-Path $Repo "runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv"),
    "--candidate-receipt-table", (Join-Path $Repo "runtime\cn_compositional_nline_large_search_20260715\preflight_active_candidate_receipts.jsonl"),
    "--candidate-pair-receipt-table", (Join-Path $Repo "runtime\cn_compositional_nline_large_search_20260715\preflight_active_pair_receipts.jsonl"),
    "--unified-registry", (Join-Path $Repo "reports\cn_unified_capability_discovery_20260714\completed_f8169e1\registry\unified_capability_registry.json"),
    "--data-release-hash", "cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827",
    "--min-obs-per-time", "20",
    "--cost-bps", "5",
    "--top-quantile", "0.2",
    "--portfolio-mode", "long_only_top",
    "--fast-mode",
    "--numexpr-threads", "16",
    "--disable-incremental-checkpoints",
    "--persistent-cache-mode", "off",
    "--enforce-pair-shared-support"
)

$stdout = Join-Path $runtime "phase0_legacy_baseline.stdout.log"
$stderr = Join-Path $runtime "phase0_legacy_baseline.stderr.log"
$startedAt = [DateTime]::UtcNow
$process = Start-Process -FilePath $Python -ArgumentList $arguments -WorkingDirectory $Repo -PassThru -WindowStyle Hidden -RedirectStandardOutput $stdout -RedirectStandardError $stderr
$samples = [System.Collections.Generic.List[object]]::new()
$deadline = $startedAt.AddSeconds($DurationSeconds)
$timedOut = $false

while (-not $process.HasExited) {
    $current = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if ($null -eq $current) { break }
    $allProcesses = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue)
    $tracked = [System.Collections.Generic.HashSet[int]]::new()
    [void]$tracked.Add($process.Id)
    do {
        $before = $tracked.Count
        foreach ($item in $allProcesses) {
            if ($tracked.Contains([int]$item.ParentProcessId)) { [void]$tracked.Add([int]$item.ProcessId) }
        }
    } while ($tracked.Count -gt $before)
    $tree = @($allProcesses | Where-Object { $tracked.Contains([int]$_.ProcessId) })
    $cpuSeconds = ($tree | ForEach-Object { ([double]$_.KernelModeTime + [double]$_.UserModeTime) / 10000000.0 } | Measure-Object -Sum).Sum
    $workingSet = ($tree | Measure-Object -Property WorkingSetSize -Sum).Sum
    $privateBytes = ($tree | ForEach-Object { [int64]$_.PageFileUsage * 1024 } | Measure-Object -Sum).Sum
    $virtualBytes = ($tree | Measure-Object -Property VirtualSize -Sum).Sum
    $readBytes = ($tree | Measure-Object -Property ReadTransferCount -Sum).Sum
    $writeBytes = ($tree | Measure-Object -Property WriteTransferCount -Sum).Sum
    $samples.Add([pscustomobject]@{
        sampled_at_utc = [DateTime]::UtcNow.ToString("o")
        elapsed_seconds = ([DateTime]::UtcNow - $startedAt).TotalSeconds
        process_count = $tree.Count
        cpu_seconds = [double]$cpuSeconds
        working_set_bytes = [int64]$workingSet
        private_bytes = [int64]$privateBytes
        virtual_bytes = [int64]$virtualBytes
        read_bytes = [int64]$readBytes
        write_bytes = [int64]$writeBytes
    })
    if ([DateTime]::UtcNow -ge $deadline) {
        $timedOut = $true
        & taskkill.exe /PID $process.Id /T /F | Out-Null
        break
    }
    Start-Sleep -Seconds $SampleSeconds
    $process.Refresh()
}
$process.WaitForExit()
$endedAt = [DateTime]::UtcNow
$samples | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $runtime "CN_PHASE3CM_RESOURCE_TIMELINE.csv")

$peakRss = if ($samples.Count) { ($samples | Measure-Object working_set_bytes -Maximum).Maximum } else { 0 }
$last = if ($samples.Count) { $samples[$samples.Count - 1] } else { $null }
$summary = [ordered]@{
    schema_version = "cn_phase3cm_legacy_baseline_telemetry_v1"
    status = if ($timedOut) { "CONTROLLED_STOP_AT_PHASE0_GATE" } elseif ($process.ExitCode -eq 0) { "COMPLETED" } else { "FAILED" }
    purpose = "BOTTLENECK_ATTRIBUTION_ONLY"
    evaluator = "legacy_reference"
    started_at_utc = $startedAt.ToString("o")
    ended_at_utc = $endedAt.ToString("o")
    wall_seconds = ($endedAt - $startedAt).TotalSeconds
    cpu_seconds = if ($null -ne $last) { $last.cpu_seconds } else { 0 }
    effective_cores = if ($null -ne $last -and ($endedAt - $startedAt).TotalSeconds -gt 0) { $last.cpu_seconds / ($endedAt - $startedAt).TotalSeconds } else { 0 }
    peak_working_set_bytes = $peakRss
    read_bytes = if ($null -ne $last) { $last.read_bytes } else { 0 }
    write_bytes = if ($null -ne $last) { $last.write_bytes } else { 0 }
    exit_code = $process.ExitCode
    controlled_timeout = $timedOut
    duration_gate_seconds = $DurationSeconds
    sample_seconds = $SampleSeconds
    sample_count = $samples.Count
    heavy_processes = 1
    allocated_compute_threads = 16
    thread_environment = [ordered]@{
        NUMBA_NUM_THREADS = $env:NUMBA_NUM_THREADS
        ARROW_NUM_THREADS = $env:ARROW_NUM_THREADS
        OMP_NUM_THREADS = $env:OMP_NUM_THREADS
        MKL_NUM_THREADS = $env:MKL_NUM_THREADS
        OPENBLAS_NUM_THREADS = $env:OPENBLAS_NUM_THREADS
        NUMEXPR_MAX_THREADS = $env:NUMEXPR_MAX_THREADS
    }
    validation_reads = 0
    holdout_reads = 0
    forward_2026_reads = 0
    strict_stage_a = "NOT_AUTHORIZED"
    command = ($Python + " " + ($arguments -join " "))
    stdout = $stdout
    stderr = $stderr
}
$summary | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 (Join-Path $runtime "CN_PHASE3CM_LEGACY_BASELINE_TELEMETRY.json")
$summary | ConvertTo-Json -Depth 8 -Compress
