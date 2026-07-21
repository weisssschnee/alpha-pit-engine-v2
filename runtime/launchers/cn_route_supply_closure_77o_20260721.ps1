$ErrorActionPreference = 'Stop'

$workspace = 'D:\ChengboRemote\workspace\alpha_pit_true1min_route_supply_8e6e8cce06a4'
$python = 'D:\ChengboRemote\venvs\alpha311\Scripts\python.exe'
$output = 'D:\ChengboRemote\runtime\cn_route_supply_closure_20260721_0c18916'
$stdout = 'D:\ChengboRemote\runtime\jobs\cn_route_supply_closure_20260721_0c18916.stdout.log'
$stderr = 'D:\ChengboRemote\runtime\jobs\cn_route_supply_closure_20260721_0c18916.stderr.log'
$status = 'D:\ChengboRemote\runtime\jobs\cn_route_supply_closure_20260721_0c18916.status.json'

$env:PYTHONPATH = Join-Path $workspace 'src'
$env:PYTHONUNBUFFERED = '1'
$env:ARROW_NUM_THREADS = '1'

$arguments = @(
    (Join-Path $workspace 'app.py'),
    'cn-route-supply-closure',
    '--',
    '--registry', (Join-Path $workspace 'runtime\field_registry\cn_unified_capability_registry_v3_20260717\unified_capability_registry.json'),
    '--prior-run-root', 'D:\ChengboRemote\runtime\cn_iterative_search_v1_20260721_final_8c09bf2',
    '--broad-event-entry-pack', (Join-Path $workspace 'reports\cn_broad_event_recovery_20260713\DISCOVERY_ENTRY_PACK.json'),
    '--output-root', $output,
    '--repo-sha', '0c18916eceda45e33676c53f7efeccc010c131ca',
    '--seeds', '1729,2718',
    '--attempt-caps', '64,256,1024',
    '--required-pairs', '12',
    '--split-manifest', 'D:\ChengboRemote\workspace\cn_phase3cm_1024_sidecar_closure_0aba8c5\runtime\run_plans\phase3ga_true1min_2024_2025_global_split_manifest.csv',
    '--sidecar-closure', 'D:\ChengboRemote\runtime\cn_core_pack_aggressive_discovery_20260718_595c5fc\strict_wave_01024_sidecars_3509d0c\CN_PHASE3CM_1024_SIDECAR_CLOSURE.json',
    '--active-field-root', 'D:\ChengboRemote\runtime\cn_core_pack_aggressive_discovery_20260718_595c5fc\strict_wave_01024_sidecars_3509d0c\active_time_major_train_v1',
    '--active-label-root', 'D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git\runtime\cn_phase3cm_streaming_repair_20260716\time_major_train_v3_labels',
    '--session-field-root', 'D:\ChengboRemote\runtime\cn_core_pack_aggressive_discovery_20260718_595c5fc\strict_wave_01024_sidecars_3509d0c\session_time_major_train_v1',
    '--session-label-root', 'D:\ChengboRemote\workspace\alpha_pit_compositional_667c82f_git\runtime\cn_phase3cm_streaming_repair_20260716\session_time_major_train_v3_labels',
    '--active-threads', '11',
    '--session-threads', '2'
)

$started = Get-Date
$exitCode = 1
try {
    & $python @arguments 1> $stdout 2> $stderr
    $exitCode = $LASTEXITCODE
}
catch {
    $_ | Out-String | Set-Content -LiteralPath $stderr -Encoding UTF8
    $exitCode = 1
}
$ended = Get-Date

[ordered]@{
    task_name = 'CNRouteSupplyClosure_20260721_0c18916'
    started_at = $started.ToString('o')
    ended_at = $ended.ToString('o')
    exit_code = $exitCode
    output_root = $output
    stdout = $stdout
    stderr = $stderr
} | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $status -Encoding UTF8

exit $exitCode
