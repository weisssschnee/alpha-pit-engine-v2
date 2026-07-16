Set-StrictMode -Version Latest

function Write-CnAtomicJson {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Payload
    )
    $Parent = Split-Path -Parent $Path
    if ($Parent) { New-Item -ItemType Directory -Force -Path $Parent | Out-Null }
    $Temporary = $Path + ".tmp"
    $Payload | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $Temporary -Encoding UTF8
    Move-Item -LiteralPath $Temporary -Destination $Path -Force
}

function Get-CnProcessTreeRssSnapshot {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Roots
    )
    $Rows = @(Get-CimInstance Win32_Process | Select-Object ProcessId, ParentProcessId)
    $Children = @{}
    foreach ($Row in $Rows) {
        $Parent = [int]$Row.ParentProcessId
        if (-not $Children.ContainsKey($Parent)) { $Children[$Parent] = [System.Collections.Generic.List[int]]::new() }
        $Children[$Parent].Add([int]$Row.ProcessId)
    }

    $Groups = [ordered]@{}
    $TotalRss = [int64]0
    foreach ($Name in @($Roots.Keys | Sort-Object)) {
        $RootId = [int]$Roots[$Name]
        $Seen = [System.Collections.Generic.HashSet[int]]::new()
        $Queue = [System.Collections.Generic.Queue[int]]::new()
        $Queue.Enqueue($RootId)
        while ($Queue.Count -gt 0) {
            $Current = $Queue.Dequeue()
            if (-not $Seen.Add($Current)) { continue }
            if ($Children.ContainsKey($Current)) {
                foreach ($Child in $Children[$Current]) { $Queue.Enqueue([int]$Child) }
            }
        }

        $LiveIds = [System.Collections.Generic.List[int]]::new()
        $Rss = [int64]0
        foreach ($ProcessId in @($Seen | Sort-Object)) {
            $Process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
            if ($null -ne $Process) {
                $LiveIds.Add($ProcessId)
                $Rss += [int64]$Process.WorkingSet64
            }
        }
        $Groups[$Name] = [ordered]@{
            root_process_id = $RootId
            live_process_ids = @($LiveIds)
            rss_bytes = $Rss
        }
        $TotalRss += $Rss
    }
    return [pscustomobject]@{
        sampled_at = (Get-Date).ToUniversalTime().ToString("o")
        groups = $Groups
        total_rss_bytes = $TotalRss
    }
}

function Stop-CnProcessTrees {
    param(
        [Parameter(Mandatory = $true)][hashtable]$Roots
    )
    $Snapshot = Get-CnProcessTreeRssSnapshot -Roots $Roots
    foreach ($Name in @($Snapshot.groups.Keys | Sort-Object)) {
        $RootId = [int]$Snapshot.groups[$Name].root_process_id
        $Ids = @($Snapshot.groups[$Name].live_process_ids | Where-Object { [int]$_ -ne $RootId })
        foreach ($ProcessId in @($Ids | Sort-Object -Descending)) {
            Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
        }
        Stop-Process -Id $RootId -Force -ErrorAction SilentlyContinue
    }
}

function Add-CnRssTimelineSample {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Snapshot
    )
    $Active = $Snapshot.groups["active_bar"]
    $Session = $Snapshot.groups["stock_session"]
    $Line = @(
        $Snapshot.sampled_at,
        ($Active.live_process_ids -join "|"),
        [string]$Active.rss_bytes,
        ($Session.live_process_ids -join "|"),
        [string]$Session.rss_bytes,
        [string]$Snapshot.total_rss_bytes
    ) -join ","
    Add-Content -LiteralPath $Path -Value $Line -Encoding UTF8
}
