# Unified capability discovery attempt 1 diagnosis

Status: `CANARY_GATE_FAILED_IMPLEMENTATION_DEFECT`

This attempt is diagnostic evidence only. It is not a discovery result and
must not be used for candidate promotion, challenge access or forward access.

## Frozen identity

- repository SHA: `21a06153f0b2ab7229e54f7767f5d6e73161998b`
- contract hash: `11229cafe00cc6d08dfa393b064368d4229486cd23a63bd6467677788ad356cc`
- access role: 2024-2025 development only
- validation reads: 0
- holdout reads: 0
- 2026 forward reads: 0

## Completed evidence

The frozen 11-mechanism Broad Event replay completed on all 16 physical
shards before the CANARY gate stopped the run. It materialized 1,655,645
episodes, produced seven shared two-seed survivors and a mean matched-control
increment of 0.0009667947580579803. Those facts remain valid for this exact
attempt identity, but the attempt did not enter unified discovery.

## Gate failure

The two-seed capability CANARY reported zero strict support on
`DISCLOSURE_EVENT`, and incomplete two-seed support on
`SLOW_CROSS_SECTIONAL_LEVEL` and `SLOW_TEMPORAL_CHANGE`. The gate correctly
failed closed and no unified discovery or frozen candidate pack was produced.

The underlying PIT sidecar materializations were populated: each seed had
2,296 stock-session coordinates and the sampled level/change fields were
mostly non-null. The evaluator then joined minute-panel codes such as
`000001.SZ` directly to canonical sidecar codes such as `000001`, silently
turning the populated materializations into null evaluation evidence. The
same mismatch also prevented disclosure episode keys from matching the
stock-session coordinates. The fixed CANARY date `2024-04-29` already had
246 eligible balance-sheet disclosure episodes, so no performance- or
support-directed date change was necessary.

## Resolution

Commit `6d49be00f5c1e31bbbf4b161979bb9ce953142e7` normalizes evaluation
coordinates to the canonical six-digit security key before materialization
and merge, rejects invalid/duplicate normalized coordinates, and adds a
regression test using exchange-suffixed panel codes. Forty-one unified,
fundamental and Broad Event tests passed on 77o before the replacement run.

