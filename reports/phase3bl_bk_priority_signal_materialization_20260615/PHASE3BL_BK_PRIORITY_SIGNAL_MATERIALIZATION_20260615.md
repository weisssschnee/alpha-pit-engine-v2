# Phase3BL BK Priority Signal Materialization 2026-06-15

Decision: `PHASE3BL_SIGNAL_MATERIALIZATION_COMPLETE_DIAGNOSTIC_ONLY`

## Scope

- true-1min shard panels: `16`
- priority candidates: `4`
- sampled signal trade_times per shard: `120`
- total eval rows: `617516`
- signal-vector pair count: `6`

## Boundary

- Uses true `trade_time` 1min panels from Phase3AU AQ shards.
- Reads contiguous minute warmup windows before sampled signal times.
- Does not modify X0/R3 and does not promote candidates.
- Old 149 signal caches are not same-domain minute vectors; this stage only proves candidate-to-candidate minute-vector crowding.

## Primary Horizon Results

| bk_rank | h | lane | fields | aligned_ic | spread | turnover | wrong_lag_future_ic | decision | blockers |
|---:|---:|---|---|---:|---:|---:|---:|---|---|
| 1 | 1 | `pure_fresh_cross` | `close|volume|vwap` | 0.1259834512 | 0.0004140451667 | 0.7365219938 | 0.3215184178 | `bl_signal_materialized_watch` | `future_signal_wrong_lag_too_strong` |
| 2 | 1 | `pure_fresh_cross` | `close|volume|vwap` | 0.0991719322 | 0.000392963984 | 0.791464021 | -0.231308738 | `bl_signal_materialized_watch` | `future_signal_wrong_lag_too_strong` |
| 3 | 30 | `pure_fresh_cross` | `close|m1_first30_vwap|vwap` | 0.0508481573 | 0.0001439667384 | 0.7416094522 | -0.04086017951 | `bl_signal_materialized_pass` | `` |
| 4 | 5 | `opening_vwap_residual` | `close|m1_first30_vwap` | 0.04899406812 | 0.0001434719703 | 0.7619393164 | -0.01135236772 | `bl_signal_materialized_pass` | `` |

## Pairwise Signal Rank Correlation

| left | right | corr |
|---|---|---:|
| `b68c515d47ec76c97d51f2db` | `c32780d6a803e8a4944e7918` | -0.7470322497 |
| `3885a1e358e7b5d27ec6ee39` | `579b91fa7699467e04d671e3` | -0.345387792 |
| `3885a1e358e7b5d27ec6ee39` | `c32780d6a803e8a4944e7918` | 0.1227448517 |
| `579b91fa7699467e04d671e3` | `c32780d6a803e8a4944e7918` | -0.1018988693 |
| `3885a1e358e7b5d27ec6ee39` | `b68c515d47ec76c97d51f2db` | -0.09495060714 |
| `579b91fa7699467e04d671e3` | `b68c515d47ec76c97d51f2db` | 0.04253051802 |

## Next Gate

If primary-horizon aligned IC survives and pairwise crowding is acceptable, run a wider materialization on more signal times or a full replay shard job. If not, redirect search budget away from this crowded `volume/vwap/close` family.
