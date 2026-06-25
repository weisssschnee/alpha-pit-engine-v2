# Phase3BO Mature CEM Bridge True-1min Pack 2026-06-15

Decision: `PHASE3BO_MATURE_CEM_BRIDGE_TRUE1MIN_COMPLETE_DIAGNOSTIC_ONLY`

## Plain Answer

- The old mature chain does not expose a single `cem.py` file.
- Its CEM-like logic is `rx_typed_beam` plus bandit/UCB policy routing plus family/memory guards.
- Phase3BN used true 1min data, but did not fully call that mature policy stack.
- Phase3BO fixes the route by keeping true 1min shards and adding mature-style quotas, memory, and crowding caps.

## Scope

- candidates generated: `32`
- true-1min shard panels: `6`
- sampled signal trade_times per shard: `50`
- total eval rows: `96822`
- followup priority: `1`

## Lane Counts

| lane | count | best abs aligned IC | followup |
|---|---:|---:|---:|
| `intraday_efficiency_fresh` | 3 | 0.1864421834 | 0 |
| `opening_divergence_representative` | 1 | 0.1118294146 | 0 |
| `opening_range_location` | 11 | 0.08979847378 | 0 |
| `bounded_vwap_mixed` | 2 | 0.0570469563 | 1 |
| `opening_amount_pressure_orthogonal` | 9 | 0.02783835692 | 0 |
| `amount_volume_flow` | 3 | 0.01268674875 | 0 |
| `range_volatility_residual` | 3 | 0.007155048398 | 0 |

## Top Decisions

| rank | lane | h | fields | abs IC | direction | turnover | decision | blockers |
|---:|---|---:|---|---:|---|---:|---|---|
| 1 | `intraday_efficiency_fresh` | 1 | `intraday_ret_from_open|ret_1m` | 0.1864421834 | `short_top` | 0.7908368726 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 2 | `intraday_efficiency_fresh` | 1 | `intraday_ret_from_open|ret_1m` | 0.1709688351 | `short_top` | 0.7919602341 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 3 | `intraday_efficiency_fresh` | 1 | `intraday_ret_from_open|ret_1m` | 0.1562124146 | `short_top` | 0.7906689789 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 4 | `opening_divergence_representative` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1118294146 | `long_top` | 0.7728436652 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 5 | `opening_range_location` | 1 | `close|high|low` | 0.08979847378 | `short_top` | 0.8058823795 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 6 | `opening_range_location` | 1 | `close|high|low` | 0.08861946793 | `short_top` | 0.7983424958 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 7 | `bounded_vwap_mixed` | 5 | `amount|m1_first5_amount|vwap` | 0.0570469563 | `long_top` | 0.7399702234 | `bo_watch_or_reject` | `future_signal_wrong_lag_too_strong` |
| 8 | `bounded_vwap_mixed` | 15 | `amount|m1_first5_amount|vwap` | 0.05082457575 | `long_top` | 0.7500375388 | `bo_followup_priority` | `` |
| 9 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first15_amount|m1_first15_vol|volume` | 0.02783835692 | `long_top` | 0.7858442067 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 10 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first5_amount|m1_first5_vol|volume` | 0.02627000541 | `long_top` | 0.7820054802 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 11 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first15_amount|m1_first15_vol|volume` | 0.02211051396 | `long_top` | 0.7826906069 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 12 | `opening_range_location` | 5 | `amount|m1_first15_amount|m1_first15_range|open` | 0.02165524966 | `long_top` | 0.5017097998 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 13 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first30_amount|m1_first30_vol|volume` | 0.02152809276 | `long_top` | 0.7855027434 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 14 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first5_amount|m1_first5_vol|volume` | 0.02035089315 | `long_top` | 0.7822008458 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 15 | `opening_range_location` | 5 | `amount|m1_first30_amount|m1_first30_range|open` | 0.02005334907 | `long_top` | 0.4938124867 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 16 | `opening_range_location` | 5 | `amount|m1_first15_amount|m1_first15_range|open` | 0.01998865833 | `long_top` | 0.5901927383 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 17 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first15_amount|m1_first15_vol|volume` | 0.0192555133 | `long_top` | 0.7877532675 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 18 | `opening_range_location` | 5 | `amount|m1_first30_amount|m1_first30_range|open` | 0.01888725493 | `long_top` | 0.5917539039 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 19 | `opening_range_location` | 5 | `amount|m1_first5_amount|m1_first5_range|open` | 0.01801373783 | `long_top` | 0.5149118077 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 20 | `opening_amount_pressure_orthogonal` | 30 | `amount|m1_first5_amount|m1_first5_vol|volume` | 0.01791070536 | `long_top` | 0.7834173953 | `bo_watch_or_reject` | `signal_corr_abs_ge_0.75|future_signal_wrong_lag_too_strong|too_few_positive_horizons|weak_dense_primary_abs_ic` |

## Mature Algorithm Files

- `src/our_system_phase2/runtime/phase3ab_launch_large_search.py`
- `src/our_system_phase2/runtime/stock_pit_large_search_supervisor.py`
- `src/our_system_phase2/runtime/stock_pit_large_search_worker.py`
- `src/our_system_phase2/services/stock_pit_forward_first_search.py`
- `src/our_system_phase2/services/stock_pit_ledger_policy.py`

## Boundary

- True `trade_time` 1min shards only.
- No old 1D default stock-PIT panel is used.
- X0/R3 remains read-only.
- Diagnostic pack, not alpha promotion proof.
