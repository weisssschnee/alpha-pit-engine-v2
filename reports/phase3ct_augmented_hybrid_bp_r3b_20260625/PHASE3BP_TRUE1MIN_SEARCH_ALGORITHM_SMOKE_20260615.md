# Phase3BP True-1min Search Algorithm Smoke 2026-06-15

Decision: `PHASE3BP_TRUE1MIN_SEARCH_ALGORITHM_SMOKE_COMPLETE_DIAGNOSTIC_ONLY`

## Scope

- generator mode: `true1min_hybrid_rx_cem_smoke`
- candidates generated: `192`
- true-1min shard panels: `4`
- sampled signal trade_times per shard: `160`
- total eval rows: `207055`
- followup priority: `0`

## Generator Comparison

| generator | count | best abs aligned IC | followup | future-wrong-lag |
|---|---:|---:|---:|---:|
| `phase3bp_true1min_hybrid_rx_cem` | 96 | 0.2353486411 | 0 | 40 |

## Lane Summary

| lane | count | best abs aligned IC | followup | future-wrong-lag |
|---|---:|---:|---:|---:|
| `cem_interaction::rx_range_location::rx_opening_amount::residual` | 9 | 0.2353486411 | 0 | 9 |
| `cem_interaction::rx_range_location::coverage_guarded_context::residual` | 6 | 0.2273641845 | 0 | 4 |
| `cem_interaction::rx_range_location::rx_opening_amount::spread` | 6 | 0.227218288 | 0 | 6 |
| `cem_interaction::rx_range_location::coverage_guarded_context::spread` | 3 | 0.2271862281 | 0 | 3 |
| `rx_interaction::rx_range_location::rx_opening_amount::product` | 4 | 0.2188802277 | 0 | 4 |
| `cem_interaction::rx_opening_range::coverage_guarded_context::residual` | 11 | 0.1689142267 | 0 | 7 |
| `cem_interaction::rx_opening_range::coverage_guarded_context::spread` | 6 | 0.1689142267 | 0 | 6 |
| `rx_interaction::rx_intraday_price_location::coverage_guarded_context::product` | 1 | 0.06249323073 | 0 | 1 |
| `rx_interaction::rx_opening_amount::rx_range_location::product` | 5 | 0.02216509187 | 0 | 0 |
| `rx_interaction::rx_opening_amount::rx_range_location::spread` | 3 | 0.0217072448 | 0 | 0 |
| `rx_interaction::rx_opening_range::rx_range_location::product` | 4 | 0.007678683248 | 0 | 0 |
| `typed_event_state::event_payload_state::event_inverted` | 6 | 0 | 0 | 0 |
| `typed_event_state::event_payload_state::event_rank` | 7 | 0 | 0 | 0 |
| `typed_lagged_context::coverage_guarded_context::context_inverted` | 1 | 0 | 0 | 0 |
| `typed_lagged_context::coverage_guarded_context::context_rank` | 1 | 0 | 0 | 0 |
| `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 9 | 0 | 0 | 0 |
| `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 7 | 0 | 0 | 0 |
| `rx_interaction::rx_range_location::coverage_guarded_context::spread` | 1 | 0 | 0 | 0 |
| `rx_interaction::rx_opening_range::coverage_guarded_context::product` | 2 | 0 | 0 | 0 |
| `rx_interaction::rx_range_location::coverage_guarded_context::product` | 1 | 0 | 0 | 0 |

## Top Decisions

| rank | generator | lane | h | fields | abs IC | direction | turnover | decision | blockers |
|---:|---|---|---:|---|---:|---|---:|---|---|
| 1 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_amount::rx_range_location::product` | 30 | `amount|high|low|m1_first15_amount|open` | 0.02216509187 | `long_top` | 0.6750398653 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 2 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_amount::rx_range_location::spread` | 30 | `amount|high|low|m1_first30_amount|open` | 0.0217072448 | `long_top` | 0.6666007481 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 3 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_amount::rx_range_location::product` | 30 | `amount|high|low|m1_first30_amount|open` | 0.02001261363 | `long_top` | 0.6355366891 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 4 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_amount::rx_range_location::spread` | 30 | `amount|high|low|m1_first15_amount|open` | 0.01953624415 | `long_top` | 0.6387065392 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 5 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_amount::rx_range_location::product` | 30 | `amount|high|low|m1_first15_amount|open` | 0.01938305126 | `long_top` | 0.6355561027 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 6 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_range::rx_range_location::product` | 1 | `high|low|m1_first5_high|m1_first5_low|open` | 0.007300918965 | `long_top` | 0.6567551308 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 7 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_range::rx_range_location::product` | 1 | `high|low|m1_first5_high|m1_first5_low|open` | 0.006804721278 | `long_top` | 0.6675994229 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 8 | `phase3bp_true1min_hybrid_rx_cem` | `rx_interaction::rx_opening_range::rx_range_location::product` | 1 | `high|low|m1_first5_high|m1_first5_low|open` | 0.006791856724 | `long_top` | 0.6764686014 | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 9 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 10 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_state::event_payload_state::event_rank` | 1 | `evt_uplimit_auction_buy` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 11 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_auction_money` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 12 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_state::event_payload_state::event_rank` | 1 | `evt_uplimit_auction_offer` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 13 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_fd_close` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 14 | `phase3bp_true1min_hybrid_rx_cem` | `typed_lagged_context::coverage_guarded_context::context_inverted` | 1 | `amount|ctx_ths_hot_rank_diff` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 15 | `phase3bp_true1min_hybrid_rx_cem` | `typed_lagged_context::coverage_guarded_context::context_rank` | 1 | `amount|ctx_ths_hot_circulation_value` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 16 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_zls_strong|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 17 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_ths_hot_last_pct|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 18 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_sent_lb_3_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 19 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_ths_hot_circulation_value|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 20 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_mian_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 21 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_downlimit_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 22 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_lb_2_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 23 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_ditian_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 24 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_sent_downlimit_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 25 | `phase3bp_true1min_hybrid_rx_cem` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_lt5_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |

## Interpretation

- This tests the search algorithm, not production alpha.
- `future_signal_wrong_lag_too_strong` is treated as a hard smoke blocker.
- True `trade_time` 1min shards only; no old 1D stock-PIT default panel.
- X0/R3 remains read-only.
