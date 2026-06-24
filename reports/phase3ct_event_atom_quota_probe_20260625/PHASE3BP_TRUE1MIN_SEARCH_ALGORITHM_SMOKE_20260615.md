# Phase3BP True-1min Search Algorithm Smoke 2026-06-15

Decision: `PHASE3BP_TRUE1MIN_SEARCH_ALGORITHM_SMOKE_COMPLETE_DIAGNOSTIC_ONLY`

## Scope

- generator mode: `true1min_event_state_smoke`
- candidates generated: `64`
- true-1min shard panels: `2`
- sampled signal trade_times per shard: `60`
- total eval rows: `38983`
- followup priority: `0`

## Generator Comparison

| generator | count | best abs aligned IC | followup | future-wrong-lag |
|---|---:|---:|---:|---:|
| `phase3bp_true1min_typed_event_state` | 32 | 0 | 0 | 0 |

## Lane Summary

| lane | count | best abs aligned IC | followup | future-wrong-lag |
|---|---:|---:|---:|---:|
| `typed_event_state::event_payload_state::event_inverted` | 5 | 0 | 0 | 0 |
| `typed_event_state::event_payload_state::event_rank` | 4 | 0 | 0 | 0 |
| `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 15 | 0 | 0 | 0 |
| `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 8 | 0 | 0 | 0 |

## Top Decisions

| rank | generator | lane | h | fields | abs IC | direction | turnover | decision | blockers |
|---:|---|---|---:|---|---:|---|---:|---|---|
| 1 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 2 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_auction_money` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 3 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_rank` | 1 | `evt_uplimit_auction_buy` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 4 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_auction_buy` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 5 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_auction_money` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 6 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_rank` | 1 | `evt_uplimit_auction_offer` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 7 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_inverted` | 1 | `evt_uplimit_fd_close` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 8 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_rank` | 1 | `evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 9 | `phase3bp_true1min_typed_event_state` | `typed_event_state::event_payload_state::event_rank` | 1 | `evt_uplimit_auction_pre1max_ratio` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 10 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_zls_strong|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 11 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_ths_hot_last_pct|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 12 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_sent_lb_3_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 13 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_ths_hot_circulation_value|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 14 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_mian_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 15 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_downlimit_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 16 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_lb_2_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 17 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_ditian_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 18 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_sent_downlimit_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 19 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_lt5_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 20 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_ths_hot_rank_diff|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 21 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_minus_state` | 1 | `amount|ctx_ths_hot_last_price|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 22 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_mian_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 23 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_ths_hot_rank|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 24 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_lb_3_num|evt_uplimit_type_code` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 25 | `phase3bp_true1min_typed_event_state` | `typed_event_interaction::event_payload_state::coverage_guarded_context::event_x_state` | 1 | `amount|ctx_sent_up_num|evt_uplimit_up_limit_keep_times` |  | `short_top` |  | `bp_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |

## Interpretation

- This tests the search algorithm, not production alpha.
- `future_signal_wrong_lag_too_strong` is treated as a hard smoke blocker.
- True `trade_time` 1min shards only; no old 1D stock-PIT default panel.
- X0/R3 remains read-only.
