# Phase3BN Open Diversified True-1min Canary 2026-06-15

Decision: `PHASE3BN_OPEN_DIVERSIFIED_CANARY_COMPLETE_DIAGNOSTIC_ONLY`

## Scope

- candidates generated: `48`
- true-1min shard panels: `4`
- sampled signal trade_times per shard: `60`
- total eval rows: `77662`
- followup priority: `1`

## Lane Counts

| lane | count | best aligned IC | followup |
|---|---:|---:|---:|
| `opening_vs_intraday_return_divergence` | 15 | 0.1159485662 | 1 |
| `opening_directional_return` | 2 | 0.02812459417 | 0 |
| `opening_volume_range_imbalance` | 15 | 0.02610984157 | 0 |

## Top Decisions

| rank | lane | h | fields | abs IC | direction | turnover | decision | blockers |
|---:|---|---:|---|---:|---|---:|---|---|
| 1 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1159485662 | `long_top` | 0.7782191547 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 2 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1135656332 | `long_top` | 0.7784825501 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 3 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1117617753 | `long_top` | 0.7775060887 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 4 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1115654056 | `long_top` | 0.7801223747 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 5 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1087263553 | `long_top` | 0.7797543149 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 6 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1085120194 | `long_top` | 0.7813093233 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 7 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1080688145 | `long_top` | 0.7782020342 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 8 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.107782343 | `long_top` | 0.7842814675 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 9 | `opening_vs_intraday_return_divergence` | 1 | `m1_first15_vwap_return_vs_open|ret_1m` | 0.1067902391 | `long_top` | 0.7809701944 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 10 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1065230386 | `long_top` | 0.7797607796 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 11 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1055187797 | `long_top` | 0.7810689453 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 12 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.104924186 | `long_top` | 0.7822038094 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 13 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1020402536 | `long_top` | 0.7794068239 | `bn_followup_priority` | `` |
| 14 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1013096785 | `long_top` | 0.7826965155 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 15 | `opening_vs_intraday_return_divergence` | 1 | `m1_first5_vwap_return_vs_open|ret_1m` | 0.1001272441 | `long_top` | 0.7806993422 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80` |
| 16 | `opening_directional_return` | 30 | `m1_first5_last_close|open` | 0.02812459417 | `long_top` | 0.7707447739 | `bn_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 17 | `opening_directional_return` | 30 | `m1_first15_last_close|open` | 0.02763803847 | `long_top` | 0.7754050964 | `bn_watch_or_reject` | `too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 18 | `opening_volume_range_imbalance` | 5 | `m1_first5_range|m1_first5_vol|open|volume` | 0.02610984157 | `long_top` | 0.4749331652 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 19 | `opening_volume_range_imbalance` | 5 | `m1_first5_range|m1_first5_vol|open|volume` | 0.02607137128 | `long_top` | 0.4784745596 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80|too_few_positive_horizons|weak_dense_primary_abs_ic` |
| 20 | `opening_volume_range_imbalance` | 5 | `m1_first5_range|m1_first5_vol|open|volume` | 0.02540656908 | `long_top` | 0.4494970138 | `bn_watch_or_reject` | `signal_corr_abs_ge_0.80|too_few_positive_horizons|weak_dense_primary_abs_ic` |

## Boundary

- True `trade_time` 1min panels only.
- This is open diversified canary, not promotion evidence.
- Crowded pure `close/vwap` residual family is capped and not allowed to dominate.
- X0/R3 remains read-only.
