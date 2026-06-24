# Phase3CT Controlled Data Inventory 2026-06-25

This inventory records local controlled data assets relevant to true1min field usage. It is an availability catalog, not proof of alpha value.

| asset | lane | exists | file_count | size_gb |
|---|---|---:|---:|---:|
| `stock_1min_2023_2025_symbol_parquet` | `true1min_base` | True | 15582 | 0.0241 |
| `stock_1min_2026_parquet_by_date` | `true1min_base` | True | 63 | 1.413 |
| `hfq_daily_2024_2025` | `lagged_context_sidecar_source` | True | 2 | 0.2637 |
| `hfq_daily_2026` | `lagged_context_sidecar_source` | True | 1 | 0.0454 |
| `fundamental_top200_aggregated` | `lagged_context_source_smoke` | True | 13 | 0.034 |
| `fundamental_fullA_partitioned_pit` | `lagged_context_source` | True | 26193 | 3.0201 |
| `fundamental_fullA_silver_gold_pit` | `lagged_context_source` | True | 3 | 0.9864 |
| `zzshare_limit_sentiment_pack` | `event_state_and_lagged_context_source` | True | 2537 | 0.1436 |
| `zzshare_uplimit_history_silver` | `event_state_source` | True | 7 | 0.0399 |
| `rzrq_daily_silver` | `lagged_context_source` | True | 3 | 0.4026 |
| `xsection_no_kline_silver` | `lagged_context_event_source` | True | 9 | 0.104 |
| `local_index_1min_silver_v2` | `market_context_source` | True | 34 | 0.1285 |
| `zzshare_plate_fund_probe` | `membership_context_probe` | True | 22 | 0.0016 |
| `zzshare_advanced_field_probe` | `event_context_probe` | True | 72 | 0.0011 |
| `phase3cs_zls_sidecar_pack` | `current_sidecar_pack` | True | 5 | 0.0002 |

Current searchable proof is still governed by Phase3CT: field availability must be paired with candidate effective signal usage.
