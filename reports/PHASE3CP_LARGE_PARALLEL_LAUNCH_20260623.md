# Phase3CP Large Parallel Launch 2026-06-23

Decision: `PHASE3CP_TRUE1MIN_LARGE_PARALLEL_RUNNING`

## Purpose

Run a larger true-1min Phase3CP search using real Phase3CM train portfolio Sortino reward, not fragment replay and not proxy IC.

## Hard Boundaries

```text
X0/R3:
  read-only

data:
  true trade_time 1min shards only
  old 1D / tdxofficial paths are refused by route guard

reward:
  Phase3CM train portfolio Sortino reward
  validation/holdout are report-only guards
  Phase3BZ fragment replay remains diagnostic-only

memory:
  --memory-root reports
  prior expression_hash values are blocked during generation
```

## Code Change

`phase3cp-real-cm-small-loop` now supports cross-run expression memory:

```text
--memory-root
--memory-glob
```

The route writes:

```text
phase3cp_real_cm_memory_roots.csv
memory_hash_count
memory_roots
```

This is bookkeeping / duplicate prevention. It is not a reward signal and does not bias sampling toward historical winners.

## Arm Budgets

```text
runtime/run_plans/phase3cp_large_rx_typed_budget_20260623.csv
runtime/run_plans/phase3cp_large_challenger_cem_budget_20260623.csv
```

## Local Jobs

```text
launcher:
  G:\Chengbo\runtime\phase3cp_large_parallel_local_20260623.ps1

true1min shard:
  G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3au_aq_only_true1min_sharded_20260611

jobs:
  phase3cp_large_local_rx_typed_q20_20260623
    generation: 1200
    ca_top_n: 900
    cm_candidate_limit: 192
    cm_max_shards: 6
    cm_sample_trade_times_per_shard: 160
    cm_top_quantile: 0.20

  phase3cp_large_local_challenger_cem_q20_20260623
    generation: 900
    ca_top_n: 700
    cm_candidate_limit: 160
    cm_max_shards: 6
    cm_sample_trade_times_per_shard: 160
    cm_top_quantile: 0.20
```

## Company Jobs

```text
launcher:
  D:\HermesWorker\runtime\phase3cp_large_parallel_company_20260623_logs

true1min shard:
  D:\HermesWorker\workspace\phase3aj_new_data_current\runtime\phase3au_company_full_true1min_sharded_20260611

wrapper task ids:
  job_20260623_112916_6f09f9
  job_20260623_112916_b20ba2

jobs:
  phase3cp_large_company_rx_typed_q20_20260623
    generation: 1600
    ca_top_n: 1200
    cm_candidate_limit: 256
    cm_max_shards: 8
    cm_sample_trade_times_per_shard: 192
    cm_top_quantile: 0.20

  phase3cp_large_company_challenger_cem_q25_20260623
    generation: 1200
    ca_top_n: 900
    cm_candidate_limit: 224
    cm_max_shards: 8
    cm_sample_trade_times_per_shard: 192
    cm_top_quantile: 0.25
```

## Initial Health

```text
local:
  two Phase3CP Python workers entered compute
  stderr empty at launch

company:
  start-detached wrapper used after Start-Process jobs exited without logs
  two new Phase3CP Python workers entered compute
  stderr empty at launch
```

## Next Checkpoint

Check when either side writes:

```text
phase3cm_train_reward/phase3cm_train_reward.csv
phase3cp_real_cm_small_loop_summary.json
phase3cn_feedback_memory/phase3cn_search_feedback_memory.csv
```

Promotion remains blocked unless CM followup families pass multi-shard confirmation.
