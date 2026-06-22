# Phase3CG True1min Fastpath Prep 2026-06-19

## Decision

`PHASE3CG_TRUE1MIN_FASTPATH_READY_FOR_CONTROLLED_RESTART`

This is an execution-engine fix and launch preparation step. It does not promote
alpha, does not modify X0/R3, and does not change the true1min data root.

## Problem Confirmed

The previous large run was slow because the active true1min search path used
pandas full-panel groupby/rank/transform and signal-vector object-index sorting:

- `CSRank/Rank`: `groupby(...).rank(pct=True)`
- `ZScore/MaskedZScore`: `groupby(...).transform("mean/std")`
- signal vector crowding: `pd.concat(chunks).sort_index()`
- pairwise crowding was computed before a bounded top-N signal-vector cut.

Installed acceleration libraries were present, but the hot path did not call
`numba`, `polars`, `joblib`, `use_fast_context`, or a global worker limit.
`pyarrow` was only active for parquet column/time reads.

## Changes

- Added memory-stable `fast_rank_pct_by_group`.
- Added memory-stable `fast_zscore_by_group`.
- Routed `CSRank/Rank`, `ZScore`, `MaskedZScore`, and Phase3BL label/signal
  ranks through the fast implementations.
- Bounded signal-vector storage for pairwise crowding:
  `pairwise_candidate_limit=96`.
- Removed the high-memory `pd.concat(chunks).sort_index()` path for signal
  vectors. Pairwise vectors now concatenate by stable evaluation order with
  `ignore_index=True`.

## Validation

Local compile:

```text
py_compile passed:
  real_market_validation.py
  phase3bl_bk_priority_signal_materialization.py
  phase3bs_adaptive_ucb_cem_practice.py
  phase3bt_ast_algorithm_bakeoff.py
```

Parity smoke:

```text
rank_max_abs_diff: 0.0
zscore_max_abs_diff: 8.88e-16
rank_nan_match: true
z_nan_match: true
```

True1min smoke:

```text
route: phase3bs-adaptive-ucb-cem-practice
shards: 1
sample_trade_times_per_shard: 16
seed_candidates: 24
round1 elapsed_seconds: 16.699
round1 rows_per_second: 310.91
pairwise_candidate_limit: 96
status: ok
```

Company sync:

```text
uploaded patched files to:
  D:\HermesWorker\workspace\alpha_pit_true1min_engine_20260619

company py_compile: passed
```

## Prepared Launchers

Local prepared launcher, not auto-started:

```text
G:\Chengbo\runtime\phase3cg_local_fastpath_3lane_20260619.ps1
G:\Chengbo\runtime\phase3cg_local_fastpath_3lane_20260619\launcher_manifest.preview.json
```

Local policy:

```text
max lanes: 3
true1min shard root:
  G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531\runtime\phase3au_aq_only_true1min_sharded_20260611
old 1D data: forbidden
```

Company prepared launcher:

```text
G:\Chengbo\runtime\phase3cg_company_fastpath_1lane_20260619.ps1
remote:
  D:\HermesWorker\runtime\phase3cg_company_fastpath_1lane_20260619.ps1
```

Company policy:

```text
max A-share lane: 1
crypto-line: untouched
true1min shard root:
  D:\HermesWorker\workspace\phase3aj_new_data_current\runtime\phase3au_company_full_true1min_sharded_20260611
old 1D data: forbidden
```

## Next Launch Contract

Do not restart with 8 local lanes. Recommended restart:

```text
local: 2-3 lanes max
company: 1 A-share lane max while crypto-line is active
checkpoint check: every 30-45 minutes
scale-up condition:
  no pandas MemoryError
  report/runtime files written
  hard-blocked ratio and research_pool_count parsed
```

If MemoryError remains after this patch, the next required fix is a deeper
chunked expression evaluator for rolling/code-time operators and residuals.
