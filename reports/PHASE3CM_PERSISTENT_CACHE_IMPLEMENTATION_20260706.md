# Phase3CM Persistent Cache Implementation

Date: 2026-07-06

## Decision

Phase3CM now supports an optional persistent cache layer for true1min train-reward evaluation.

This is an acceleration feature only. It does not change X0/R3, promotion gates, portfolio construction, train/validation/holdout split rules, or reward semantics.

## Scope

Implemented in:

- `src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py`
- `src/our_system_phase2/runtime/phase3cp_real_cm_small_loop.py`

New Phase3CM flags:

```text
--persistent-cache-root
--persistent-cache-mode off|read|write|readwrite
--disable-persistent-expression-cache
--disable-persistent-operator-cache
--disable-persistent-feature-matrix-cache
```

New Phase3CP pass-through flags:

```text
--cm-persistent-cache-root
--cm-persistent-cache-mode off|read|write|readwrite
--cm-disable-persistent-expression-cache
--cm-disable-persistent-operator-cache
--cm-disable-persistent-feature-matrix-cache
```

## Safety Contract

Persistent cache keys include:

```text
cache_version
panel file fingerprint
read column fingerprint
sample block index
context window
context trade_time fingerprint
eval trade_time fingerprint
expression/operator key
```

This prevents reuse across different shard files, field sets, sampled time blocks, context windows, or eval trade_time sets.

## Cache Types

```text
factor expression signal:
  persisted as float64 .npy series

operator subtree result:
  persisted as bounded float64 .npy series

feature matrix context frame:
  persisted as parquet only when the exact column set matches
```

## Runtime Boundary

The old in-memory cache still exists and remains the first layer:

```text
expression_cache_scope: per_shard
feature_matrix_cache_scope: per_shard_context_window
operator_cache_scope: per_shard_context_window
```

The new persistent cache is optional and enabled only when a root is passed.

Recommended 77O root:

```text
D:\ChengboRemote\cache\phase3cm_persistent_series_cache
```

## Verification

Completed locally:

```text
python -m py_compile
synthetic persistent series cache write/read/length-mismatch smoke
git diff --check
```

Completed on 77O true1min data:

```text
machine: DESKTOP-77OPJ6F
candidate_count: 8
max_shards: 1
sample_trade_times_per_shard: 32
horizons: 1,5
status: PASS
reward_rows_stable: true

pass1:
  persistent_factor_expression_cache_disk_stores: 8
  operator_cache_disk_stores: 59
  persistent_feature_matrix_cache_disk_stores: 3

pass2:
  persistent_factor_expression_cache_disk_hits: 8
```

Remote summary:

```text
D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill\runtime\phase3cm_persistent_cache_tiny_validation_20260706\phase3cm_persistent_cache_tiny_validation_summary.json
```

Local pulled copy:

```text
G:\Chengbo\runtime\phase3cm_persistent_cache_tiny_validation_pull_20260706\phase3cm_persistent_cache_tiny_validation_summary.json
```

## Operational Rule

Do not enable this by editing old running jobs. Use it in the next launched Phase3CP/Phase3CM run.
