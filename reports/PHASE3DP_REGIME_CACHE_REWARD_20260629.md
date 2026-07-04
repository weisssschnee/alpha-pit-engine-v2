# Phase3DP Regime Stability Reward and Cache Wiring 2026-06-29

## Experiment Record

- date: 2026-06-29
- experiment_id: `20260629_phase3dp_regime_cache_reward_wiring_001`
- objective: make Phase3CM reward usable for deeper true1min search by adding low-weight train regime-stability reward and bounded evaluator caches
- status: completed
- mode: research

## Changes

### Reward

Phase3CM now adds a train-only low-weight regime component:

```text
 train_regime_reward_component
```

It is computed from train split only. Validation and holdout remain report-only.

Default configuration:

```text
regime_stability_weight: 0.08
regime_component_cap: 0.10
```

The component is intentionally small. It is a search-gradient term, not a promotion gate.

New reward fields:

```text
train_regime_stability_score
train_regime_reward_component
train_regime_worst_day_sortino
train_regime_median_day_sortino
train_regime_positive_share
train_regime_count
train_regime_method
train_regime_rows
```

Optimizer reward metric name for new runs:

```text
train_portfolio_sortino_rankic_regime_composite_reward
```

Regime method:

```text
primary: train market-return terciles
fallback: train chronological terciles
```

### Cache

Phase3CM now has three bounded cache layers:

```text
factor expression cache:
  expression -> evaluated signal on eval frame

feature matrix cache:
  context window -> sliced context frame + eval mask

operator subtree cache:
  recursive expression/operator subtree -> Series
```

Default configuration:

```text
operator_cache_max_entries: 512 per shard/context window
feature_matrix_cache_max_windows: 6 per shard
```

Cache stats are written to:

```text
phase3cm_train_reward_audit_summary.json
phase3cm_shard_meta.csv
```

## Files Changed

```text
src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py
src/our_system_phase2/runtime/phase3cp_real_cm_small_loop.py
src/our_system_phase2/services/candidate_schema.py
```

## Smoke Test

Command:

```text
G:\PythonProject\.venv\Scripts\python.exe -m our_system_phase2.runtime.phase3cm_train_portfolio_sortino_reward_audit --candidate-audit runtime\phase3do_d_local_light_fresh_prep_20260629\phase3ca_bridge\phase3ca_bz_candidate_audit.csv --shard-root runtime\phase3cy_true1min_sidecar_augmented_shards_20260626 --output-root runtime\phase3dp_regime_cache_smoke_20260629 --report-root reports\phase3dp_regime_cache_smoke_20260629 --candidate-limit 2 --max-shards 1 --sample-trade-times-per-shard 16 --horizons 1,5 --train-fraction 0.6 --validation-fraction 0.2 --min-obs-per-time 20 --cost-bps 5 --top-quantile 0.2 --rank-ic-loss-weight 6.0 --rank-ic-component-cap 0.35 --regime-stability-weight 0.08 --regime-component-cap 0.10 --operator-cache-max-entries 256 --feature-matrix-cache-max-windows 4 --fast-mode --numexpr-threads 2
```

Result:

```text
candidate_count: 2
followup_count: 0
decision: PHASE3CM_TRAIN_REWARD_AUDIT_READY_DIAGNOSTIC_ONLY
```

Observed cache stats:

```text
factor_expression_cache_misses: 2
factor_expression_cache_stores: 2
feature_matrix_cache_misses: 1
feature_matrix_cache_stores: 1
feature_matrix_cache_hits: 1
operator_cache_misses: 31
operator_cache_stores: 31
operator_cache_hits: 7
```

Observed reward fields:

```text
train_regime_stability_score: present
train_regime_reward_component: present
train_regime_method: present
```

## Launch Contract For Next Large Search

Use Phase3CM through Phase3CP with:

```text
--cm-regime-stability-weight 0.08
--cm-regime-component-cap 0.10
--cm-operator-cache-max-entries 512
--cm-feature-matrix-cache-max-windows 6
```

Recommended next search direction:

```text
typed_ast_fresh:
  keep as main exploration lane

turnover_aware_fresh:
  keep, but do not trust label until lower realized turnover appears

cem_exploit:
  keep capped until at least two clean train-reward families exist

repair:
  target low-turnover variants of positive optimizer_reward families
```

Do not turn `extreme_turnover` into a pure hard rejection at search-feedback time. It should remain:

```text
promotion blocker
search penalty
repair trigger
```

## Reproducibility

- reproducible: partial
- reason: smoke is reproducible on local true1min augmented shard snapshot; full 77o run depends on remote copied workspace and node-local data snapshot

## Decision

`HOLD_RESEARCH`

This is infrastructure wiring and smoke validation, not alpha proof.
