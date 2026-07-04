# Phase3EM Schema Compatibility And Workspace Maintenance

Date: 2026-07-05

## Status

Phase3EM fixed the legacy field/schema failure without mutating the true-1min parquet panels.

The current true-1min schema remains explicit:

- `m1_first5/15/30_*`
- `ctx_*`
- `evt_*`
- base minute fields such as `open`, `high`, `low`, `close`, `amount`, `vol`, `vwap`

Legacy aliases are handled as compatibility rewrites inside the evaluator path:

- `$range_location` -> `Div(Sub($close,$low),Add(Abs(Sub($high,$low)),0.000001))`
- `$m1_first_ret` -> `$m1_first5_last_return_vs_open`
- top-level legacy infix syntax such as `A - B` and `A * B` is rewritten to `Sub(A,B)` and `Mul(A,B)`

Each rewritten candidate keeps lineage fields:

- `legacy_alias_rewrites`
- `legacy_alias_original_expression`
- `legacy_alias_source_expression_hash`

## Verification

Local smoke checks passed:

- `phase3em_cm_schema_gate_smoke_20260704`: legacy missing fields are held when compatibility rewrite is disabled.
- `phase3em_cm_schema_gate_mixed_smoke_20260704`: mixed candidates continue while bad-schema candidates are held.
- `phase3em_legacy_alias_rewrite_smoke_20260704`: legacy alias candidate is rewritten and evaluated.
- `phase3em_legacy_alias_no_rewrite_smoke_20260704`: disabling rewrite preserves the schema gate behavior.

Remote 77O compile check passed after syncing:

- `src/our_system_phase2/services/legacy_field_aliases.py`
- `src/our_system_phase2/runtime/phase3cm_train_portfolio_sortino_reward_audit.py`

## Running Search

77O task:

```text
lanjob_20260704_234200_ae1969
```

Run:

```text
phase3em_schema_fixed_fresh_cm_search_20260704_77o
```

Configuration:

```text
generation_budget: 4096
ca_top_n: 384
cm_candidate_limit: 192
cm_workers: 12
cm_max_shards: 16
cm_sample_trade_times_per_shard: 384
cm_horizons: 1,5,15,30,60
memory_root: runtime/search_memory
```

Boundary:

```text
X0/R3 read-only
true1min shard root only
no legacy alias parquet mutation
schema gate remains active
```

## Workspace Maintenance

`runtime/phase3e*/` is now ignored to prevent generated audit/search outputs from polluting git status.

Curated reports remain eligible for explicit staging under `reports/`.

Graphify note:

```text
GSD graphify is disabled because .planning/config.json is absent in both G:\Chengbo and this alpha repo.
No graph artifacts were manually edited.
```
