# Phase3CY Schema-Bound Augmented Search Runbook 2026-06-26

Decision: `PHASE3CY_SCHEMA_BOUND_AUGMENTED_TRUE1MIN_SEARCH_RUNNING_DIAGNOSTIC_ONLY`

## Problem

The first Phase3CY local launch used the AQ-only true1min shard root while train-only
feedback favored sidecar fields such as `ctx_ths_hot_last_price` and
`ctx_ths_hot_last_pct`. The materializer correctly failed with missing candidate
fields instead of silently evaluating invalid formulas.

## Fix

`phase3bs-adaptive-ucb-cem-practice`, `phase3bt-ast-algorithm-bakeoff`, and
`phase3bu-ast-fresh-winner-variants` now bind candidate generation to the common
schema of the selected parquet panels via `_panel_schema_fields(panels)`.

This means:

- AQ-only shards can only generate AQ-available formulas.
- Sidecar fields are only generated when the selected augmented shard root really
  contains those fields.
- Each summary records `schema_bound_generation`, `available_field_count`, and
  `available_fields`.

## Evidence

Compile passed:

```text
python -m py_compile phase3bs_adaptive_ucb_cem_practice.py phase3bt_ast_algorithm_bakeoff.py phase3bu_ast_fresh_winner_variants.py
```

AQ-only schema-bound smoke passed and recorded 49 available fields:

```text
reports/phase3cy_schema_bound_aq_smoke_20260626
```

Partial augmented search root is explicit, not full 16-shard:

```text
runtime/phase3cy_true1min_sidecar_augmented_shards_20260626
reports/phase3cy_true1min_sidecar_augmented_shards_20260626
```

Current partial root:

```text
shards: 3
rows: 56,763,935
columns: 121
new fields verified:
  ctx_hfq_pb
  ctx_hfq_pe_ttm
  ctx_ths_hot_last_pct
  ctx_ths_hot_last_price
  evt_uplimit_active
  evt_uplimit_amount
  evt_uplimit_up_limit_keep_times
```

## Running Searches

All searches are diagnostic only, use true `trade_time` minute panels, and use
Phase3CM train-only feedback. Validation and holdout are report-only.

```text
BT:
  runtime/phase3cy_local_augmented_bt_medium_20260626
  reports/phase3cy_local_augmented_bt_medium_20260626

BS:
  runtime/phase3cy_local_augmented_bs_parallel_20260626
  reports/phase3cy_local_augmented_bs_parallel_20260626

BU:
  runtime/phase3cy_local_augmented_bu_parallel_20260626
  reports/phase3cy_local_augmented_bu_parallel_20260626
```

## Boundary

- No old 1D data.
- No official X0/R3 modification.
- Do not treat proxy IC, first-up, or final-only reward as proof.
- Current root is a partial 3-shard augmented canary root; it is suitable for
  algorithm/search-path validation, not full production inference.

## Next Action

After the three running searches emit round outputs, audit:

- source generator and lane attribution;
- train reward vs validation/holdout report-only fields;
- missing field hits, if any;
- top family share and signal crowding;
- whether sidecar fields are actually consumed in top decisions.
