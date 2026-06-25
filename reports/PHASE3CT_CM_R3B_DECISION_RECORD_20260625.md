# Phase3CT/CM R3B Decision Record 2026-06-25

## Scope

This record covers the LAN `DESKTOP-77OPJ6F` guarded r3b run:

- BP search: `phase3ct_augmented_hybrid_bp_r3b_20260625`
- CT field usage audit: `phase3ct_augmented_hybrid_bp_r3b_20260625_field_usage_audit`
- CM train portfolio Sortino reward audit: `phase3cm_from_phase3ct_augmented_hybrid_r3b_20260625`

The run used true `trade_time` 1min sidecar-augmented shards only. X0/R3 remained read-only.

## Execution Notes

- Original r3 job was stopped because it ran for more than 50 minutes with no checkpoint or report output.
- BL/BP materialization was patched to write `phase3bp_progress.json` during evaluation.
- r3b restarted with the same BP/CT search spec and completed successfully.
- CM was run only after CT passed.

## BP Search Result

- candidates generated: 192
- panels: 4
- available schema-bound fields: 86
- total eval rows: 207,055
- followup priority count: 0
- best followup: null

Top BP rows were mostly opening amount/range-location interactions. They had weak dense IC and too few positive horizons.

Best BP row:

```text
candidate_id: phase3bp_00105
fields: amount|high|low|m1_first15_amount|open
abs_aligned_ic_mean: 0.02216509187
horizon_min: 30
blockers: too_few_positive_horizons|weak_dense_primary_abs_ic
decision: bp_watch_or_reject
```

## CT Field Usage Audit

Decision:

```text
PASS_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT
```

Key coverage:

```text
schema_intersection_field_count: 86
effective_used_field_count: 46
used_lane_counts:
  direct_formula: 3056
  event_state: 158
  lagged_context: 431
effective_used_lane_counts:
  direct_formula: 2741
  event_state: 68
  lagged_context: 296
future_wrong_lag_followup_rows: 0
```

Interpretation: the sidecar/event/context fields are now reaching the search path and producing effective non-null signals. This is a field-path pass, not an alpha pass.

## CM Train Reward Audit

- input candidates after hard-blocked filtering: 56
- followup count: 0
- fast mode: true
- checkpoint interval: 4 candidate-shards
- all audited candidates: `HOLD_TRAIN_REWARD`

Best CM row:

```text
candidate_id: phase3bp_00089
fields: amount|high|low|m1_first5_amount|open
train_reward: -0.90686831
train_day_sortino: -0.62551138
validation_day_sortino: -0.65335447
holdout_day_sortino: -0.74470714
train_day_mcmc_prob_gt_0: 0
validation_day_mcmc_prob_gt_0: 0
holdout_day_mcmc_prob_gt_0: 0
decision: HOLD_TRAIN_REWARD
```

## Decision

```text
R3B_LANE_DO_NOT_EXPAND_AS_IS
```

The field integration path is fixed enough to run guarded search, but this r3b candidate family is not worth expanding. It is dominated by weak opening amount/range-location motifs, and CM confirms negative train, validation, and holdout reward.

## Next Search Implication

Do not spend another large run on the same opening amount/range-location surface. Next search should redirect budget toward:

1. event-state primitives with explicit non-null/effective-signal constraints,
2. lagged-context interactions that are not just opening amount/range variants,
3. reward-aware generation using CM train reward, not BP proxy IC alone,
4. stricter crowding and family caps before materialization.

This run supports the chain conclusion: field-path wiring is viable; current generated motif family is not.
