# Phase3CT True1min Field Chain Decision Record

Decision: `FIELD_CHAIN_GUARD_REQUIRED_BEFORE_LARGE_SEARCH`

## What Changed

- Added `phase3ct-true1min-lifecycle-field-usage-audit`.
- Added schema-bound field lifecycle checks across:
  - true `trade_time` shard schema
  - sidecar pack presence
  - atom/lane routing
  - chronological train/validation/test field coverage
  - candidate field subset checks
  - effective evaluated signal usage
- Updated Phase3BP event-state generation to reserve separate quotas for:
  - raw event atoms
  - lagged context atoms
  - event/context interactions
- Updated Phase3CP reward-gated smoke so generator arms are schema-bound to the selected true1min shard root by default.

## Evidence From LAN Audit

Run: `phase3ct_lifecycle_augmented_zls4_event_top_20260625`

Result: `HOLD_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT`

Key findings:

- augmented true1min root has 86 schema fields.
- lane counts:
  - `direct_formula`: 39
  - `event_state`: 12
  - `lagged_context`: 24
  - `blocked_key_or_label`: 11
- atom inventory:
  - `formula_search`: 145
  - `event_state_search`: 24
  - `lagged_context_search`: 72
- chronological field coverage exists across train/validation/test for direct, event, and context lanes.
- old event_state top decisions used event/context fields symbolically but had `effective_signal_rows = 0`.

## Interpretation

The data plumbing is materially better than before: event/context fields are present in augmented true1min shards, routed into typed atoms, and covered across chronological splits.

But the previous event_state candidate selection still did not produce effective evaluated signals. Therefore the system must not claim these fields are already well used by train/validation/test reward.

Follow-up probe: `phase3ct_event_atom_quota_probe_20260625`

- raw event atom quota fixed the full-zero event symptom partially.
- top decisions showed 2/32 rows with non-empty event signal.
- lagged context usage still held because context only appeared inside interactions whose evaluated signal stayed empty.
- conclusion: context fields need explicit atom quota, not only event/context interaction exposure.

Code follow-up:

- Phase3BP now reserves lagged context atom quota.
- Phase3CT should be run against both:
  - `phase3bp_candidate_horizon_aggregate.csv`
  - `phase3bp_top_decisions.csv`
- Aggregate audit proves generated/evaluated field use; top-decision audit proves selector-visible field use.
- Phase3CP now reads schema fields from `--shard-root` and passes them into generator arms. Unbound generation requires explicit `--allow-unbound-generation`.

## Hard Rule Going Forward

For any future large search using new sidecar/event/context/fundamental fields:

1. Run CR atom/lane audit.
2. Build CS sidecar pack.
3. Augment true1min shards with CS.
4. Run BP/CP search only on the augmented shard root.
5. Run CT lifecycle field usage audit on both candidate aggregate and top decisions.
6. Do not claim success unless CT passes or the HOLD blocker is explicitly accepted as diagnostic-only.

This prevents repeating the previous failure mode: treating field registry or schema presence as proof of real search/reward usage.
