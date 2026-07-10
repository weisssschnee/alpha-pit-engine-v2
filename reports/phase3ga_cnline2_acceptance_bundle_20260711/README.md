# CNline2 Phase3GA Acceptance Bundle

Date: 2026-07-11

Decision: `CNLINE2_PHASE3GA_FIXED_CALENDAR_ACCEPTANCE_BUNDLE_READY`

## Architecture

The maintained architecture view is
[`../../../.planning/graphs/CURRENT_ARCHITECTURE.md`](../../../.planning/graphs/CURRENT_ARCHITECTURE.md).
Raw `graph.json`/`graph.html` remain navigation artifacts and are not the
runtime authority.

## Field Registry

`cnline2_true1min_121_field_registry.csv` is generated from the accepted
parquet schema plus the Phase3CS sidecar contract.

```text
metadata/key:           7  (never formula inputs)
raw true1min:          12
firstN opening state:  30  (5/15/30-bar maturity sampled and verified)
lagged context:        59  (source_date < exec_date; coverage guarded)
event/state:           13  (typed event route only; cutoff minute is metadata)
total:                121
plate membership:       0  (not claimed)
```

Old 1D data is forbidden. The registry describes the repaired 2024-2025
true1min root; it does not imply every field may enter every primitive.

## Train / Validation / OOS

The fixed full-calendar manifest contains 485 trade dates:

| role | dates | observed boundary | optimizer usage |
|---|---:|---|---|
| train | 364 | 2024-01-02 through 2025-07-07 | allowed |
| validation | 73 | 2025-07-08 through 2025-10-24 | report only |
| holdout / internal OOS | 48 | 2025-10-27 through 2025-12-31 | report only |
| 2026 forward OOS | separate asset | not evaluated in this run | forbidden during search |

Artifact publication exposed a historical shard-local split bug. Before
normalization, 73 dates appeared in multiple splits and 138,465 of 1,532,890
reward atoms required reassignment. The fixed manifest covers every observed
atom date; post-normalization overlap and unassigned rows are both zero.

## Exact Reward Result

- 384 input candidates across 12 shards; coverage failures: 0.
- 60 semantic-degenerate candidates quarantined; 324 remain.
- 12 train-followup candidates, but only one has positive train, validation,
  and holdout Sortino: `phase3cp_23755` (0.1518 / 0.6762 / 0.0280).
- This is a follow-up canary, not an official alpha promotion.

## Files

- `cnline2_true1min_121_field_registry.csv`: row-level schema and search policy.
- `cnline2_true1min_121_field_registry_summary.json`: counts and schema digest.
- `phase3cm_fixed_calendar_split_manifest.csv`: effective 485-date split.
- `phase3cm_split_reassignment_audit.json`: overlap and relabel evidence.
- `phase3cm_exact_fixed_calendar_summary.json`: exact 12-shard reward summary.

External parquet shards and caches are intentionally not committed.

