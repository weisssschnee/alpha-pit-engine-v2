# ADR 0001: Fixed Global Trade-Date Splits

Date: 2026-07-11

Status: Accepted

## Context

Phase3CM previously called `_split_map()` inside each shard evaluation loop.
Every shard therefore derived train, validation, and holdout from its own
sampled timestamps. The 2026-07-10 exact atom ledger showed 73 trade dates in
more than one split across shard chunks. This breaks temporal OOS isolation
even though validation and holdout remain report-only.

Using the union of observed reward-atom dates removes overlap for one run but
still lets candidate coverage and sampling budget move the date boundaries.

## Decision

- Freeze one split manifest from the complete accepted 2024-2025 trade-day
  calendar before candidate evaluation.
- The accepted manifest contains 485 dates: 364 train, 73 validation, and 48
  holdout dates.
- All workers, final atom aggregation, and recovery tooling consume the same
  manifest and split by `trade_date`.
- A reward date missing from the fixed manifest is a hard failure.
- Train is the only optimizer/reward-feedback split. Validation and holdout
  are report-only. The separate 2026 asset remains forward OOS and is not part
  of this manifest.
- Emit the effective split manifest and reassignment audit with every final
  Phase3CM reward result.

## Consequences

- Shard-parallel execution can no longer assign the same date to multiple
  evaluation roles.
- Search/recovery results become reproducible across worker counts and shard
  scheduling for a fixed data release.
- Changing the accepted data calendar requires a new versioned manifest.
- Historical Phase3GA OOS metrics produced before this ADR are diagnostic and
  are superseded by the fixed-calendar exact recovery.

## Evidence

- `runtime/run_plans/phase3ga_true1min_2024_2025_global_split_manifest.csv`
- `reports/phase3ga_cnline2_acceptance_bundle_20260711/phase3cm_split_reassignment_audit.json`
- `tests/test_phase3cm_global_date_split.py`

