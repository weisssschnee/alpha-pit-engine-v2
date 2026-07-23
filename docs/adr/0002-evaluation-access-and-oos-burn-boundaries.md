# ADR 0002: Evaluation Access and OOS Burn Boundaries

Date: 2026-07-11

Status: Accepted

## Context

The fixed-calendar repair prevents a trade date from belonging to multiple
splits, but temporal disjointness alone does not preserve out-of-sample status.
The 2025 validation and holdout candidate results were reported and manually
viewed, and validation influenced canary selection. Both splits are therefore
spent for future candidate choice even though the runtime optimizer used train
reward only.

Phase3CN also persisted candidate-level validation and holdout columns inside
the same feedback memory consumed by later search code. The scoring functions
did not add those values to optimizer reward, but co-location left an unsafe
feedback channel and made the boundary dependent on convention.

## Decision

- Register five data roles: development, challenge, sealed, spent, and forward.
- Assign the 364 train dates to development.
- Reclassify the 73 validation and 48 holdout dates as spent.
- Keep challenge and sealed unassigned until new untouched assets exist.
- Keep the separate 2026 asset in the forward role; do not read its performance
  during EVALRESET or hypothesis-space design.
- Permit search memory, CEM/UCB credit, and scheduler updates only from
  development rows with `optimizer_reward_split=train`.
- Physically remove candidate-level non-development fields before writing
  feedback memory.
- Fail closed if validation, holdout, challenge, sealed, forward, or OOS fields
  appear in feedback or scheduler input.
- Record every evaluation access and OOS burn in versioned ledgers.
- Validate role, access, and burn ledgers in tests. Version 1 does not intercept
  arbitrary filesystem or human reads; operators must create the ledger entry
  before controlled evaluation access.

## Consequences

- Existing Phase3CN feedback tables containing candidate-level validation or
  holdout columns are legacy evidence, not valid future scheduler input.
- Validation/holdout may be used for burn forensics or aggregate historical
  reporting, but not candidate selection, feature design, positive memory, or
  scheduler credit.
- A future challenge or sealed release requires an explicit registry update and
  access-ledger entry.
- `scripts/validate_evaluation_ledgers.py` checks registry/ledger consistency,
  but cannot prove that an out-of-band human or filesystem read was logged.
- Reporting and optimizer feedback now have separate physical schemas.

## 2026-07-23 bounded spent-holdout addendum

The user separately authorized one fixed report-only evaluation of 12 candidate
pairs frozen before this access. It reused the 48-date `holdout` split only as a
candidate non-collapse check. The project-level calendar classification remains
`spent`; this access does not relabel the dates as sealed or pristine OOS.

- The candidate list, exact behavior signatures, train hashes and access rules
  were frozen before the run.
- The run was long-only and prohibited validation/2026 reads, feedback,
  scheduler, behavior-archive and promotion writes.
- Results may classify the frozen candidates as strict non-collapse, partial or
  collapsed, but cannot authorize promotion or update later search policy.
- No additional holdout access is authorized by this addendum.
- The 2026 forward asset remains unopened and forbidden.

Runtime evidence is
`runtime/run_plans/cn_core_pack_fixed_holdout_20260723_receipt.json`.

## Evidence

- `runtime/run_plans/evaluation_data_roles_v1.json`
- `runtime/run_plans/evaluation_access_ledger_v1.csv`
- `runtime/run_plans/oos_burn_ledger_v1.csv`
- `src/our_system_phase2/services/evaluation_access_guard.py`
- `tests/test_evaluation_access_guard.py`
