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

## 2026-07-29 aggregate route-design addendum

The user separately authorized one report-only validation of a cohort frozen
at 128 Hybrid and 128 Uniform pairs, followed by one offline route-budget
decision. This is a narrow exception for aggregate route contraction, not
candidate selection or adaptive search feedback.

- The report did not reopen the accepted `HYBRID_TPE_AVAILABILITY` policy.
- Validation reads were positive, while feedback, scheduler, archive,
  promotion, holdout and 2026 writes or reads remained forbidden.
- Only arm-by-route aggregate transfer, score-tail and blocked-pair summaries
  may inform the next fixed bounded route budget.
- Candidate identities, candidate-level validation values and ranks cannot
  enter reward, Optuna observations or tells, persistent memory, archive
  credit, promotion, or a within-tranche scheduler.
- Once the aggregate report was used for route contraction, it became
  `SPENT_AGGREGATE_ROUTE_DESIGN_ONLY_NOT_PROMOTION_EVIDENCE`.
- This addendum authorizes no further validation access and no unlimited or
  20,000-target search.

Runtime evidence is
`runtime/run_plans/cn_hybrid_bounded_large_tranche_p07_20260729_receipt.json`.

## 2026-08-05 one-shot forward-confirmation addendum

The user explicitly authorized spending the sole unopened 2026 forward asset
for one intention-to-treat confirmation of the already frozen ten-pair cohort.
This is a destructive evidence-boundary transition, not a new search,
selector fit, or promotion decision.

- The cohort is fixed at ten pairs / twenty members in original finalist order
  `2/6/7/8/9/10/11/15/18/21`, selection payload SHA256
  `7cfc2e454da7ae7561b57979db8010324422cd87ca3eef42167809400d59ef77`.
- The forward calendar is fixed at the 63 sessions from 2026-01-05 through
  2026-04-10. No replacement, backfill, post-freeze filtering or tuning is
  permitted after forward values are opened.
- The execution policy remains `TOPK_10_EQUAL`, prior-close signal,
  next-open execution, A-share T+1, frozen fees and final-close MTM without a
  fabricated terminal sale.
- The run is report-only. Validation and holdout reads, optimizer/search
  feedback, scheduler, archive and automatic promotion writes remain
  forbidden.
- The forward asset becomes project-level `spent` on the first financial row
  read regardless of success, failure or economic outcome. A failed run may
  be recovered only from a pre-read boundary or from immutable artifacts that
  do not change the cohort, data, policy or result.
- Any positive result remains `HOLD_PROMOTION` until a separate explicit
  authority decision; this addendum itself authorizes no live deployment.

The pre-read authorization contract is
`runtime/run_plans/cn_fixed10_forward_2026_one_shot_authorization.json`.

The authorized attempt opened forward row coordinates on 2026-08-05 while
constructing the field-sidecar allowed-code universe, then failed at the legacy
chip-context 2026 guard before any candidate or pair result. The 2026 asset is
therefore `spent`, confirmation economics are unavailable, retry is forbidden
and promotion remains held. Commit
`635ace825e6f2b1b5e884f3b0cfb9eeb4acad5b0` moves explicit forward-chip role
qualification ahead of future forward source scanning; this remediation does
not reopen the spent asset. The outcome receipt is
`runtime/run_plans/cn_fixed10_forward_2026_spent_failure_receipt.json`.

## Evidence

- `runtime/run_plans/evaluation_data_roles_v1.json`
- `runtime/run_plans/evaluation_access_ledger_v1.csv`
- `runtime/run_plans/oos_burn_ledger_v1.csv`
- `src/our_system_phase2/services/evaluation_access_guard.py`
- `tests/test_evaluation_access_guard.py`
