# ADR 0019: Program-level optimizer tournament

- Status: Accepted
- Date: 2026-08-13
- Scope: one prospective CN Joint Program development tournament
- Authority effect: none; Hybrid TPE plus Availability remains the formal development-search policy

## Context

The completed Search Engine V2 replacement canary returned valid negative
development evidence. Its `CONDITIONAL_UPLIFT_EXPLOIT` proposals came from a
deterministic Program reservoir and empirical template fallback, not from the
accepted Hybrid TPE plus Availability implementation. All 448 enhanced
Programs were unseen, so the program-level head was never selected; the
template-level acquisition also had coarse ties. This is
`LOCAL_REIMPLEMENTATION_DRIFT`, not evidence against mature Hybrid TPE or a
structured Program surrogate.

## Decision

1. Add one shared `ProgramSearchOptimizerAdapter` contract with ask, tell,
   snapshot, restore, proposal identity and optimizer metadata. Uniform,
   Hybrid TPE and Structured Surrogate consume the same exact Program index and
   Availability controller. Candidate composition, compiler, materializer,
   matched Full/Base control, Phase3CM evaluator, checkpoint engine, Project
   Control and Evaluation Asset authority remain the existing implementations.
2. Build a fixed-dimension Program gene representation by composing the
   existing route categorical receipts with Program template, active roles,
   topology, combination policy, clock/lag class, compiled complexity, raw
   field/rolling structure and interaction topology. Inactive role slots are
   explicit categorical values; no route gene definition is reauthored.
3. `HYBRID_TPE_PROGRAM` reuses `RouteConditionalTPESearchAdapter` and the
   official Optuna `TPESampler` with multivariate, group, constant-liar,
   Availability, real trials, transcripts and restore. Admission is a native
   constraint. Conditional uplift is a finite objective only for admitted
   whole Programs; no Full-program credit is broadcast to components. The TPE
   lane is template-conditional over frozen legal exact Program identities,
   with normalized structural-gene Hamming distance supplied to the official
   categorical model. An unavailable exact draw may project only to the
   minimum-distance entry in the full eligible legal set; first-entry and
   global fallback are forbidden and the raw-to-evaluated binding is recorded.
4. `STRUCTURED_SURROGATE_PROGRAM` uses deterministic sklearn ExtraTrees. A
   classifier estimates admission feasibility; a regressor is trained only on
   admitted conditional uplift. Frozen one-hot hierarchical encoding,
   per-tree dispersion, cold start and feasibility-times-positive-uplift UCB
   provide acquisition, ask/tell and exact snapshot/restore. Every eligible
   remaining Program is compared at each ask; `candidate_pool_size` is only an
   inference-batch bound and cannot truncate the comparison universe.
5. Freeze one prospective staged tournament: 32 Base parity records; equal
   Stage 0 and Stage 1 support for all three optimizer arms; then an automatic
   frozen Wilson one-sided 95% futility decision. Uniform always receives its
   eight-per-template Stage 2 floor. Each supported, nonfutile smart arm
   receives eight per template. Winner-takes-all and manual reallocation after
   financial reads are forbidden. Maximum budget is 536 records.
   The common prefinancial Program space is frozen to 3,616 exact entries and
   SHA256 `86d9bce8f7bdc75e55c791b6e101ec4beefe093e346abfc39c76ee1c30f354e0`;
   its raw reservoir, executable component pool and Registry source hashes are
   also bound and must replay exactly before any economic evaluation.
6. Stage 2 is part of the same Project Control admission and campaign state.
   Its decision and freeze are self-hashed, carry the Stage 1 optimizer state,
   exclude every Program exact identity already evaluated in Stage 1, and do
   not create a successor campaign.
7. The authorization is requestable only through the target-bound
   `cn-program-optimizer-tournament-v1` Project Control route and canonical 77o
   wrapper. This decision creates no admission and starts no tournament.
   Validation, holdout, spent-2023, Forward-B and 2026 remain denied; promotion
   and automatic successor are forbidden.
8. The old 512 artifacts may support disclosed retrospective diagnosis only.
   New optimizer state imports zero financial observations, serialized state,
   candidate results, factor statistics, behavior statistics or template
   classifications.

## Consequences

- The implementation is an `EXPERIMENTAL` candidate for the existing
  development-search authority; static and synthetic tests make it ready to
  request one prospective tournament but provide no economic qualification.
- Search V2's negative result and frozen artifacts remain immutable.
- The formal Hybrid TPE plus Availability authority is neither replaced nor
  reopened by this experiment.

## Rollback

Remove the experimental route, adapters, freeze and tournament plan while
retaining this decision and Git history. Rollback cannot restore the empirical
median scheduler as authority, scalarize the two economic heads, import old
financial state, weaken Project Control or promote an optimizer.
