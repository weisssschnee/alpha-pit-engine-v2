# ADR 0018: Search Engine V2 two-head prospective canary

- Status: Accepted
- Date: 2026-08-11
- Scope: CN Joint Program development search-control experiment
- Authority effect: none; Hybrid TPE plus Availability remains the formal development-search policy

## Context

Phase C showed that a program can have positive Full-vs-Base uplift while its
standalone economics, stability, turnover efficiency and execution survival
degrade. The absolute-first allocator repaired much of the standalone side,
but the closed Phase D evidence then showed weaker matched-return attribution,
weaker turnover efficiency and insufficient template breadth. Those immutable
outcomes are accepted design motivation. They are not imported observations or
optimizer state for the new campaign and are not reinterpreted as Search V2
results.

A single scalar reward cannot honestly distinguish these two questions:

1. Is the Full program itself economically admissible for continued research?
2. Conditional on that admission, did its enhancer add value relative to the
   exact Base control?

## Decision

1. Search Engine V2 is an `EXPERIMENTAL` search-control implementation with two
   independent heads. `AbsoluteEconomicAdmission` is a gate over authoritative
   Joint Program evaluator fields: complete executable replay, exact Full/Base
   identity, valid control/compile/DAG state, no candidate-local blocker,
   positive primary net reward and cumulative return, two-of-three positive
   development windows, positive net return per turnover and nonzero fill/window
   coverage. It does not read matched uplift and emits no optimizer credit.
2. Only an admitted Full program may produce conditional uplift credit. That
   credit uses the existing exact Full-vs-Base matched-control result: matched
   reward and return increments, aligned-window consistency, median and lower
   tail window uplift, and turnover differential. A failed admission produces
   `enhancer_credit = NONE`, not a large negative enhancer reward.
3. Conditional credit is authoritative only at program/template level. Joint
   uplift is not divided among Temporal, Market or Event components. Component
   statistics remain proposal priors or diagnostics and are labelled
   `COMPONENT_ATTRIBUTION_UNIDENTIFIED` unless a later separately authorized
   identifiable ablation design exists.
4. The campaign-local scheduler maintains separate empirical admission and
   conditional-uplift heads. Conditional observations are written only for
   admitted records. It uses no scalar absolute-plus-uplift reward and has no
   API for cross-campaign optimizer-state import. Restore is self-hashed and
   restricted to the same campaign id.
5. The prospective canary is frozen at exactly 512 asks: 64 for each of the
   eight existing templates. `BASE` is 64 `UNIFORM_FRESH`; each of seven
   enhanced templates is 32 `UNIFORM_FRESH`, 24
   `CONDITIONAL_UPLIFT_EXPLOIT` and 8 `NOVELTY_RESERVE`. Every eight-record
   checkpoint is template-local with the enhanced shape 4/3/1. There is no
   adaptive budget reallocation, spillover, early template cancellation or
   result-dependent schedule change.
6. Initial state is genuinely fresh:
   `serialized_optimizer_state_imported=false`,
   `development_financial_observations_imported=false`,
   `development_observation_count=0`, candidate/factor/behavior/template result
   imports are false. The design honestly records
   `manual_diagnosis_imported=true`,
   `objective_designed_after_parent_results=true` and
   `cross_campaign_development_feedback=true`.
7. Prospective success requires both strict zero-margin absolute
   noninferiority and conditional-uplift superiority with frozen support and
   template-breadth requirements. Neither head may compensate for failure of
   the other. Insufficient admitted support fails closed. A deterministic
   gate evaluator consumes one manifest-bound feedback byte buffer and writes
   the dual PASS/FAIL result into the final V2 closure; the shared engine uses
   a separate internal closure name, so no public V2 completion artifact
   exists before this decision. These gates cannot be changed after any canary
   financial observation.
8. The new route reuses Candidate Program V1, route-local proposal generation,
   compiler, exact matched control, Phase3CM evaluator and checkpoint engine.
   It does not create another evaluator, replay, materializer or candidate
   executor. Existing Hybrid TPE plus Availability remains formal authority;
   Search V2 is only a candidate for that authority.
9. Any future financial launch must enter `app.py` through
   `cn-joint-program-search-v2-canary`, consume a target-bound Project Control
   admission, bind the exact self-hashed campaign authorization, run id,
   checkout SHA and output root, and use the existing recovery lineage rules.
   Direct runner invocation is forbidden. The current implementation task
   creates no Project Control admission and does not run the canary.
10. `evaluation_asset_authority.py` remains the evaluation boundary. Search V2
    verifies that 2023 is spent, Forward-B is sealed and validation/holdout are
    default-denied before the financial runner. Search V2 owns no promotion or
    sealed-data authority. The future freeze binds execution contract, train
    price, train field manifest, registry and node capacity back to the
    accepted immutable Phase B input binding; caller-selected development-like
    assets cannot silently replace those inputs.

## Consequences

- `strong Base + useless enhancer` can pass admission while receiving zero or
  negative conditional program credit.
- `bad standalone + strong relative uplift` fails admission and produces no
  enhancer optimizer credit.
- The canary design is adaptive in motivation but prospective and fresh in
  execution state; it cannot claim independent theoretical origin.
- Phase B/C/D artifacts and conclusions remain immutable. The factorized and
  revised allocators remain held experimental predecessors, not reusable V2
  initializers.
- Static and synthetic tests establish implementation and boundary behavior;
  they are not runtime, alpha, OOS or economic evidence.

## Rollback

The experimental route, plan and scheduler may be removed while preserving
this decision and Git history. Rollback cannot restore scalar mixed reward,
import parent financial observations, reopen spent/sealed assets, weaken
Project Control, or promote either predecessor allocator. Any replacement must
retain the separate admission and conditional-uplift domains or supersede this
ADR explicitly.
