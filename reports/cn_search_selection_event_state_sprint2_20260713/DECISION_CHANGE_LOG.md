# CN Search Selection and Event-State Repair Sprint-2 Decision Log

Updated: 2026-07-13

## Fixed decisions

- Existing development survivor was reclassified as `DEVELOPMENT_ELIGIBLE`.
- `STRICT_PRIORITY_ELIGIBLE` is the frozen 10% pre-strict quality layer; its model is offline, group-aware and cross-fitted, not cross-sprint adaptive memory.
- Broad Event capability was not evaluated. The historical generator used one
  cutoff-latched trigger, the Event audit applied invalid lifecycle semantics,
  and Epoch-C assigned zero Event proposal, admission and strict budget.
- State generation is operational for non-degenerate behaviour discovery, but does not show matched-static economic increment in Epoch-C.
- RX/UCB is the only primary ephemeral adaptive challenger. CEM, UCT/MCTS, evolutionary and surrogate remain controls.
- Epoch-C is development-only. Validation, holdout, spent, sealed and 2026 forward remain unread.

## Executed evidence

- Repair Capability CANARY: 8,192 proposal rows, 224 strict evaluations after event-lane natural underfill, two seeds, zero forbidden access.
- Epoch-C: 49,152 execution rows across three seeds, 1,024 strict evaluations, zero forbidden access.
- Shared deterministic backbone: 8,192 / 8,192 exact identities; pairwise development and strict-priority rank correlations are 1.0.
- RX/UCB strict matched-control gate passed in all three seeds.
- Strict-priority selector beat the current scalar at realized top-decile strict selection in all three seeds.
- Temporal produced 250 new clusters per seed and state produced 465 new
  clusters per seed. Event recorded zero clusters because its budget was zero
  and the lane was not executed.
- Strict-pack exact union is 377; three-way intersection is 320 and three-way Jaccard is 0.8488.

## Defect and disposition

Epoch-C generated 32,739 globally unique exact identities instead of the frozen
32,768 because 29 RX adaptive identities overlapped across seed expansions.
All 1,024 strict evaluations completed, but the unique-proposal contract was
not exactly met. A seed-namespace guard and three-seed regression test now
prevent recurrence. The run is not relabeled or silently repaired.

Epoch-D remains unrun: state did not beat its matched static group
economically, while broad Event capability was not evaluated. The 29-identity
overlap is not evidence of insufficient mechanism sampling and does not
justify another performance run.

## Event interpretation supersession

The earlier broad Event denial was an invalid extrapolation caused jointly by
trigger scope, field semantics, audit semantics and zero execution budget:

- the generator used only `Transition($evt_uplimit_active,0,1)`;
- `evt_uplimit_active` is latched after its cutoff through the close, not a
  real-time sealed-board state;
- requiring that field to exhibit 1-to-0 exits, board breaks or reseals was a
  semantic error;
- Epoch-C event-conditioned proposal, admission and strict budgets were all
  zero.

The active statuses are now:

- `BROAD_EVENT_CAPABILITY_NOT_EVALUATED`;
- `CURRENT_EVENT_TRIGGER_SEMANTICS_MISMATCH`;
- `EVENT_AUDIT_CONTRACT_INVALID`;
- `EVENT_LANE_ZERO_BUDGET_NOT_EXECUTED`;
- `EVENT_DATA_NOT_INVALIDATED`.

Historical proposal, strict and performance artifacts are not rewritten. This
section supersedes only their Event interpretation. Sprint-2 selector, RX/UCB,
temporal, state and shared-backbone conclusions remain unchanged.

## Closure

- Status: `CN_SEARCH_SELECTION_EVENT_STATE_SPRINT2_PARTIALLY_COMPLETED`
- Recommendation: `CONTINUE_SEARCH_SELECTION_AND_GENERATOR_RESEARCH`
- `FORWARD_2026_SEALED`
- `NO_CANDIDATE_PROMOTION`
- `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`
- Broad Event recovery authorized; implementation and CANARY not started
