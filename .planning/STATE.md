# CN true1min State

Updated: 2026-07-12

Current state: `CN_GENERATOR_RESEARCH_SPRINT1_ACTIVE`

## Accepted foundations

- `PHASE_A_EVALRESET_ACCEPTED`
- `NEXTGEN_DARK_INFRASTRUCTURE_READY`
- `CN_B1S_CANARY_COMPLETED_WITH_NATURAL_UNDERFILL`
- `FORMAL_SEARCH_FROZEN`
- `FORWARD_2026_SEALED`
- `NO_CANDIDATE_PROMOTION`
- `NO_CROSS_SPRINT_ADAPTIVE_MEMORY`

The accepted Phase A signal-sketch evidence remains immutable. Structural
generation redundancy was confirmed; catastrophic signal-level collapse and
downstream concentration amplification were not observed.

## Formal B1S CANARY closure

The formal development-only CANARY completed at repo SHA
`80684b8afa8c64a82853888db51abcf4cdfb721c`, and the remote branch and tag
were verified at that exact SHA.

- physical release: 16 shards, 446,443,583 rows, 576 development-only row groups
- release hash: `cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827`
- read ledger: zero forbidden opens and zero validation/holdout/forward rows
- tests: 213 passed on 77o
- funnel: 1,344 proposals -> 1,187 legal -> 1,094 exact identities ->
  279 clusters -> 863 development survivors -> 64 strict candidates
- candidate pack SHA:
  `7b941185ebf3447ad34a8ad110333a6009fdaa4c6126a6526695b7681b6e837c`
- temporal/event/state contributed 80 clusters outside static
- RX/UCB and evolutionary beat matched controls; CEM, UCT/MCTS and surrogate did not
- primary diagnosed bottleneck: generator

The earlier mixed-role CANARY remains `DIAGNOSTIC_ONLY`,
`INVALIDATED_BY_MIXED_ROLE_PHYSICAL_READ`, and
`NOT_VALID_FOR_RESEARCH_DECISION`.

## Active Sprint-1 boundary

Sprint-1 may run at most three development-only rounds: Generator Capability
CANARY, conditional frozen Epoch-A, and conditional Epoch-B. Each round must
freeze repo SHA, release, seeds, budgets, grammar, objective, admission,
benchmark and candidate contract. Automatic continuation is allowed only after
zero access violations, zero contract drift and complete prior-round closure.

Plate/industry remains user-deferred and disabled. Validation, holdout, spent,
sealed and 2026 data cannot enter reward, search policy, scheduler, family
decisions or memory.

## Next formal decision point

Complete the formal CANARY funnel diagnosis, implement distinct generators and
the hard-gate/Pareto/limited-scalar development objective, then freeze the
Generator Capability CANARY. Heavy computation runs only on 77o.
