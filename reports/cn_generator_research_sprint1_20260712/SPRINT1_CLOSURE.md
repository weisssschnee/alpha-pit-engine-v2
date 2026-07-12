# CN Generator Research Sprint-1 Closure

Updated: 2026-07-13

Status: `CN_GENERATOR_RESEARCH_SPRINT1_PARTIALLY_COMPLETED`

Recommendation: `CONTINUE_GENERATOR_RESEARCH_SPRINT`

## Delivered outcome

Sprint-1 replaced the generic lane-labelled grammar with distinct static,
firstN, temporal, event, state and orthogonal generators; added hypothesis-arm
RX/UCB, typed evolutionary lineage, hard gates, Pareto admission, cost-adjusted
quality, four-block stability and strict lower-confidence metrics; and executed
the authorized development-only Capability CANARY plus a two-seed Epoch-A.

Epoch-A ran at repo SHA `09a4d80df9bfaafef054b43595269ac70f40174f`:

- 16,384 proposals and 512 strict evaluations across two frozen seeds;
- 8,108 / 8,099 legal and 5,704 / 5,692 exact identities;
- 5,593 / 5,616 development survivors;
- 224 / 219 clusters outside static, almost entirely temporal;
- zero validation, holdout or forward reads and zero persistent memory writes;
- two 256-candidate research packs, not authorized for promotion or forward use.

## What improved

- RX/UCB beat its matched control on both seeds at proxy level and in the
  strict H5 adaptive subgroup. Its H5 adaptive abs-IC was 0.150975 versus
  0.145686 on seed A and 0.180883 versus 0.151398 on seed B, with lower
  turnover than control on both seeds.
- Pareto admission raised median proxy reward from 0.051570 to 0.063079 on
  seed A and from 0.050912 to 0.062477 on seed B.
- Temporal programs produced 223 and 217 clusters outside static. State added
  four and five; event remained empty because the frozen session lacks useful
  cross-sectional event coverage.
- Pack balance remained bounded: maximum lane share 21.88% and maximum
  primitive share 11.72%.
- The 69 exact identities shared by both packs reproduced H5 signed IC exactly
  on the same frozen development coordinates.

## What did not clear the sprint gate

- New clusters per strict evaluation fell to 0.875 and 0.855 in Epoch-A from
  about 1.40 in the repaired Capability run. Larger proposal budgets therefore
  did not improve discovery efficiency.
- Evolutionary adaptive children were worse than their matched controls on
  both seeds at strict H5. Evolutionary is downgraded to a control lane.
- RX/UCB improved over its controls but did not make the entire RX lane
  consistently beat the simple benchmark after strict selection.
- Event discovery remained zero, and state evidence remained too sparse for
  formal strict allocation.
- Cross-seed pack exact overlap was only 69/256 (Jaccard 0.1558). Mechanism
  balance is reproducible, but candidate-level discovery is still seed-sensitive.

## Fixed decision

Keep RX/UCB as the primary adaptive challenger. Retain frozen pre-reward LLM
hypothesis construction and typed AST as mechanism generators. Downgrade
evolutionary, CEM, UCT/MCTS and surrogate to matched-control roles. A future
sprint should improve generator semantic volume and broaden development
coordinates before spending another large search budget.

No candidate pack is prepared for forward authorization in this closure.
`FORWARD_2026_SEALED`, `NO_CANDIDATE_PROMOTION`, and
`NO_CROSS_SPRINT_ADAPTIVE_MEMORY` remain in force.
