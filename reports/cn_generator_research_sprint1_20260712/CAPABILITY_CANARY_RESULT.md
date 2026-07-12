# CN Generator Research Sprint-1 — Capability CANARY Result

Status: `COMPLETED_WITH_NATURAL_UNDERFILL_REQUIRES_GENERATOR_REPAIR`

Evidence identity:

- Frozen repo SHA/tag target: `7c7e08b0855136a3b6aa61cd216c10790c8ad04b`
- Remote run: `D:\ChengboRemote\runtime\cn_generator_research_sprint1\capability_canary\7c7e08b_r5`
- Runtime: `460.967` seconds on 77o
- Candidate pack SHA256: `f0b4e4def1e199802aa834357cbec70ca79d5d3adb5a0a0d8b57280856e7a9c1`
- Result role: development diagnostic only; no candidate promotion.

## Boundary result

The run read the 16-file development-only release for the fixed 2025-04-01 session. The read ledger recorded:

- forbidden file opens: `0`;
- forbidden row-group reads: `0`;
- validation rows: `0`;
- holdout rows: `0`;
- forward rows: `0`;
- selected rows: `34,704`;
- release hash: `cfb2742d975f2f6f1dcdf78d011f6d471b8d0e444164bae1d1816ba1fdcc5827`.

`FORWARD_2026_SEALED`, `NO_CANDIDATE_PROMOTION`, and `NO_CROSS_SPRINT_ADAPTIVE_MEMORY` remained true.

## Funnel outcome

| Measure | Capability CANARY | Prior formal B1S |
|---|---:|---:|
| Proposals | 4,400 | 1,344 |
| Legal | 4,336 | 1,187 |
| Legal exact identities | 3,684 | 1,094 |
| Exact identity rate | 83.7% | 81.4% |
| Development survivors | 1,716 | 863 |
| Strict candidates | 137 | 64 |

The strict pack naturally underfilled the frozen budget of 188 because the hard benchmark gate and one-cluster-one-vote Pareto admission rejected or deduplicated candidates. It still stayed inside the authorized Capability range of 128–256 strict evaluations.

## Generator outcome

- Temporal expanded materially: 192 survivor clusters, of which 184 (`95.8%`) were outside static. Prior temporal/event/state combined added 80 clusters.
- FirstN produced 69 clusters; orthogonal/exile 63; typed-random 51; typed-AST 27.
- RX/UCB produced 84 clusters and evolutionary 80, demonstrating that the mechanism grammar is broader than the old generic adaptive grammar.
- Static produced 33 clusters. Its top cluster share was `16.4%`, so it remained more concentrated than temporal (`2.0%`).
- CEM and surrogate stayed narrow: 10 and 9 clusters, with top cluster shares `33.9%` and `49.5%`. Their control-only downgrade remains justified.

Event and state produced zero survivors. This is a capability failure, not negative alpha evidence:

- event rows had no eligible cross-sectional IC and at most 12 unique values;
- state rows had at most one unique value;
- grammar referenced five fields not actually materialized by the release;
- a label-free coverage audit over all 364 row-group-0 development dates found the best date had only three event-active stocks and at most one active stock in any cross-section;
- daily context state fields had only one within-date value and therefore cannot be treated as intraday transitions.

The event coverage audit selected 2024-04-29 solely by activation/coverage, read no label or return columns, and still proved the available event panel is too sparse for the current cross-sectional event/state contract.

## Benchmark and strict evidence

At proxy stage:

- evolutionary median increment over the simple benchmark median: `+0.02369`;
- typed-AST: `+0.01469`;
- RX/UCB: `-0.00163`;
- CEM, UCT/MCTS, surrogate, and the current LLM pack were negative.

At strict H5 stage, mean evidence by lane was:

| Lane | Absolute IC | Positive LCB95 | Cost-adjusted IC | Turnover |
|---|---:|---:|---:|---:|
| Benchmark | 0.1749 | 0.1484 | 0.1619 | 0.4448 |
| Temporal | 0.1794 | 0.1591 | 0.1707 | 0.7048 |
| Orthogonal/exile | 0.1848 | 0.1588 | 0.1723 | 0.6777 |
| Evolutionary | 0.1914 | 0.1715 | 0.1828 | 0.7795 |
| RX/UCB | 0.1376 | 0.1029 | 0.1147 | 0.5327 |

Temporal, orthogonal, and evolutionary improved quality and positive lower-confidence evidence, but turnover worsened materially. Epoch-A cannot be justified until the objective and admission explicitly penalize that cost.

## Admission outcome

- Global top-K admitted 406 clusters; median proxy reward `0.05468`.
- Hybrid admitted 410; median `0.05307`.
- Stratified admitted 373; median `0.05710`.
- Pareto hybrid admitted 326; median `0.06111`, mean `0.08098`.

Pareto raised median proxy quality by `11.8%` relative to global top-K while enforcing benchmark and cluster gates. It underfilled naturally and allocated 102 of 326 slots to temporal (`31.3%`), so family/lane concentration still needs a harder cap before a large epoch.

## Adaptive outcome

Evolutionary was the only adaptive lane to beat its matched control, with median reward delta `+0.00079`; it also beat the benchmark at proxy and strict stages, but with high turnover.

RX/UCB failed its matched control (`-0.00570`). Diagnosis shows the implementation ranked hypothesis arms but then cycled nearly uniformly through them, so information-gain scores did not actually control proposal allocation. CEM, UCT/MCTS, and surrogate also lost to controls and remain low-budget controls.

## Decision

Do not enter Epoch-A yet. Repair only the defects identified by this run:

1. constrain all generators to truly materialized raw fields;
2. stop treating daily context values as intraday transitions;
3. mark event/state cross-sectional search data-limited instead of lowering quality gates;
4. make RX/UCB allocate budget to positive benchmark-increment hypothesis arms rather than cycling all arms;
5. enforce turnover/cost and lane concentration in Pareto admission;
6. correct the bottleneck reporter, which still incorrectly says no cost model and one stability window despite the new strict fields.

Then repeat a bounded Capability CANARY. Only a repaired run can authorize Epoch-A.
