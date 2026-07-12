# CN Generator Research Sprint-1 — Frozen B1S Funnel Diagnosis

Status: `COMPLETED_DEVELOPMENT_ONLY_DIAGNOSTIC`

Evidence identity:

- Code SHA: `35115e7ac1a2d5249c29ccdfac64dc82d55cb95a`
- Experiment: `20260712_cn_generator_sprint1_funnel_005`
- Machine artifact SHA256: `596376900cd5c40cf60b003faa055e79aa27e1985d313d4f991f5b954c88b16d`
- Remote run root: `D:\ChengboRemote\runtime\cn_generator_research_sprint1\diagnostics\35115e7_r1`
- Targeted tests on 77o: `7 passed`
- Data access: frozen formal B1S artifacts only; no raw data, validation, holdout, or 2026 access.

## Funnel result

The formal funnel is internally consistent when stages are kept separate:

| Stage | Rows | Exact identities | Signal clusters |
|---|---:|---:|---:|
| Proposal | 1,344 | 1,240 | n/a |
| Legal | 1,187 | 1,094 | n/a |
| Materialized | 1,122 | 1,031 | 278 |
| Survivor | 863 | 813 | 242 |
| Strict | 64 | 64 candidates | n/a |

All 15 lane-level legal identity counts and survivor cluster counts reproduce the frozen `lane_funnel.csv` exactly.

## What generated useful breadth

The strongest evidence of genuinely distinct hypothesis production came from:

- `temporal_program`: 52 survivor clusters, 47 exclusive against all other lanes, 40.6 survivor clusters per 100 proposals, median proxy increment `+0.0306` over the benchmark median, and strict median absolute H5 IC `0.1935`.
- `static_cross_sectional`: 47 survivor clusters, 42 exclusive, 36.7 clusters per 100 proposals, proxy increment `+0.0260`, strict median absolute H5 IC `0.2411`.
- `state_transition`: only 12 survivor clusters, but 11 were exclusive and proxy increment was `+0.0518`; low survivor conversion (`18.8%`) makes it promising but under-developed rather than proven.
- `firstn_intraday_path`: 28 survivor clusters, 16 exclusive, strict median absolute H5 IC `0.2069`; however proxy increment was `-0.0077` and median turnover was `0.822`, so the objective currently underprices its implementation cost and instability.

`llm_proposal_repair` produced 28 survivor clusters and all 28 were lane-exclusive with proxy increment `+0.0209`, but only one candidate reached strict evaluation. It is evidence of proposal breadth, not evidence of strict superiority.

## What did not generate enough new hypotheses

- `typed_random`: 7 survivor clusters and zero lane-exclusive clusters from 96 proposals.
- `typed_ast`: 8 survivor clusters from 96 proposals.
- `cem`, `rx_ucb`, `uct_mcts`, `evolutionary`, and `surrogate`: only 8–13 survivor clusters per lane despite 64–96 proposals each. CEM, RX/UCB, evolutionary, and surrogate also had top-cluster share above 20%.
- `orthogonal_exile`: 6 survivor clusters and only one exclusive cluster.
- `event_conditioned`: 20 survivor clusters, only five exclusive, and proxy increment `-0.0112`.

This confirms a generator-design bottleneck, not a general reward-evaluator bottleneck. The frozen run recorded proxy-to-strict absolute-IC correlation `0.9862`, while nine lanes failed to beat the simple benchmark median at proxy stage.

## Admission result

All three methods preserved one exact identity per signal cluster, but they allocated opportunity differently:

- Global top-K admitted 168 clusters. It concentrated 43 in temporal and 24 in static; temporal alone consumed `25.6%` of the budget.
- Stratified admission admitted 129 clusters with natural quota underfill, covered all 15 lanes, and had a higher median proxy reward (`0.0695`) than global top-K (`0.0627`).
- Hybrid admitted 168 clusters and covered all lanes, but still assigned 38 to temporal and 24 to static. Its median proxy reward was identical to global top-K (`0.0627`).

Therefore hybrid currently softens concentration but does not yet create a meaningful quality/diversity frontier. The next implementation needs lane/family hard floors, novelty-aware Pareto selection, and explicit natural-underfill reporting rather than another scalar top-K variant.

## Adaptive algorithms

Only RX/UCB and evolutionary beat their matched non-adaptive controls; CEM, UCT/MCTS, and surrogate did not. Yet RX/UCB produced only 8 survivor clusters and evolutionary 13, both with top-cluster share above 20%. Their advantage is therefore selection inside a narrow grammar, not expansion of the hypothesis space.

Sprint-1 should refocus:

- RX/UCB arms on typed hypothesis cells and information gain, not token/motif reuse.
- Evolutionary search on explicit parent/mutation lineage and benchmark-delta Pareto survival.
- CEM/MCTS/surrogate remain controls until they demonstrate new-cluster yield.

## Objective defects that must be fixed before Capability CANARY

The frozen strict artifacts cannot measure:

- cost-adjusted strict quality;
- IC uncertainty or a valid confidence bound;
- worst time-block quality;
- per-lane runtime attribution;
- portfolio mapping and net benchmark increment.

The next objective must use hard legality/PIT/coverage/stability gates, then Pareto rank quality, worst-block stability, turnover/cost, novelty, and compute cost. A scalar score may only break bounded ties; it must not erase these dimensions.

## Decision

Proceed to generator and objective redesign. Do not run Capability CANARY until the distinct-lane contracts, RX/UCB/evolutionary redesign, Pareto admission, and the five missing evidence fields are tested and frozen.
