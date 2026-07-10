# Phase3GA Semantic Search Efficiency Repair

Date: 2026-07-10

Decision: `PHASE3GA_CNLINE2_SEMANTIC_AND_THROUGHPUT_CHAIN_READY_FOR_VERSIONED_REMOTE_SMOKE`

## Scope

- CNline2 / A-share true1min only.
- No crypto process, workspace, data, or tunnel was modified.
- Train/search data remains the repaired 2024-2025 true1min root.
- 2026 remains a separate forward/OOS asset and is not mixed into train.

## Root Causes Confirmed

1. The DSL admitted value-domain degeneracy such as `Sign(CSRank(x))` and redundant `Abs(CSRank(x))`.
2. Expression hashes and AST skeletons could not detect numerically equivalent signal vectors or equivalent portfolio buckets.
3. The pre-CM gate ran a full reward audit merely to check whether an expression produced rows.
4. `DataFrame.attrs` held full-panel layout arrays; pandas 3 deep-copy behavior could allocate another full array during group operations.
5. In-memory caches were bounded by entry count only. A 256-512 entry cache could retain tens of GB because every value was a full-panel Series/DataFrame.
6. Daily bootstrap Sortino used Python nested loops. On a 500-day, 600-draw benchmark the vectorized implementation was 27.26x faster.

## Implemented Controls

- Value-domain inference, algebraic rewriting, and hard blocks at candidate construction.
- Shared typed gate consults the semantic validator before field-family rules.
- Rank and portfolio-bucket signal equivalence detection before full CM.
- Constant-signal rejection with availability-mask preservation.
- Safe division denominator statistics and output tail-concentration diagnostics.
- Semantic-only CM mode: no future labels, portfolio PnL, validation reward, or holdout reward.
- Sampled numeric rejects are exact-expression observations, not permanent unsafe skeleton blocks.
- Operator and feature-matrix caches now enforce per-process byte budgets with eviction.
- NumPy-vectorized bootstrap engine `numpy_vectorized_v1`; checkpoint bootstrap defaults to 128 draws and final reward remains 600 draws.
- Semantically degenerate historical rows cannot update CN feedback or CEM/UCB policy credit.
- Atomic field/operator/window credit is normalized by formula token count; adaptive defaults are learning rate 0.30 and entropy floor 0.03.

## Evidence

```text
full local suite: 25 passed
full 77O suite before exact-recovery addition: 24 passed
semantic-only end-to-end: future-return builder replaced with a fail-fast stub; route still passed
real 121-column generation smoke:
  rx_ucb       768/768, semantic hard blocks in output: 0
  event_state  768/768, semantic hard blocks in output: 0
  orthogonal   768/768, semantic hard blocks in output: 0
  CEM          768/768, semantic hard blocks in output: 0
  hybrid       768/768, semantic hard blocks in output: 0
  elapsed      40.441 seconds
legacy DS/DT plus DU/DV audit: 43,052 generated candidates/atoms inspected, 0 semantic hard blocks
bootstrap benchmark: 4.3056s -> 0.1580s for eight workloads, 27.26x
```

## Remote Observation

The existing 77O run was not idle or dead. Three shard chunks had completed;
the final chunk reached `1152/1152` candidate-shard evaluations. Its remaining
single-core phase was the old serial bootstrap/final reduction code. That
active workspace remains unchanged; this repair must be deployed to a new
versioned workspace.

## Remaining Boundaries

- Signal-vector equivalence is a sampled pre-CM control, not alpha promotion proof.
- Safe-division tail diagnostics do not silently clip or rewrite a financial signal.
- Validation and holdout stay report-only; neither may update search policy.
- Plate/industry membership is still a separate canary boundary, not claimed as full PIT membership in the repaired 121-column root.
- Raw graphify artifacts remain at the 2026-07-05 build because `graphifyy` is not installed locally; curated architecture files are current.
