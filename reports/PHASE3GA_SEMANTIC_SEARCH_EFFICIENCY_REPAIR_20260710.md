# Phase3GA Semantic Search Efficiency Repair

Date: 2026-07-10

Decision: `PHASE3GA_CNLINE2_SEMANTIC_AND_THROUGHPUT_CHAIN_REMOTE_ACCEPTED_WITH_EXACT_12_SHARD_AUDIT`

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

## Exact 12-Shard Recovery Audit

The old run completed three shard chunks. Its fourth chunk stopped after an
incremental checkpoint and emitted no exact reward atoms. Phase3GA recovered
only the missing candidate/shard cells and then rebuilt the reward table from
the exact atom ledger; it did not replay the three completed chunks.

```text
candidate_count:                    384
expected_shard_count:                12
exact_reward_atom_count:      1,532,890
coverage_failure_count:               0
semantic_blocked_candidates:         60  (15.625%)
semantically_valid_candidates:      324
semantic_equivalent_candidates:       0  after canonical quarantine
train_followup_candidates:           11
```

The two highest optimizer rewards were not stable alpha promotions:

```text
phase3cp_00007: train 0.5534, validation 0.2158, holdout -0.1196
phase3cp_00006: train 0.5090, validation 0.1530, holdout -0.1670
```

Sixteen of the 324 semantically valid candidates had positive train,
validation, and holdout Sortino. Only one of the 11 train-followup candidates
also had all three signs positive:

```text
phase3cp_23755: train 0.1510, validation 0.4061, holdout 0.0692
```

This is a follow-up canary, not an official alpha. The strongest minimum
three-split Sortino belonged to `phase3cp_24219` (train 0.1314, validation
0.9317, holdout 0.2588), but it remained `HOLD_TRAIN_REWARD`. The exact audit
therefore confirms that optimizer reward is a search-budget signal and cannot
replace promotion gates.

Remote accepted workspace:

```text
D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260710_phase3ga_semantic_efficiency_v2
```

Exact recovered output:

```text
D:\ChengboRemote\runtime\phase3fix_repaired_2y_train75_large_search_20260710_77o_scheduled_w4s12\phase3cm_train_reward_exact_recovered_phase3ga
```

## Why The Old Workers Died

The failure was real allocation exhaustion, not a mysterious normal exit. The
old worker logs contain NumPy `_ArrayMemoryError` failures while requesting
only 31.6 MiB, 56.8 MiB, and 120 MiB arrays. On inspection the machine had
about 95.6 GiB physical RAM and manually configured page files totalling about
300 GiB; the D: page file had reached a recorded peak near 238 GiB.

This was aggregate commit pressure created by concurrent full-panel arrays,
pandas deep copies through `DataFrame.attrs`, and entry-count-only caches. A
larger page file can delay the crash but is not the primary repair. Phase3GA
removes full-panel arrays from attrs and enforces byte-bounded operator and
feature caches.

## Remaining Boundaries

- Signal-vector equivalence is a sampled pre-CM control, not alpha promotion proof.
- Safe-division tail diagnostics do not silently clip or rewrite a financial signal.
- Validation and holdout stay report-only; neither may update search policy.
- Plate/industry membership is still a separate canary boundary, not claimed as full PIT membership in the repaired 121-column root.
- State-like AST subtrees are now checked for known degeneracy, but they are not yet promoted to independently versioned and evaluated State objects.
- Formula-token credit is now less aggressive, but true subtree marginal-credit or ablation attribution remains future work.
- Raw graphify artifacts remain at the 2026-07-05 build because `graphifyy` is not installed locally; curated architecture files are current.
