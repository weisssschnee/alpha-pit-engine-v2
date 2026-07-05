# Phase3EP Medium Reward-Gated True1min Search Runbook

Date: 2026-07-05

## Purpose

Phase3EP is the next medium-scale true-1min search after Phase3EO semantic viability gating.
It is not a production promotion run. It tests whether a larger fresh search can produce
nonzero, tradable train-reward candidates after semantic viability filtering.

## Deployment

- Machine: DESKTOP-77OPJ6F
- Task id: `lanjob_20260705_165934_2f731c`
- Remote repo: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill`
- Script: `D:\ChengboRemote\runtime\phase3ep_77o_medium_search_20260705.ps1`
- Output root: `runtime\phase3ep_medium_search_20260705_77o`
- Report root: `reports\phase3ep_medium_search_20260705_77o`
- Shard root: `D:\ChengboRemote\data\phase3dz_true1min_sidecar_augmented_full16_20260702`

## Search Configuration

- generation budget: 4096
- CA top-n: 384
- CM candidate limit: 128
- CM max shards: 6
- CM sample trade times per shard: 128
- event-aware sample times: enabled
- event sample trade times per shard: 160
- horizons: `1,5,10`
- train / validation / holdout split: `0.60 / 0.20 / 0.20`
- workers: 6
- parallel axis: shard
- numexpr threads: 2

## Reward Boundary

The optimizer-facing reward is train-side only:

- train day Sortino
- bounded rankIC loss component
- low-weight regime stability component

Validation and holdout remain report-only. They must not feed back into generation reward.

## Semantic Memory

Phase3EP uses two semantic memory protections:

1. The previous Phase3EO output root is passed as a memory root when available.
2. Phase3CP now writes rejected pre-CM semantic viability candidates into:

```text
runtime/search_memory/phase3cp_semantic_blocks
```

These records block both exact expression keys and unsafe/weak semantic skeleton keys.
This prevents zero-row event/state structures from being regenerated with trivial edits.

## Current Startup Evidence

After launch, 77O showed:

- Python process count: 2
- main Python working set: about 15.5GB
- generated files already present:
  - `phase3cp_real_cm_all_generated_top_decisions.csv`
  - `phase3cp_real_cm_candidate_audit.csv`
  - `phase3cp_pre_cm_semantic_viability_gate/phase3cm_candidate_progress.csv`

## Interpretation

This run is expected to answer:

1. Whether fresh generation still collapses into old weak structures after semantic block memory.
2. Whether semantic gate rejection rate stays reasonable at larger budget.
3. Whether the train reward can find candidates that remain nonzero across multiple true1min shards.
4. Whether rankIC loss and low-weight regime stability improve candidate quality without becoming a hard gate.
