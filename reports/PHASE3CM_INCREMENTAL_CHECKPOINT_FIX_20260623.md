# Phase3CM Incremental Checkpoint Fix 2026-06-23

## Decision

Phase3CM train portfolio Sortino reward must write incremental reward checkpoints during long runs. A final-only `phase3cm_train_reward.csv` is not acceptable for multi-hour reward evaluation because it hides failed runs, prevents early audit, and wastes generated candidate pools when a job is stopped.

## Change

`phase3cm_train_portfolio_sortino_reward_audit.py` now writes the following files during execution:

- `phase3cm_candidate_progress.csv`
- `phase3cm_train_reward_partial.csv`
- `phase3cm_incremental_shard_meta.csv`
- `phase3cm_incremental_checkpoint_summary.json`

The partial reward file is explicitly marked as not-final unless `final=true` in the checkpoint summary.

## Recovery Plan

Existing Phase3CP large candidate audit files are reused directly. The recovery runs start at Phase3CM, not at candidate generation:

- local `phase3cp_large_local_rx_typed_q20_20260623`
- local `phase3cp_large_local_challenger_cem_q20_20260623`
- company `phase3cp_large_company_rx_typed_q20_20260623`
- company `phase3cp_large_company_challenger_cem_q25_20260623`

## Boundary

This is reward observability and recoverability infrastructure. It does not promote candidates, modify X0/R3, or convert fragment replay into proof.
