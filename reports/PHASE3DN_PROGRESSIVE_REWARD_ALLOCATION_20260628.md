# Phase3DN Progressive Reward Allocation Contract 2026-06-28

Decision: `PHASE3DN_PROGRESSIVE_REWARD_ALLOCATION_READY_DIAGNOSTIC_ONLY`

## Reward Contract

```text
optimizer_reward_metric:
  train_portfolio_sortino_rankic_composite_reward

optimizer_reward:
  train-side portfolio Sortino reward
  + bounded train-side rank IC loss component
  - turnover / instability / inherited blocker penalties

rank_ic_loss:
  -train_rank_ic_mean

rank_ic_reward_component:
  clamp(-rank_ic_loss_weight * rank_ic_loss, -rank_ic_component_cap, rank_ic_component_cap)

default rank_ic_loss_weight:
  6.0

default rank_ic_component_cap:
  0.35
```

Validation and holdout remain report-only. They are not optimizer inputs and must not change CEM/UCB/RX sampling weights.

## Allocation Rule

Cheap proxy, fragment replay, or partial reward probes may allocate compute, but they must not permanently reject a candidate for weak short-sample performance.

Hard rejection is limited to structural invalidity:

```text
wrong-lag / future leakage
blocked or unavailable fields
typed primitive violation
unsafe skeleton
parse or evaluation invalidity
extreme turnover / cost safety guard
```

Performance evidence should be used progressively:

```text
stage 1:
  cheap proxy / accounting / memory / typed gate
  purpose: route candidates and avoid known bad structures

stage 2:
  small true train reward probe
  purpose: prioritize more compute, not delete novelty reserve

stage 3:
  full train composite reward audit
  purpose: optimizer feedback and next-round budget schedule

stage 4:
  validation / holdout
  purpose: report-only leakage control and promotion blocking, not reward fitting
```

## Search Basket Constraint

Every CM reward batch should keep a mixed basket:

```text
reward-ranked candidates
per-arm quota candidates
fresh / novelty reserve
event-state reserve
random orthogonal reserve
```

This prevents a weak early reward proxy from starving fresh search and prevents CEM from over-exploiting the first positive family.

## Implementation Status

Implemented wiring:

```text
phase3cm_train_portfolio_sortino_reward_audit:
  computes train / validation / holdout rank IC diagnostics
  adds train_rank_ic_loss to optimizer reward through bounded component

candidate_schema:
  preserves train / validation / holdout rank IC fields
  default optimizer_reward_metric is composite reward

search_feedback:
  consumes composite optimizer_reward_metric
  keeps validation and holdout report-only

phase3cn_feedback_memory_smoke:
  preserves optimizer_reward instead of overwriting it with legacy train_reward

phase3db_train_sortino_feedback_controller:
  budgets arms from composite optimizer reward
  keeps proxy as veto-only

phase3cp_real_cm_small_loop:
  passes rank IC reward parameters into serial / parallel CM workers
  records composite optimizer metric in parallel summaries
```

## Boundary

This contract is not alpha proof. It only defines how search should spend compute after Phase3CM reward is available.

X0/R3 remain read-only.
