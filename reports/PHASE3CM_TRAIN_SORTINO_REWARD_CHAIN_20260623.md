# Phase3CM Train Sortino Reward Chain 2026-06-23

Decision: `PHASE3CM_CHAIN_PREPARED_NO_SEARCH_STARTED`

## Why This Replaces Fragment Reward

`fragment_sortino` is demoted to diagnostic-only. It measures sampled trade-time slices and can over-reward a local fragment/horizon pocket without proving that a candidate forms a stable train-set portfolio curve.

The next reward target is `train portfolio Sortino`:

```text
candidate expression
-> true trade_time 1min signal
-> top/bottom portfolio at each trade_time
-> turnover-adjusted cost
-> equal-weight horizon-sleeve PnL curve
-> train / validation / holdout Sortino summaries
```

## New Route

```text
phase3cm-train-portfolio-sortino-reward-audit
```

Default output:

```text
runtime/phase3cm_train_portfolio_sortino_reward_audit_20260623
reports/phase3cm_train_portfolio_sortino_reward_audit_20260623
```

The route computes:

```text
train_reward
train_day_sortino
train_worst_horizon_day_sortino
train_day_mcmc_p25
validation_day_sortino
holdout_day_sortino
turnover
reward blockers
```

## Chain Contract

```text
CA bridge:
  still allowed as dedupe / hard-reject / proxy queue builder

CM train reward:
  primary search feedback target

BZ fragment replay:
  optional diagnostic slice replay
  not primary reward
```

## Not Started

No search was started by this change. This is code and chain packaging only.

Run plan:

```text
runtime/run_plans/phase3cm_train_sortino_reward_chain_20260623.json
```

## Smoke Verification

A tiny non-search route smoke was run only to verify the new reward audit can read true1min shards and write outputs:

```text
candidate_limit: 1
max_shards: 1
sample_trade_times_per_shard: 8
horizons: 1,5
```

Result:

```text
status: ok
candidate: phase3bs_00122
train_reward: -0.9922991
train_day_sortino: -0.58502053
decision: HOLD_TRAIN_REWARD
blockers:
  non_positive_train_day_sortino
  non_positive_worst_horizon_train_sortino
  weak_train_day_mcmc
  extreme_turnover
  inherited_search_blocker
```

## Next Required Check Before Search

The CM route smoke has passed. Before any large search restart, wire searchers so CEM/UCB consume only `train_reward`, with validation used for arm allocation and holdout kept read-only.
