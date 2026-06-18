# Phase3BZ Fragment Replay Audit 2026-06-16

Decision: `HOLD_RESEARCH_FRAGMENT_REPLAY_AUDIT_COMPLETE`

## Scope

Replays selected BV/BX formulas on true `trade_time` 1min shards and expands proxy spread into trade-time fragments.

## Summary

- candidates: `1`
- fragments: `26`
- followup: `0`
- cost bps: `5.0`
- max shards: `1`
- sampled trade times per shard: `16`

## Top Candidates

| rank | candidate | fragments | days | sortino | mcmc median | day-block prob>0 | turnover | decision | blockers | expression |
|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|
| 1 | `phase3bt_00118` | 26 | 13 | -0.78917412 | -0.80136227 | 0.0 | 0.79235572 | `HOLD_FRAGMENT_REPLAY` | `weak_fragment_mcmc|weak_day_block_mcmc|extreme_turnover` | `CSRank(Mul(Sign(ZScore(Delta($intraday_ret_from_open,10))),ZScore(Div($m1_first30_vol,Add(Abs($volume),0.000001)))))` |

## Bias Audit Boundary

- Signal is computed at sampled true `trade_time`; labels use future minute close returns.
- Cost model is a simple long-short fragment cost proxy, not real fill simulation.
- Day-block bootstrap is used to reduce minute-fragment overconfidence.
- Inherited BP blockers are retained; this audit does not override wrong-lag/crowding flags.
- Decision is HOLD unless fragment evidence survives cost, day-block stability, and blocker checks.
