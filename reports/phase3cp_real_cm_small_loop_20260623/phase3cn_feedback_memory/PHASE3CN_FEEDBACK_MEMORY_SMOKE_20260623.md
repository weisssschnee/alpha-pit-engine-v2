# Phase3CN Feedback Memory Smoke 2026-06-23

Decision: `PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY`

## Scope

Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.

## Summary

- input tables: `1`
- candidates: `4`
- families: `4`
- exploit-allowed families: `0`
- blocked/frozen families: `2`

## Arm Scores

| arm | rows | clean | update | median reward | validation rate | wrong-lag/corr | rewardhack | top family | score |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| `rx_ucb_fresh` | 2 | 0 | `false` | -0.61839196 | 0.5 | 0.0 | 0.0 | 0.5 | 0.88160804 |
| `cem_exploit` | 2 | 0 | `false` | -1.028523 | 0.0 | 0.0 | 0.0 | 0.5 | -0.028523 |

## Top Families

| family | status | rows | median reward | clean | validation | reasons |
|---|---|---:|---:|---:|---:|---|
| `ed6b86d1110a56ffa4` | `freeze` | 1 | -0.58857199 | 0 | 0 | `high_turnover` |
| `6dfdc2653ce052bd27` | `normal` | 1 | -0.64821192 | 0 | 1 | `` |
| `9e96ab1433d465d733` | `normal` | 1 | -0.84887634 | 0 | 0 | `` |
| `e1320d40a94aee9778` | `freeze` | 1 | -1.20816966 | 0 | 0 | `high_turnover` |

## Boundary

- Holdout fields are carried through as read-only metadata and are not used in arm_score.
- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.
- Proxy-high but CM-negative families are frozen or blocked before exploit.
