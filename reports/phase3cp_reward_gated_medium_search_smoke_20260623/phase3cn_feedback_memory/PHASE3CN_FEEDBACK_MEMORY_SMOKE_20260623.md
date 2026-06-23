# Phase3CN Feedback Memory Smoke 2026-06-23

Decision: `PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY`

## Scope

Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.

## Summary

- input tables: `1`
- candidates: `24`
- families: `13`
- exploit-allowed families: `2`
- blocked/frozen families: `11`

## Arm Scores

| arm | rows | clean | update | median reward | validation rate | wrong-lag/corr | rewardhack | top family | score |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| `typed_ast_fresh` | 11 | 5 | `true` | -0.16 | 0.45454545 | 0.0 | 0.54545455 | 0.27272727 | 1.56727273 |
| `rx_ucb_fresh` | 9 | 3 | `false` | -0.16 | 0.33333333 | 0.0 | 0.66666667 | 0.22222222 | 1.50666667 |
| `challenger_repair` | 4 | 0 | `false` | -0.16 | 0.0 | 0.0 | 1.0 | 0.25 | 0.59 |

## Top Families

| family | status | rows | median reward | clean | validation | reasons |
|---|---|---:|---:|---:|---:|---|
| `3a0aa671f151a6385d` | `exploit_allowed` | 1 | 0.19753824 | 1 | 1 | `cm_positive_validation_survivor` |
| `1e9e7c338a7cdf0d26` | `exploit_allowed` | 1 | 0.19751635 | 1 | 1 | `cm_positive_validation_survivor` |
| `dd0cbfd4948290aab1` | `freeze` | 3 | 0.19885699 | 2 | 2 | `proxy_high_cm_negative` |
| `c3be9eb0a3620ec39b` | `freeze` | 2 | 0.01923698 | 1 | 1 | `proxy_high_cm_negative` |
| `f3d0ccad29ac7200a7` | `freeze` | 2 | 0.01914122 | 1 | 1 | `proxy_high_cm_negative` |
| `9e96ab1433d465d733` | `freeze` | 2 | 0.01878553 | 1 | 1 | `proxy_high_cm_negative` |
| `0eefd6eb7e19cb2f4e` | `freeze` | 1 | -0.16 | 0 | 0 | `proxy_high_cm_negative` |
| `152605e05e909b77c2` | `freeze` | 1 | -0.16 | 0 | 0 | `proxy_high_cm_negative` |
| `4926a97a8ff2d2c4d5` | `freeze` | 1 | -0.16 | 0 | 0 | `proxy_high_cm_negative` |
| `505c9d9dcd48c5334b` | `freeze` | 3 | -0.16 | 0 | 0 | `proxy_high_cm_negative` |
| `6ace37bd35f0fab316` | `freeze` | 2 | -0.16 | 0 | 0 | `proxy_high_cm_negative` |
| `6dfdc2653ce052bd27` | `freeze` | 4 | -0.16 | 1 | 1 | `proxy_high_cm_negative` |
| `82d605af753316a0b5` | `freeze` | 1 | -0.16 | 0 | 0 | `proxy_high_cm_negative` |

## Boundary

- Holdout fields are carried through as read-only metadata and are not used in arm_score.
- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.
- Proxy-high but CM-negative families are frozen or blocked before exploit.
