# Phase3CN Feedback Memory Smoke 2026-06-23

Decision: `PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY`

## Scope

Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.

## Summary

- input tables: `1`
- candidates: `2`
- families: `2`
- exploit-allowed families: `1`
- blocked/frozen families: `1`

## Arm Scores

| arm | rows | clean | update | median reward | validation rate | wrong-lag/corr | rewardhack | top family | score |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| `cem_exploit` | 2 | 1 | `false` | -0.115 | 0.5 | 0.0 | 0.5 | 0.5 | 1.385 |

## Top Families

| family | status | rows | median reward | clean | validation | reasons |
|---|---|---:|---:|---:|---:|---|
| `5cb2d01478fef46431` | `exploit_allowed` | 1 | 0.38 | 1 | 1 | `cm_positive_validation_survivor` |
| `50b3739ba3bb277607` | `freeze` | 1 | -0.61 | 0 | 0 | `proxy_high_cm_negative|high_turnover` |

## Boundary

- Holdout fields are carried through as read-only metadata and are not used in arm_score.
- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.
- Proxy-high but CM-negative families are frozen or blocked before exploit.
