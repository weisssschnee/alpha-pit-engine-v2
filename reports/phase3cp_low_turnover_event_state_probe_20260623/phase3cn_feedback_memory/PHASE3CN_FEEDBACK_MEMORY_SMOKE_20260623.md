# Phase3CN Feedback Memory Smoke 2026-06-23

Decision: `PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY`

## Scope

Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.

## Summary

- input tables: `1`
- candidates: `24`
- families: `5`
- exploit-allowed families: `0`
- blocked/frozen families: `5`

## Arm Scores

| arm | rows | clean | update | median reward | validation rate | wrong-lag/corr | rewardhack | top family | score |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| `event_state` | 24 | 0 | `false` | -0.76392913 | 0.08333333 | 0.0 | 1.0 | 0.25 | -0.72226246 |

## Top Families

| family | status | rows | median reward | clean | validation | reasons |
|---|---|---:|---:|---:|---:|---|
| `b0b42baf8b96c07171` | `freeze` | 6 | -0.55929062 | 0 | 0 | `proxy_high_cm_negative` |
| `8644f273d365016bf4` | `freeze` | 4 | -0.57136798 | 0 | 0 | `proxy_high_cm_negative` |
| `66735011e77830c49a` | `freeze` | 4 | -0.80578749 | 0 | 0 | `proxy_high_cm_negative` |
| `9b7cb4f44ecb642b25` | `freeze` | 4 | -0.80658389 | 0 | 2 | `proxy_high_cm_negative` |
| `cced4d5630efed27cf` | `freeze` | 6 | -0.93383593 | 0 | 0 | `proxy_high_cm_negative` |

## Boundary

- Holdout fields are carried through as read-only metadata and are not used in arm_score.
- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.
- Proxy-high but CM-negative families are frozen or blocked before exploit.
