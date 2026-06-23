# Phase3CN Feedback Memory Smoke 2026-06-23

Decision: `PHASE3CN_FEEDBACK_MEMORY_READY_DIAGNOSTIC_ONLY`

## Scope

Reads Phase3CM train reward outputs and writes standardized search feedback memory. This route does not run search.

## Summary

- input tables: `1`
- candidates: `12`
- families: `9`
- exploit-allowed families: `0`
- blocked/frozen families: `9`

## Arm Scores

| arm | rows | clean | update | median reward | validation rate | wrong-lag/corr | rewardhack | top family | score |
|---|---:|---:|---|---:|---:|---:|---:|---:|---:|
| `event_state` | 2 | 0 | `false` | -0.48001933 | 0.5 | 0.0 | 1.0 | 0.5 | 0.51998067 |
| `rx_ucb_fresh` | 3 | 0 | `false` | -0.53254818 | 0.33333333 | 0.0 | 1.0 | 0.33333333 | 0.46745182 |
| `challenger_repair` | 2 | 0 | `false` | -0.8097812 | 0.0 | 0.0 | 1.0 | 0.5 | -0.3097812 |
| `random_orthogonal` | 2 | 0 | `false` | -0.91368209 | 0.0 | 0.0 | 1.0 | 0.5 | -0.41368209 |
| `typed_ast_fresh` | 3 | 0 | `false` | -1.16062275 | 0.0 | 0.0 | 1.0 | 0.66666667 | -1.16062275 |

## Top Families

| family | status | rows | median reward | clean | validation | reasons |
|---|---|---:|---:|---:|---:|---|
| `6ace37bd35f0fab316` | `freeze` | 2 | -0.25913256 | 0 | 1 | `proxy_high_cm_negative` |
| `82d605af753316a0b5` | `freeze` | 1 | -0.37184462 | 0 | 1 | `proxy_high_cm_negative` |
| `6dfdc2653ce052bd27` | `freeze` | 2 | -0.73459827 | 0 | 0 | `proxy_high_cm_negative` |
| `9e96ab1433d465d733` | `freeze` | 1 | -0.8453687 | 0 | 0 | `proxy_high_cm_negative` |
| `c3be9eb0a3620ec39b` | `freeze` | 1 | -0.86557817 | 0 | 0 | `proxy_high_cm_negative` |
| `e405978a5756844266` | `freeze` | 1 | -0.89071584 | 0 | 0 | `proxy_high_cm_negative` |
| `f3d0ccad29ac7200a7` | `freeze` | 1 | -0.94622189 | 0 | 0 | `proxy_high_cm_negative` |
| `4926a97a8ff2d2c4d5` | `freeze` | 1 | -1.11511405 | 0 | 0 | `proxy_high_cm_negative` |
| `dd0cbfd4948290aab1` | `freeze` | 2 | -1.17039963 | 0 | 0 | `proxy_high_cm_negative` |

## Boundary

- Holdout fields are carried through as read-only metadata and are not used in arm_score.
- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.
- Proxy-high but CM-negative families are frozen or blocked before exploit.
