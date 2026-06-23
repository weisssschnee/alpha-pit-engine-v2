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
| `event_state` | 2 | 1 | `false` | -0.31501473 | 0.5 | 0.0 | 0.5 | 0.5 | 1.68498526 |
| `rx_ucb_fresh` | 3 | 0 | `false` | -0.60969415 | 0.33333333 | 0.0 | 1.0 | 0.33333333 | 0.39030585 |
| `challenger_repair` | 2 | 0 | `false` | -0.79998804 | 0.0 | 0.0 | 1.0 | 0.5 | -0.29998805 |
| `random_orthogonal` | 2 | 0 | `false` | -0.88398276 | 0.0 | 0.0 | 1.0 | 0.5 | -0.38398275 |
| `typed_ast_fresh` | 3 | 0 | `false` | -1.09496861 | 0.0 | 0.0 | 1.0 | 0.66666667 | -1.09496861 |

## Top Families

| family | status | rows | median reward | clean | validation | reasons |
|---|---|---:|---:|---:|---:|---|
| `6ace37bd35f0fab316` | `freeze` | 2 | -0.1026596 | 1 | 1 | `proxy_high_cm_negative` |
| `82d605af753316a0b5` | `freeze` | 1 | -0.15805198 | 0 | 1 | `proxy_high_cm_negative` |
| `6dfdc2653ce052bd27` | `freeze` | 2 | -0.74125715 | 0 | 0 | `proxy_high_cm_negative` |
| `c3be9eb0a3620ec39b` | `freeze` | 1 | -0.84403228 | 0 | 0 | `proxy_high_cm_negative` |
| `9e96ab1433d465d733` | `freeze` | 1 | -0.87506302 | 0 | 0 | `proxy_high_cm_negative` |
| `e405978a5756844266` | `freeze` | 1 | -0.89514537 | 0 | 0 | `proxy_high_cm_negative` |
| `f3d0ccad29ac7200a7` | `freeze` | 1 | -0.95400507 | 0 | 0 | `proxy_high_cm_negative` |
| `4926a97a8ff2d2c4d5` | `freeze` | 1 | -1.07068129 | 0 | 0 | `proxy_high_cm_negative` |
| `dd0cbfd4948290aab1` | `freeze` | 2 | -1.09713059 | 0 | 0 | `proxy_high_cm_negative` |

## Boundary

- Holdout fields are carried through as read-only metadata and are not used in arm_score.
- `feedback_update_allowed=false` means CEM/UCB must not update from that arm.
- Proxy-high but CM-negative families are frozen or blocked before exploit.
