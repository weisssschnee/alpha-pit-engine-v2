# Phase3CO Multi-Arm Scheduler Smoke 2026-06-23

Decision: `PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_PASS_DIAGNOSTIC_ONLY`

## Result

```text
total_budget: 512
allocated_budget: 512
fresh_share: 0.73242188
cem_exploit_budget: 7
cem_probe_cap_budget: 31
exploit_allowed_family_count: 1
blocked_or_frozen_family_count: 1
```

## Arm Budgets

| arm | budget | share | action | reason |
|---|---:|---:|---|---|
| `rx_ucb_fresh` | 153 | 0.29882812 | `fresh_floor` | no CN feedback for this arm; use fresh/control floor only |
| `typed_ast_fresh` | 130 | 0.25390625 | `fresh_floor` | no CN feedback for this arm; use fresh/control floor only |
| `challenger_repair` | 92 | 0.1796875 | `fresh_floor` | no CN feedback for this arm; use fresh/control floor only |
| `event_state` | 82 | 0.16015625 | `event_floor` | no CN feedback for this arm; use fresh/control floor only |
| `cem_exploit` | 7 | 0.01367188 | `probe_only` | CEM exploit capped until CN clean feedback and exploit families are sufficient |
| `random_orthogonal` | 48 | 0.09375 | `schedule` | no CN feedback for this arm; use fresh/control floor only |

## Family Actions

| family | action | cap | reason |
|---|---|---:|---|
| `50b3739ba3bb277607` | `freeze` | 0 | proxy_high_cm_negative|high_turnover |
| `5cb2d01478fef46431` | `allow_followup` | 128 | cm_positive_validation_survivor |

## Boundary

- No search generation.
- No true1min portfolio evaluation.
- Holdout is not an input to scheduler scoring.
- CEM exploit remains capped when CN feedback_update_allowed=false.
