# Phase3CO Multi-Arm Scheduler 2026-06-23

Decision: `PHASE3CO_MULTI_ARM_SCHEDULER_IMPLEMENTED_NO_SEARCH_STARTED`

## Scope

Phase3CO turns Phase3CN feedback memory into explicit Phase3CP search budgets.
It does not generate candidates and does not run true1min portfolio evaluation.

## Implemented

```text
service:
  src/our_system_phase2/services/multi_arm_scheduler.py

route:
  phase3co-multi-arm-scheduler-smoke

run plan:
  runtime/run_plans/phase3co_multi_arm_scheduler_20260623.json
```

## Inputs

```text
phase3cn_search_feedback_memory.csv
phase3cn_arm_score_table.csv
phase3cn_family_score_table.csv
phase3cn_blocked_family_table.csv
phase3cn_exploit_allowed_family_table.csv
```

## Outputs

```text
phase3co_arm_budget_table.csv
phase3co_family_action_table.csv
phase3co_scheduler_summary.json
```

## Rules

```text
fresh arms:
  keep fresh_floor_share

CEM exploit:
  capped to probe share while CN feedback_update_allowed=false

family actions:
  block/freeze -> zero followup cap
  downweight -> reduced cap
  exploit_allowed -> bounded followup cap

holdout:
  not used in scheduler scoring
```

## Smoke Verification

```text
route:
  phase3co-multi-arm-scheduler-smoke

decision:
  PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_PASS_DIAGNOSTIC_ONLY

input rows:
  feedback: 2
  arm_score: 1
  family: 2
  blocked_family: 1
  exploit_allowed_family: 1

budget:
  total_budget: 512
  allocated_budget: 512
  fresh_budget: 375
  fresh_share: 0.73242188
  cem_exploit_budget: 7
  cem_probe_cap_budget: 31

checks:
  fresh_floor_ok: true
  cem_probe_cap_ok: true
  family_block_ok: true
  exploit_family_ok: true
  total_budget_ok: true
```

## Output Evidence

```text
reports/phase3co_multi_arm_scheduler_smoke_20260623/PHASE3CO_MULTI_ARM_SCHEDULER_SMOKE_20260623.md
reports/phase3co_multi_arm_scheduler_smoke_20260623/phase3co_arm_budget_table.csv
reports/phase3co_multi_arm_scheduler_smoke_20260623/phase3co_family_action_table.csv
reports/phase3co_multi_arm_scheduler_smoke_20260623/phase3co_scheduler_summary.json
```

## Next Gate

Phase3CP can now be implemented as the first medium closed-loop search that reads:

```text
phase3co_arm_budget_table.csv
phase3co_family_action_table.csv
```

and then runs:

```text
generation -> CA bridge -> CM train reward -> CN feedback memory -> CO reschedule
```
