# Phase3DB Train-Sortino Feedback Controller

Decision: `PHASE3DB_TRAIN_SORTINO_FEEDBACK_CONTROLLER_READY_DIAGNOSTIC_ONLY`

## Why Proxy Still Exists

Proxy is not the optimizer reward.

Its only allowed roles are:

```text
1. cheap safety veto before expensive CM evaluation
2. compute-cost triage when the formula space is too large
3. reward-hacking detector when proxy is positive but train Sortino is negative
```

Proxy has zero positive scoring weight in Phase3DB:

```text
proxy_positive_weight = 0.0
proxy_usage = veto_only
```

This means proxy can reject or cap a candidate/family, but it cannot increase
budget, grant CEM exploit permission, or define a winner.

## Optimizer Reward

The optimizer feedback is:

```text
Phase3CM train portfolio Sortino reward
```

Validation and holdout are report-only:

```text
validation_usage = report_only
holdout_usage = report_only
```

They must not feed search budget updates.

## Controller Logic

The controller reads real CM train reward labels and emits the next
`phase3co_arm_budget_table.csv` for Phase3CP.

Positive budget expansion requires:

```text
train_sortino_controller_score > 0
feedback_update_allowed = true
clean_feedback_count >= min_clean_arm
```

Negative train reward cannot be converted into a positive budget signal. A
"less negative" arm is not treated as a winner.

## Smoke Result On Prior CZ Labels

Input:

```text
reports/phase3cz_cn_feedback_after_cm_20260627
reports/phase3cz_lan77_cm_train_reward_20260627
```

Result:

```text
input_candidate_count: 50
exploit_allowed_family_count: 0
blocked_family_count: 15
fresh_share: 0.75
cem_exploit_budget: 819 / 32768
proxy_positive_weight: 0.0
```

Interpretation:

```text
The prior proxy-winner set had no positive clean train-Sortino evidence.
CEM exploit is capped to probe-only.
Fresh search remains dominant.
```

## 77 Deployment

Current heavy run:

```text
task_id: lanjob_20260627_143521_9935f1
route: phase3cp-real-cm-small-loop
reward gate: Phase3CM train portfolio Sortino
```

Post-run budget watcher:

```text
task_id: lanjob_20260627_144820_5691f2
script: D:\ChengboRemote\runtime\lan_77_phase3db_after_da_budget_20260627.ps1
```

The watcher waits for the current DA CM summary, then runs Phase3DB and writes:

```text
reports\phase3db_from_phase3da_lan77_20260627
runtime\phase3db_from_phase3da_lan77_20260627
```

It does not automatically launch another heavy search.
