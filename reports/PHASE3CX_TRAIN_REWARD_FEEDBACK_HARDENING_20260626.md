# Phase3CX Train Reward Feedback Hardening 2026-06-26

Decision: `PHASE3CX_TRAIN_REWARD_FEEDBACK_WIRED_NO_SEARCH_STARTED`

## Objective

Make CEM/UCB/CP feedback consume true Phase3CM train reward as optimizer input.
Validation and holdout remain report-only leakage controls and are not used to
update generator policy or arm budgets.

## Code Changes

```text
src/our_system_phase2/services/candidate_schema.py
  adds optimizer_reward / optimizer_reward_source / optimizer_reward_metric /
  optimizer_reward_split / validation_usage / holdout_usage

src/our_system_phase2/runtime/phase3cn_feedback_memory_smoke.py
  writes train-only optimizer_reward fields into CN feedback memory
  removes validation survival from exploit permission and arm score

src/our_system_phase2/services/search_feedback.py
  clean feedback now uses optimizer_reward/train_reward only
  validation_used_for_score=false and holdout_used_for_score=false are explicit
  adds clean_optimizer_feedback_rows and load_search_feedback_rows

src/our_system_phase2/services/multi_arm_scheduler.py
  removes validation_survival_rate from arm health score

src/our_system_phase2/runtime/phase3bs_adaptive_ucb_cem_practice.py
  adds _policy_with_train_reward_feedback
  external CN feedback now updates policy from train reward rows instead of proxy decisions

src/our_system_phase2/runtime/phase3bt_ast_algorithm_bakeoff.py
src/our_system_phase2/runtime/phase3bu_ast_fresh_winner_variants.py
  route external feedback through the same train-reward policy updater

src/our_system_phase2/runtime/phase3cn_searcher_feedback_smoke.py
  adds a positive train-reward policy-update smoke
```

## Boundary

```text
optimizer input:
  optimizer_reward
  train_reward

not optimizer input:
  validation_day_sortino
  validation_mcmc_prob_gt_0
  holdout_day_sortino
  holdout_mcmc_prob_gt_0

search status:
  no large search started in this patch
  no X0/R3 modification
```

## Verification

```powershell
$env:PYTHONPATH='src'
G:\PythonProject\.venv\Scripts\python.exe -m py_compile `
  src\our_system_phase2\services\candidate_schema.py `
  src\our_system_phase2\services\search_feedback.py `
  src\our_system_phase2\services\multi_arm_scheduler.py `
  src\our_system_phase2\runtime\phase3cn_feedback_memory_smoke.py `
  src\our_system_phase2\runtime\phase3bs_adaptive_ucb_cem_practice.py `
  src\our_system_phase2\runtime\phase3bt_ast_algorithm_bakeoff.py `
  src\our_system_phase2\runtime\phase3bu_ast_fresh_winner_variants.py

G:\PythonProject\.venv\Scripts\python.exe app.py phase3cn-searcher-feedback-smoke --allow-diagnostic -- `
  --output-root runtime/phase3cx_searcher_feedback_train_only_smoke_20260626 `
  --report-root reports/phase3cx_searcher_feedback_train_only_smoke_20260626

G:\PythonProject\.venv\Scripts\python.exe app.py phase3cn-integrated-feedback-smoke --allow-diagnostic -- `
  --output-root runtime/phase3cx_integrated_train_only_feedback_smoke_20260626 `
  --report-root reports/phase3cx_integrated_train_only_feedback_smoke_20260626
```

## Smoke Results

```text
phase3cn-searcher-feedback-smoke:
  decision: PHASE3CN_SEARCHER_FEEDBACK_GUARD_PASS_DIAGNOSTIC_ONLY
  bad feedback blocked: true
  policy_scores_unchanged under blocked feedback: true
  allowed_train_reward_policy_updated: true
  allowed_train_reward_train_only: true
  validation_used_for_score: false
  holdout_used_for_score: false

phase3cn-integrated-feedback-smoke:
  decision: PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_PASS_DIAGNOSTIC_ONLY
  clean_feedback_count: 1
  strict_update_allowed: false
  loose_update_allowed: true
  holdout_used_for_score: false
```

## Next Search Use

Next BS/BT/BU/CP search should pass:

```text
--feedback-table phase3cn_search_feedback_memory.csv
--arm-score-table phase3cn_arm_score_table.csv
--family-memory phase3cn_family_score_table.csv
--blocked-family-table phase3cn_blocked_family_table.csv
--exploit-allowed-family-table phase3cn_exploit_allowed_family_table.csv
```

If external CN feedback is present but not clean enough, policy update remains
blocked instead of silently falling back to proxy reward.
