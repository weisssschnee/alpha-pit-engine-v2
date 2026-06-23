# Phase3CP Reward-Gated Medium Search 2026-06-23

Decision: `PHASE3CP_SMOKE_IMPLEMENTED_NO_TRUE1MIN_PORTFOLIO_EVAL`

## Scope

Phase3CP is the first closed-loop search stage after CN/CO. The initial route is
a controlled smoke: it generates candidates from the existing true1min generator
functions under Phase3CO budgets, then runs CA, a controlled CM reward fixture,
CN feedback memory, and CO reschedule.

This does not claim alpha and does not run true Phase3CM portfolio reward yet.

## Implemented

```text
route:
  phase3cp-reward-gated-medium-search-smoke

run plan:
  runtime/run_plans/phase3cp_reward_gated_medium_search_smoke_20260623.json
```

## Chain

```text
Phase3CO arm budget table
  -> scheduler-controlled candidate generation
  -> Phase3CA candidate bridge
  -> controlled Phase3CM reward fixture
  -> Phase3CN feedback memory
  -> Phase3CO reschedule
```

## Hard Boundary

```text
search_generation: true
true1min_portfolio_eval: false
cm_reward_source: controlled_fixture

The next escalation must replace the fixture with:
  phase3cm-train-portfolio-sortino-reward-audit
```

## Smoke Verification

```text
route:
  phase3cp-reward-gated-medium-search-smoke

decision:
  PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_PASS_DIAGNOSTIC_ONLY

requested_smoke_candidates: 48
generated_candidates: 48
ca_candidate_count: 24
cm_candidate_count: 24
cn_candidate_count: 24

initial arm plan:
  rx_ucb_fresh: 14
  typed_ast_fresh: 12
  challenger_repair: 9
  event_state: 8
  cem_exploit: 1
  random_orthogonal: 4

reschedule:
  total_budget: 512
  allocated_budget: 512
  fresh_budget: 345
  fresh_share: 0.67382812
  cem_exploit_budget: 31
  exploit_allowed_family_count: 2
  blocked_or_frozen_family_count: 11

checks:
  co_budget_used: true
  generated_budget_ok: true
  ca_has_candidates: true
  cm_fixture_has_candidates: true
  cn_memory_has_candidates: true
  initial_cem_probe_capped: true
  initial_fresh_floor_ok: true
  reschedule_total_ok: true
  reschedule_fresh_floor_ok: true
  holdout_not_optimizer_input: true
```

## Output Evidence

```text
reports/phase3cp_reward_gated_medium_search_smoke_20260623/PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_20260623.md
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cp_arm_execution_plan.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cp_all_generated_top_decisions.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3ca_bridge/phase3ca_bz_candidate_audit.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cm_reward_fixture/phase3cm_train_reward.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cn_feedback_memory/phase3cn_search_feedback_memory.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cp_next_arm_budget_table.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cp_next_family_action_table.csv
reports/phase3cp_reward_gated_medium_search_smoke_20260623/phase3cp_closed_loop_summary.json
```

## Next Gate

Replace the controlled CM fixture with the real route:

```text
phase3cm-train-portfolio-sortino-reward-audit
```

Then run a real small CP loop before any larger Phase3CQ restart.
