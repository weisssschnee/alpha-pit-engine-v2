# Phase3CP Reward-Gated Medium Search Smoke 2026-06-23

Decision: `PHASE3CP_REWARD_GATED_MEDIUM_SEARCH_SMOKE_PASS_DIAGNOSTIC_ONLY`

## Result

```text
requested_smoke_candidates: 48
generated_candidates: 48
ca_candidate_count: 24
cm_candidate_count: 24
cn_candidate_count: 24
reschedule_allocated_budget: 512
reschedule_fresh_share: 0.67382812
reschedule_cem_budget: 31
```

## Checks

```text
co_budget_used: True
generated_budget_ok: True
ca_has_candidates: True
cm_fixture_has_candidates: True
cn_memory_has_candidates: True
initial_cem_probe_capped: True
initial_fresh_floor_ok: True
reschedule_total_ok: True
reschedule_fresh_floor_ok: True
holdout_not_optimizer_input: True
```

## Boundary

- Candidate generation uses existing true1min generator functions.
- CM reward is a controlled fixture in this smoke.
- No true1min portfolio reward evaluation is run here.
- Holdout remains report-only and is not used for scheduler decisions.
