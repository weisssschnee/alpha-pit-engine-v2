# Phase3CU Mature Asset Import And Algorithm Gap 2026-06-26

## Decision

`R3B_RESULTS_ARE_NOT_A_VALID_FULL_SEARCH_PROOF`

The r3b run completed, but it did not exercise the mature search chain as intended. The main failure was not insufficient hardware time. The main failure was an incomplete v2 migration of mature true1min prior/reward/search-memory assets plus a narrow runtime algorithm path.

## Root Cause

The v2 repo was missing the mature true1min prior files referenced by `phase3bp_true1min_search_algorithm_smoke.py`.

Before this fix:

```text
policy.total_observation_count = 0
```

Because `_read_csv()` returned an empty list for missing files, BP continued as a cold-start run while looking like a mature UCB/CEM run. This made the r3b search effectively blind template generation plus weak BP proxy ranking.

## Imported Mature Assets

Source:

```text
G:\Project_V7_Rotation\alpha_pit_data_feature_workspace_20260531
```

Target:

```text
G:\Project_V7_Rotation\alpha_pit_true1min_engine_20260619
```

Imported report roots:

```text
reports/phase3bk_bj_top64_strict_audit_20260615
reports/phase3bl_bk_priority_signal_materialization_20260615
reports/phase3bm_bl_pass_focused_replay_20260615
reports/phase3bn_open_diversified_true1min_canary_20260615
reports/phase3bo_mature_cem_bridge_true1min_pack_20260615
reports/phase3bq_compute_allocation_benchmark_20260615
reports/phase3br_midscale_algorithm_practice_20260615
reports/phase3bs_adaptive_ucb_cem_practice_20260615
reports/phase3bs_adaptive_ucb_cem_practice_v2_20260615
reports/phase3bs_adaptive_ucb_cem_practice_ast_v3_20260615
reports/phase3bt_ast_algorithm_bakeoff_20260615
reports/phase3bu_company_ast_fresh_winner_variants_retry3_20260615_pull
reports/phase3bv_company_large_ast_fresh_search_20260616
reports/phase3bv_local_large_ast_fresh_search_20260616
reports/phase3bw_company_feedback_deepen_20260616
reports/phase3ca_bz_candidate_audit_smoke_20260616
```

Imported runtime roots:

```text
runtime/search_memory
runtime/phase3bn_open_diversified_true1min_canary_20260615
runtime/phase3bo_mature_cem_bridge_true1min_pack_20260615
runtime/phase3bm_bl_pass_focused_replay_20260615
runtime/phase3bl_bk_priority_signal_materialization_20260615
```

After import:

```text
policy.total_observation_count = 66
prior decision files = 3/3 present
prior hash files = 5/5 present
```

## Algorithm Coverage Reality

The system is not only CEM/UCB. The scheduler defines these arms:

```text
rx_ucb_fresh
typed_ast_fresh
challenger_repair
event_state
cem_exploit
random_orthogonal
```

BP directly exposes these generator modes:

```text
rx_ucb
cem_elite
hybrid_rx_cem
event_state
```

However, the r3b run effectively exercised only:

```text
phase3bp_true1min_hybrid_rx_cem
```

This is a narrow path. It is not a full multi-arm large search.

## Code Fix

`phase3bp_true1min_search_algorithm_smoke.py` now has a checked seed-policy loader:

```text
build_checked_seed_policy(...)
```

Default behavior:

```text
missing prior decision/hash files -> hard RuntimeError
empty policy observations -> hard RuntimeError
```

Only explicit diagnostics may bypass this with:

```text
--allow-empty-policy
```

The hard-check is now used by:

```text
phase3bp_true1min_search_algorithm_smoke.py
phase3bs_adaptive_ucb_cem_practice.py
phase3bt_ast_algorithm_bakeoff.py
phase3bu_ast_fresh_winner_variants.py
phase3cp_reward_gated_medium_search_smoke.py
phase3cp_real_cm_small_loop.py
```

## Verification

```text
py_compile: pass
missing prior smoke: HARD_FAIL_OK
imported prior smoke: POLICY_OK 66 3 5
```

## Search Implication

Do not treat r3b as evidence that the full true1min search space failed.

The next valid run must be a multi-arm run with explicit budgets for:

```text
typed_ast_fresh
rx_ucb_fresh
event_state
challenger_repair
random_orthogonal
cem_exploit capped until clean feedback exists
```

It must consume real CM train reward feedback and search memory, and it must report arm-level reward, Sortino, MCMC, wrong-lag rejection, and family concentration.
