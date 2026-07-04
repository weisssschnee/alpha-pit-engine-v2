# Phase3DV Budget-Pool Self-Deepen 2026-06-30

Decision: `PHASE3DV_CURRENT_SEARCH_ROUTE_ACTIVE`

## Why This Phase Exists

Phase3DU exposed a design problem: the search loop was still behaving like it
was looking for a near-perfect seed before spending more compute. That is too
strict for true1min alpha discovery. A useful cluster can be regime-limited,
turnover-imperfect, or horizon-fragile and still deserve a bounded follow-up
budget.

Phase3DV changes the semantics from hard elimination to budget allocation.

```text
Hard structural blockers:
  still reject.

Imperfect but informative train evidence:
  allocate bounded budget.

Validation / holdout:
  report-only; never optimizer input.
```

## Current Route

```text
phase3dv-budget-pool-self-deepen-pack
```

The route builds a candidate pack for Phase3CM train composite reward audit.
It is not an alpha proof and does not modify X0/R3.

## Budget States

```text
global_survivor:
  broadly positive train evidence; receives elite deepen budget.

regime_specialist:
  not globally clean, but has enough regime-specific structure to receive
  specialist budget.

probation:
  weak or incomplete evidence, but not structurally invalid; receives bounded
  probe budget.

fresh_self_deepen:
  novelty reserve; keeps the searcher from collapsing into only old survivor
  families.

reject_no_budget:
  not structurally unsafe, but below current budget threshold.
```

## What Remains Hard-Rejected

```text
wrong-lag / future leakage
future or label fields
typed primitive violations
unsafe known structures
missing schema fields
parse or evaluation invalidity
```

Notably, `non_positive_worst_horizon_train_sortino` is no longer treated as a
universal construction-time death sentence. It is a risk signal and budget
penalty unless combined with structural blockers or budget exhaustion.

## Implementation

Files:

```text
src/our_system_phase2/runtime/phase3dv_budget_pool_self_deepen_pack.py
app.py
runtime/run_plans/phase3dv_budget_pool_self_deepen_cm_reward_20260630.json
```

Current app route status:

```text
current:
  phase3dv-budget-pool-self-deepen-pack

retired:
  phase3du-adaptive-regime-free-deepen-pack
    reason: superseded by Phase3DV budget-pool self-deepen
    app.py behavior: blocked unless --allow-diagnostic is passed
```

Phase3DS and Phase3DT remain provenance/upstream pack routes. They are not the
current large-search entry.

## Remote Run

Node:

```text
DESKTOP-77OPJ6F
alias: chengbo-lan-77opj6f
task_id: lanjob_20260630_150345_ab2922
workspace: D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill
true1min shard root: D:\ChengboRemote\data\phase3cy_true1min_sidecar_augmented_shards_20260626
```

Pack:

```text
candidate_count: 6144
seed_count: 216
available_field_count: 121
configured_missing_fields: []
typed_gate_registry_version: phase3ce1_typed_primitive_gate_v1_20260618
```

Lane counts:

```text
phase3dv_lane_a_elite_budget_deepen: 1080
phase3dv_lane_b_regime_specialist_budget: 1890
phase3dv_lane_c_probation_budget_probe: 1080
phase3dv_lane_d_fresh_self_deepen: 2094
```

Seed status counts:

```text
global_survivor: 90
regime_specialist: 54
probation: 72
```

CM reward parameters:

```text
max_parallel: 6
chunk_size: 128
max_shards: 6
sample_trade_times_per_shard: 384
horizons: 1,5,15,30,60
rank_ic_loss_weight: 6.0
rank_ic_component_cap: 0.35
regime_stability_weight: 0.08
regime_component_cap: 0.10
operator_cache_max_entries: 256
feature_matrix_cache_max_windows: 4
```

## Why Not 8 Parallel

An 8-parallel attempt pushed DESKTOP-77OPJ6F free physical memory to roughly
0.8GB. It was stopped before instability. The current 6-parallel run is the
active heavy setting because it keeps a meaningful memory reserve while still
using tens of GB of RAM for true1min reward work.

## Architecture Update

The current search stack is:

```text
Phase3CM:
  train portfolio Sortino + rankIC + low-weight regime stability reward
  validation / holdout report-only
  bounded evaluator caches

Phase3DV:
  train-reward-informed budget pool
  global survivor / regime specialist / probation / fresh self-deepen lanes
  true1min shard schema filter
  typed primitive gate

Remote launcher:
  builds pack once
  splits candidates into chunks
  runs Phase3CM in bounded parallel workers
  fails loudly on worker failures or zero reward rows
```

## Retired / Superseded Code

```text
phase3du_adaptive_regime_free_deepen_pack.py:
  retired as current route
  retained for provenance replay only

phase3ds_targeted_family_repair_pack.py:
  retained as earlier targeted repair provenance
  not current large-search entry

phase3dt_survivor_expansion_repair_pack.py:
  retained as Phase3DV seed/evidence ancestor
  not current large-search entry
```

Runtime/report folders from local smoke runs should not be promoted as decision
evidence unless explicitly referenced by a run plan or this report.

## Next Gate

After the remote Phase3DV run completes:

```text
1. collect phase3cm_train_reward.csv and phase3dv_cm_reward_summary.json
2. group by budget lane, family, expression skeleton, turnover, horizon stability
3. check whether probation/regime/fresh lanes produce any nontrivial reward pockets
4. decide next budget, not promotion
5. keep X0/R3 read-only
```

Success is not a perfect candidate. Success is evidence that the budget-pool
allocation produces better search information than hard early elimination.
