# Phase3DV Retirement and Workspace Hygiene 2026-06-30

Decision: `PHASE3DV_WORKSPACE_HYGIENE_RECORDED`

## Route State

```text
current main route:
  phase3dv-budget-pool-self-deepen-pack

retired route:
  phase3du-adaptive-regime-free-deepen-pack
```

`app.py` now blocks the retired Phase3DU route unless the caller passes
`--allow-diagnostic`. This prevents accidental reuse of the rigid deepen/freeze
logic while preserving provenance replay.

## Why Phase3DU Was Retired

Phase3DU tried to deepen useful families, but it still treated too many weak or
fragmented candidates as near-final failures. That contradicted the intended
search behavior for true1min alpha discovery:

```text
wrong behavior:
  find a perfect seed first, then deepen

current behavior:
  allocate bounded compute to acceptable clusters until budget pressure or
  structural blockers justify rejection
```

## What Was Not Deleted

These files remain in source control scope for reproducibility:

```text
src/our_system_phase2/runtime/phase3ds_targeted_family_repair_pack.py
src/our_system_phase2/runtime/phase3dt_survivor_expansion_repair_pack.py
src/our_system_phase2/runtime/phase3du_adaptive_regime_free_deepen_pack.py
```

They should not be used as the current search entry.

## Local Workspace Hygiene Boundary

Untracked local smoke folders exist under:

```text
reports/phase3ds_targeted_family_repair_pack_20260629_local_smoke
reports/phase3dt_survivor_expansion_repair_pack_20260630_local_smoke
reports/phase3du_adaptive_regime_free_deepen_pack_20260630_local_smoke
reports/phase3dv_budget_pool_self_deepen_pack_20260630_local_smoke
runtime/phase3ds_targeted_family_repair_pack_20260629_local_smoke
runtime/phase3dt_survivor_expansion_repair_pack_20260630_local_smoke
runtime/phase3du_adaptive_regime_free_deepen_pack_20260630_local_smoke
runtime/phase3dv_budget_pool_self_deepen_pack_20260630_local_smoke
```

These are smoke/provenance artifacts. They are not the active remote run and
should not be bulk-committed. If disk cleanup is needed, archive them only after
the Phase3DV remote reward report is collected.

## Git Hygiene Rule

Do not use:

```text
git add .
```

Stage only:

```text
app.py
README.md
MIGRATION_MANIFEST.md
reports/PHASE3_TRUE1MIN_ITERATION_TREE_20260623.md
reports/PHASE3DV_BUDGET_POOL_SELF_DEEPEN_20260630.md
reports/PHASE3DV_RETIREMENT_AND_WORKSPACE_HYGIENE_20260630.md
runtime/run_plans/phase3dv_budget_pool_self_deepen_cm_reward_20260630.json
src/our_system_phase2/runtime/phase3dv_budget_pool_self_deepen_pack.py
```

Only include older DS/DT/DU source files if their diffs are explicitly reviewed
and intentionally retained.
