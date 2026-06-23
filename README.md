# Alpha Pit True1min Engine

This repository is a curated migration of the true-1min A-share research chain from
`alpha_pit_data_feature_workspace_20260531`.

It is not a full copy of the legacy workspace. The legacy repository remains the
archive and reference source. This repository is the clean working entry for:

- true `trade_time` 1min shard search
- typed primitive gating
- unsafe limit/event motif quarantine
- search-memory blocked views
- guarded BS/BT/BU fresh search with CEM/UCB feedback guards
- CA bridge into BZ candidate audit
- BZ fragment replay as diagnostic slice validation
- Phase3CM train portfolio Sortino reward audit for search feedback
- Phase3CN feedback memory wiring before multi-arm search restart

## Hard Boundaries

- Do not use old 1D kline paths for true1min claims.
- Do not promote X0/R3 from this repository. They are read-only benchmarks.
- Do not treat proxy IC or active-day metrics as proof.
- Do not treat fragment-level slice Sortino as the search reward.
- Do not let CEM/UCB learn from sparse clean feedback.
- Do not delete unsafe search-memory keys; reclassify them as blocked structures.

## Entrypoint

Use:

```powershell
$env:PYTHONPATH = "src"
G:\PythonProject\.venv\Scripts\python.exe app.py <route> --allow-diagnostic -- <route args>
```

Important routes:

```text
phase3cf-large-search-prelaunch
phase3bs-adaptive-ucb-cem-practice
phase3bt-ast-algorithm-bakeoff
phase3bu-ast-fresh-winner-variants
phase3ca-build-bz-candidate-audit
phase3bz-fragment-replay-audit
phase3cm-train-portfolio-sortino-reward-audit
phase3cn-feedback-memory-smoke
phase3cn-integrated-feedback-smoke
phase3cn-searcher-feedback-smoke
phase3co-multi-arm-scheduler-smoke
phase3cp-reward-gated-medium-search-smoke
phase3cp-real-cm-small-loop
phase3ce-unsafe-motif-quarantine-audit
phase3ce1-search-memory-blocked-view
phase3ce1-g2-input-gate-smoke
phase3ce2-typed-primitive-candidate-pack-canary
phase3ce2-typed-primitive-evaluator-smoke
```

## Current Data Assumption

The canonical true1min shard roots remain external data assets:

```text
local:   runtime/phase3au_aq_only_true1min_sharded_20260611
company: runtime/phase3au_company_full_true1min_sharded_20260611
```

They are not committed here.

## Migration Status

See `MIGRATION_MANIFEST.md` for imported assets and exclusions.

## Iteration Tree

The current external-facing research path is indexed here:

```text
reports/PHASE3_TRUE1MIN_ITERATION_TREE_20260623.md
```
