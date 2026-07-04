# Phase3DI Generator Fill Accounting And Preavoid

Date: 2026-06-28

## Decision

The current true-1min expression space is not exhausted. The earlier low output
rate came from generator reachability and accounting defects:

- accepted candidates were massively overproduced and then silently truncated by
  emit caps;
- memory/skeleton filters were applied after candidate construction instead of
  being visible as sampler pre-avoid;
- `random_orthogonal` previously behaved too close to existing RX-style
  template families;
- `event_state` was reachable but was being dominated by unsafe skeleton and
  old-memory collisions.

Phase3DI keeps the typed primitive gate strict. It does not relax unsafe
skeleton policy and does not promote any candidate. It only fixes generator
accounting and sampler efficiency before reward-gated search.

## Code Changes

- Added generation accounting columns:
  - `raw_attempts`
  - `accepted_unique`
  - `emitted_produced`
  - `dropped_by_emit_cap`
  - `dropped_by_diversity_cap`
  - `dropped_by_global_pool`
  - `accepted_to_emitted_unexplained`
  - `pre_avoided_memory_expr`
  - `pre_avoided_unsafe_skeleton`
  - top memory / unsafe / preavoid family summaries
- Added target-aware pool limits for RX, event-state, turnover-aware, CEM, and
  hybrid generators.
- Added a true `orthogonal` arm rather than reusing reversed RX output.
- Moved known memory expression and blocked skeleton checks before typed
  validation, so old structures are accounted as sampler pre-avoid.
- Kept typed gate and unsafe skeleton blocking intact.

## Remote 77 Probe Results

Machine:

```text
DESKTOP-77OPJ6F
repo: D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260627_222934_68d1d62eb94e_memfill
shards: D:\ChengboRemote\data\phase3cy_true1min_sidecar_augmented_shards_20260626
available fields: 121
```

### Before poolcap

Output:

```text
reports\phase3df_generation_drop_accounting_probe_20260628
```

Key totals:

```text
requested_budget: 8192
produced_count: 8192
raw_attempts: 3,632,876
accepted_unique: 2,539,122
dropped_by_emit_cap: 2,524,147
dropped_by_diversity_cap: 6,420
dropped_by_global_pool: 363
accepted_to_emitted_unexplained: 0
reject_memory_expr: 111,068
reject_typed_gate: 54,781
reject_unsafe_skeleton: 927,116
```

Interpretation: the accepted-to-produced blind spot was explained. Most loss
was emit-cap truncation, not an unexplained bug.

### After poolcap

Output:

```text
reports\phase3dh_generation_poolcap_probe_20260628
```

Key totals:

```text
requested_budget: 8192
produced_count: 8192
raw_attempts: 825,450
accepted_unique: 47,432
accepted_to_emitted_unexplained: 0
reject_memory_expr: 80,916
reject_typed_gate: 6,720
reject_unsafe_skeleton: 690,382
```

Interpretation: target-aware pool limits reduced raw attempts materially while
still filling the requested candidate budget.

### After preavoid

Output:

```text
reports\phase3di_generation_preavoid_probe_20260628
```

Key totals:

```text
requested_budget: 8192
produced_count: 8192
raw_attempts: 99,956
accepted_unique: 47,432
accepted_to_emitted_unexplained: 0
reject_memory_expr: 0
reject_typed_gate: 16,712
reject_unsafe_skeleton: 35,812
pre_avoided_memory_expr: 140,485
pre_avoided_unsafe_skeleton: 1,526,485
```

Interpretation: known memory and skeleton collisions are now visible as
pre-avoid rather than being repeatedly materialized. The route still produces a
full 8192-candidate generation batch.

## Current Reward Canary

Started on DESKTOP-77OPJ6F:

```text
job: lanjob_20260628_020115_821445
script: D:\ChengboRemote\runtime\lan77_phase3dj_reward_canary_poolcap_preavoid_20260628.ps1
report: reports\phase3dj_reward_canary_poolcap_preavoid_20260628
runtime: runtime\phase3dj_reward_canary_poolcap_preavoid_20260628
```

Configuration:

```text
generation_budget: 32768
ca_top_n: 8192
cm_candidate_limit: 2048
cm_selection_mode: arm_balanced
cm_max_shards: 8
cm_sample_trade_times_per_shard: 768
cm_horizons: 1,5,15,30,60
cm_workers: 14
```

Memory roots were explicitly curated to avoid treating generation-only probe
outputs as already searched reward candidates.

## Next Gate

Do not expand search again until `phase3dj` finishes and the reward CSV is
audited for:

- train day Sortino distribution;
- validation and holdout report-only metrics;
- followup count;
- arm attribution;
- field family attribution;
- turnover/cost blockers;
- whether proxy rank aligns with real train reward.
