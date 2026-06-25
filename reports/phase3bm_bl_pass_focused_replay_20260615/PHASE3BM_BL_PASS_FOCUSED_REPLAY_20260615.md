# Phase3BM BL Pass Focused Replay 2026-06-15

Decision: `PHASE3BM_BL_PASS_FOCUSED_REPLAY_COMPLETE_DIAGNOSTIC_ONLY`

## Scope

- input BL pass candidates: `2`
- true-1min shard panels: `16`
- sampled signal trade_times per shard: `240`
- total eval rows: `1238882`

## Candidate Decisions

| bk_rank | h | fields | aligned_ic | spread | turnover | BM decision | blockers |
|---:|---:|---|---:|---:|---:|---|---|
| 3 | 30 | `close|m1_first30_vwap|vwap` | 0.05655263242 | 6.009974225e-05 | 0.7369090727 | `bm_crowded_sibling_or_watch` | `direction_adjusted_signal_crowding_ge_0.70` |
| 4 | 5 | `close|m1_first30_vwap` | 0.06043644284 | 0.0001085388469 | 0.7633653592 | `bm_crowded_sibling_or_watch` | `direction_adjusted_signal_crowding_ge_0.70` |

## Direction-Adjusted Crowding

| left rank | right rank | raw corr | directed corr | crowding |
|---:|---:|---:|---:|---|
| 3 | 4 | -0.745240865 | 0.745240865 | `True` |

## Boundary

- This is true `trade_time` 1min materialization with contiguous warmup windows.
- Direction-adjusted correlation is used for economic crowding.
- X0/R3 remains read-only; no production or promotion decision.
