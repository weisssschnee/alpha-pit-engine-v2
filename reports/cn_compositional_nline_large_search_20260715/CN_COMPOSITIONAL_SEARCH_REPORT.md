# CN Compositional N-line Bounded Search Epoch 1

## Outcome

`CN_COMPOSITIONAL_SEARCH_COMPUTE_BOTTLENECK`

The expression system materially expanded the structural and signal hypothesis space, but the current minute evaluator did not qualify for the minimum 2,048-pair full-coordinate budget. Strict Stage A was therefore not started. This is an engineering capacity conclusion, not a no-Alpha conclusion.

## Delivered research results

- Proposal attempts: 200,000; legal exact-unique primaries: 28,640 (natural underfill against the 30,000 target).
- Structural pre-admission: 24,000; diversity admission: 12,000 pairs.
- Development behavior audit: 12,375 exact behaviors, 11,978 clusters, N_eff=426.453, top-1=4.435%.
- A/B sketch stability and exact-fidelity gates passed for active=True and session=True; no validation, holdout, or 2026 data was read.
- Frozen cost preflight: 32 pairs / 64 calls; 30 pairs evaluated and 2 support-blocked.

## Capacity result

The sampled active preflight covered all 16 shards but only 240 fixed minute coordinates per shard. A separate one-pair calibration disabled sampling and targeted the complete 446,443,583-row development release. It did not complete before the 120-minute resource gate, consumed 120.22 CPU minutes, read 12.85 GiB, wrote no result, and peaked at 36.33 GiB. With a 20 GiB host reserve, the 77o host supports at most 2 such workers before candidate-batch growth.

The current evaluator keeps full-series expression and reward rows in process memory. Consequently it has no qualified bounded batch size for 2,048 pairs, and a linear lower-bound projection is 48.67 days for the active share even before session work and coordination overhead. Running Stage A under the 240-coordinate sample would be cheaper, but would not satisfy the command's full-release strict claim.

## Route preflight

| Route | Preflight pairs | Evaluated | Blocked | Primary attribution |
|---|---:|---:|---:|---|
| DISCLOSURE_EVENT | 4 | 4 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| FIRSTN_PATH | 5 | 4 | 1 | COMPUTE_OR_IO_BOTTLENECK |
| INTRADAY_STATE_TRANSITION | 4 | 4 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| MARKET_REGIME_CONDITION | 4 | 4 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| MINUTE_STATIC | 5 | 5 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| SLOW_CROSS_SECTIONAL_LEVEL | 5 | 4 | 1 | COMPUTE_OR_IO_BOTTLENECK |
| SLOW_TEMPORAL_CHANGE | 5 | 5 | 0 | COMPUTE_OR_IO_BOTTLENECK |

The diagnostic matched-positive rows in this preflight are retained as observations only. They were not selected, promoted, cross-seed reproduced, or written to adaptive memory.

## Required next engineering step

Replace per-worker full-series retention with a bounded streaming DAG evaluator: materialize reusable field blocks once per shard, evaluate multiple candidates in bounded batches, evict expression/subtree series deterministically, write only pair summaries by default, and repeat the same 32-pair full-coordinate preflight. Only after that preflight demonstrates capacity should the frozen 4,096-pair Stage A start.

## Boundaries

`GLOBAL_FORMAL_SEARCH=FORBIDDEN`; `PROMOTION=FORBIDDEN`; `VALIDATION_FEEDBACK=FORBIDDEN`; `HOLDOUT_FEEDBACK=FORBIDDEN`; `FORWARD_2026=SEALED`; `CROSS_SPRINT_MEMORY=FORBIDDEN`.
