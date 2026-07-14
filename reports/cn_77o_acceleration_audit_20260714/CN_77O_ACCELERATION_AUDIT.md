# CN 77o acceleration audit

Audit date: 2026-07-14
Run: `cn_unified_capability_f8169e1`
Frozen repo SHA: `f8169e16c63bb1267fb5eeffe6f97cfe1186733e`
Evidence boundary: runtime and static-path diagnostics only; no performance selection or sealed data access.

## Finding

The unified strict-development scan is materially underusing 77o. At
2026-07-14T18:02:34+08:00 it had run for about 79.6 minutes and accumulated
4,759.5 CPU seconds, which is approximately one fully occupied logical core.
The host exposes 32 logical processors and about 100 GB physical memory. The
heavy worker used about 1.24 GB while roughly 79 GB remained free. The primary
limit is therefore the serial pandas/expression hot path, not memory capacity.

The temporary LAN outage was not a 32-core saturation event. The host boot time
did not change, the same process IDs survived, the atomic run state remained
`RUNNING`, and stderr remained empty after connectivity returned.

## Environment and package reality

Official executable:

```text
D:\ChengboRemote\venvs\alpha311\Scripts\python.exe
```

| Package | Version | Active in strict hot path |
| --- | ---: | --- |
| numpy | 2.4.6 | Yes |
| pandas | 3.0.3 | Yes; dominant dataframe/groupby work |
| pyarrow | 24.0.0 | Yes; column-selective row-group reads |
| numba | 0.65.1 | Partial; selected evaluator kernels only |
| bottleneck | 1.6.0 | Indirect pandas dependency; no explicit hot-path call |
| numexpr | 2.14.1 | Installed; no explicit strict-run expression call |
| polars | 1.42.0 | No |
| joblib | 1.5.3 | No |
| scikit-learn | 1.9.0 | No |

Installed packages are not counted as acceleration unless the active code path
calls them.

## Active hot path

`_stream_full_active_evidence` in
`src/our_system_phase2/runtime/cn_unified_capability_discovery.py`:

1. opens all 16 development files and all 576 row groups;
2. decodes the selected columns for all 446,443,583 minute rows;
3. sorts every row group by code and time;
4. constructs the same-session future target;
5. evaluates strict expressions one candidate at a time;
6. applies checkpoint masks only after decoding and expression evaluation;
7. stores per-candidate arrays and concatenates them after the scan;
8. computes pandas groupby/rank/correlation metrics serially.

The run contract sets one worker, `OMP_NUM_THREADS=1`, and
`MKL_NUM_THREADS=1`. The evaluator creates a reusable layout context within one
expression tree, but the unified runner does not share that context across
candidates. Different candidates can therefore rebuild equivalent code and
cross-sectional group layouts.

## Acceleration that is genuinely active

- PyArrow reads only required columns from each row group.
- Most pandas groupby calls use `sort=False`.
- One row-group-local expression cache reuses identical expression nodes.
- Supported rank, z-score, rolling-validity and event-count kernels can use
  NumPy/Numba implementations.
- Fundamental proxy materialization is cached under the frozen contract hash.
- The completed Broad Event full-16 replay is reused only after code, data,
  seed, pack, output and manifest hash validation.
- Checkpoint masks reduce retained evidence rows, although they do not yet
  reduce full minute decoding or most expression work.

## Acceleration that is not active

- No shard-parallel strict evaluation.
- No cross-process `global_worker_limit` scheduler beyond the fixed value one.
- No unified-run `use_fast_context` contract.
- No context shared across separate candidate expressions.
- No Polars, Joblib, or sklearn execution in the strict hot path.
- No successive halving. This is deliberately inappropriate for the already
  frozen strict budget unless separately specified before results are seen.
- No provenance-keyed per-shard strict evidence checkpoint, so an interrupted
  strict scan cannot resume at a completed shard boundary.
- No online metric aggregation; arrays are accumulated and concatenated after
  the full scan.

## Cache and selection safety

The existing fundamental cache directory is namespaced by the frozen contract
hash and field ID. The contract binds the repo, registry, data release, split,
seeds and budgets. Broad replay reuse additionally validates every dependency
and output hash. Row-group expression caches are ephemeral and cannot leak
between releases. No validation, holdout, forward, replay pass/fail, survivor,
or final cluster label enters selection.

A future persistent strict cache must key at least:

```text
canonical expression
evaluator/code version
data release hash
split manifest hash
field registry hash
delay and horizon
neutralization and cost settings
strict config hash
route and support-unit contract
shard and row-group identity
```

## Changes applied to the running job

None. The frozen job is not interrupted or mutated. Acceleration changes require
an exact before/after parity run and a new frozen repo SHA.

## Recommended safe concurrency

Start with four heavy workers, each owning disjoint physical shards. Do not run
four candidate workers against the same row groups. Four workers leave a wide
memory margin based on the observed 1.2-1.7 GB worker footprint and reduce disk
head contention compared with an immediate jump to eight or more. Benchmark
one, two and four workers on the same frozen row-group set before increasing
the cap.

## Next launch contract

```text
use_fast_context = REQUIRED
global_worker_limit = 4
worker_partition = DISJOINT_SHARDS
successive_halving = DISABLED_FOR_FROZEN_STRICT_EVIDENCE
row_group_expression_cache = REQUIRED
shared_evaluation_layout_per_row_group = REQUIRED
provenance_keyed_shard_checkpoint = REQUIRED
checkpoint_resume_hash_validation = REQUIRED
online_metric_sufficient_statistics = REQUIRED_WHERE_PARITY_PROVEN
before_after_semantics_parity = REQUIRED
validation_holdout_2026 = FORBIDDEN
```

Priority order:

1. expose and share one evaluation layout/context per row group;
2. add atomic, provenance-bound shard checkpoints;
3. run four disjoint-shard workers and deterministically reduce metrics;
4. move rank/IC/turnover/behavior statistics to streaming sufficient-state
   reducers where exact parity is proven;
5. add route-aware materialization so checkpoint-only static routes do not pay
   for unnecessary full-minute expression work.

No approximation may replace the strict evaluator for evidence or promotion.
