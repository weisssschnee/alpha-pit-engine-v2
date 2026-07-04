# Phase3DV Acceleration Audit 2026-06-30

Decision: `PHASE3DV_ACCELERATION_NOT_CPU_SATURATED_MEMORY_BOUND`

## Context

Active run:

```text
task_id: lanjob_20260630_150345_ab2922
node: DESKTOP-77OPJ6F
cpu: AMD Ryzen 9 9950X3D, 16 cores / 32 logical processors
ram: about 96GB visible
route: phase3dv-budget-pool-self-deepen-pack -> phase3cm train reward audit
```

## Package Reality

Python executable:

```text
D:\ChengboRemote\venvs\alpha311\Scripts\python.exe
```

Observed package versions:

```text
numpy: 2.4.6
pandas: 3.0.3
pyarrow: 24.0.0
numba: 0.65.1
bottleneck: 1.6.0
numexpr: 2.14.1
polars: 1.42.0
joblib: 1.5.3
sklearn: 1.9.0
```

Important distinction:

```text
numba and polars are installed, but the current Phase3CM hot path does not
materially use them.
```

## Runtime Sample

20-second process sample:

```text
worker_process_count: 12
heavy worker count: 6
total_cpu_sec_delta: 134.88
total_core_equiv: 6.74
total_machine_cpu_pct_equiv: 21.1%
```

Per heavy worker:

```text
about 1.1 CPU cores each
```

Memory / paging:

```text
free physical memory: about 9GB at probe time
free virtual memory: about 1.2GB at probe time
memory pages/sec: about 4535
```

Disk:

```text
disk bytes/sec: about 18.6MB/s
avg disk read latency: about 0.1ms
```

Interpretation:

```text
The active bottleneck is not disk I/O. It is pandas single-process hot paths plus
large per-worker memory/commit pressure. Adding more workers is unsafe unless
per-worker memory is reduced first.
```

## Active Acceleration Mechanisms

Confirmed active or present in Phase3CM:

```text
pyarrow parquet column-pruned reads
factor expression cache
feature matrix cache
operator subtree cache
fast_mode
numexpr thread cap
process-level chunk parallelism
```

These are useful, but they do not make the hot path fully multi-core inside each
worker.

## Hot Path

The current Phase3CM reward path is still dominated by pandas operations:

```text
expression evaluation
cross-section ranking / zscore / transforms
trade_time / trade_date groupby
portfolio daily aggregation
regime aggregation
```

Several operations still use pandas `groupby(sort=True)` where order is needed
or has not yet been parity-proven safe to change. Some expression primitives use
`groupby(sort=False)`, but that does not cover the whole reward path.

## Why CPU Looks Low

This is expected under the current design:

```text
6 heavy workers * about 1.1 cores per worker = about 6.7 cores
6.7 / 32 logical processors = about 21% total CPU
```

The low CPU reading does not mean the job is idle. It means the current unit of
work is mostly single-worker pandas computation with large memory residency.

## Why Memory Looks High

Each worker independently reads shard slices and builds its own caches:

```text
input panel slices
expression outputs
feature matrix cache
operator subtree cache
portfolio/reward intermediate frames
```

The caches reduce repeated expression work, but they increase per-worker memory.
At 6 heavy workers, memory is already close enough to the limit that 8 workers
was unsafe.

## Current Safe Worker Limit

Current active run:

```text
6 heavy workers
```

Do not raise this run to 8 workers. The prior 8-worker attempt pushed free
physical memory to roughly 0.8GB, and the current 6-worker run already showed
low free virtual memory.

## Next Launch Contract

For throughput, the next search should be two-stage:

```text
Stage A: screening reward
  workers: 10-12
  max_shards: 3-4
  sample_trade_times_per_shard: 192-256
  chunk_size: 64-96
  operator_cache_max_entries: 64-128
  feature_matrix_cache_max_windows: 2
  purpose: high-throughput train reward screening

Stage B: fidelity reward
  workers: 4-6
  max_shards: 6
  sample_trade_times_per_shard: 384
  chunk_size: 64-128
  operator_cache_max_entries: 256
  feature_matrix_cache_max_windows: 4
  purpose: confirm top candidates only
```

This is faster than forcing the full-fidelity reward on every candidate.

## Code-Level Acceleration Work

High-value next improvements:

```text
1. Precompute trade_time, trade_date, code group codes per shard.
2. Replace repeated groupby ranking / portfolio aggregation loops with numpy or numba kernels where parity-safe.
3. Batch candidates by shared expression skeleton so operator results are reused before process fanout.
4. Add a dynamic worker controller using free physical memory, free virtual memory, and pages/sec.
5. Use polars or Arrow only after a parity harness proves identical rank / lag / split semantics.
```

Do not simply enable numba everywhere. The hot path must first be isolated into
array kernels. Installing numba alone has no effect.

## Decision

The current acceleration is partially effective but not sufficient for full CPU
utilization.

Current run can continue as a high-fidelity reward audit, but the next run should
not repeat this exact shape for the whole candidate pool. It should use a
memory-lighter screening stage, then spend full-fidelity Phase3CM only on the
best budget-pool survivors.
