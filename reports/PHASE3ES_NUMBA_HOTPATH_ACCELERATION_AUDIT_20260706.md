# Phase3ES Numba Hot-Path Acceleration Audit - 2026-07-06

## Finding

Phase3ES did use numba, but the prior acceleration coverage was incomplete.
Runtime summaries showed `numba_rank_enabled=true`, `fast_portfolio_loop=true`,
and millions of `numba_rank_calls`. The crash path was elsewhere:

- `ValidRatioGate`
- `MaskedZScore`
- `CSRank`
- `SafeCSResidual`
- `CSResidual`

The failed 8-shard run hit pandas/numpy memory pressure inside rolling/groupby
and cross-sectional layout construction. The representative failures were
`ArrayMemoryError` and `MemoryError` while allocating small arrays after process
commit had already spiked.

## Runtime Evidence

The failed Phase3ES run on DESKTOP-77OPJ6F used 8 shard workers. Aggregate
Python private memory reached roughly 239GB, while physical memory was about
93GB. Some workers failed while trying to allocate only 15-32MB, which is
consistent with process commit pressure and allocator fragmentation after very
large pandas/groupby workloads.

The low-memory recovery run with 2 heavy workers completed candidate partial
rows without stderr during the audited window. Each worker still reached tens
of GB private memory, confirming that 8-way CM parallelism was too aggressive
for this evaluator shape.

## Patch

`src/our_system_phase2/services/real_market_validation.py` now adds:

- optional `numba.njit` acceleration kernels for rolling valid-ratio and
  rolling event-count primitives;
- evaluator-local group layout caching through `DataFrame.attrs`;
- shared cross-section/code layout reuse for `CSRank`, `ZScore`,
  `CSResidual`, `SafeCSResidual`, `ValidRatioGate`, and event-count primitives.

This is intentionally scoped to semantics-preserving hot paths. Persistent
operator caches remain Series-only and are not used to store internal layout
metadata.

## Validation

Local validation completed:

- `py_compile` passed for `real_market_validation.py`.
- A parity smoke test passed for:
  - `ValidRatioGate`
  - `EventCount`
  - `WindowStateCount`
  - `MaskedZScore`
  - `CSRank(ValidRatioGate(...))`
  - `SafeCSResidual`

Remote DESKTOP-77OPJ6F validation:

- patched file uploaded to the active true1min engine snapshot;
- remote `py_compile` passed.

## Next Launch Contract

For Phase3ES/Phase3ET-style CM reward runs on DESKTOP-77OPJ6F:

- use the patched evaluator;
- keep persistent expression/operator/feature caches enabled;
- do not use 8 concurrent shard CM workers;
- prefer 2 workers for recovery and 3 workers for next large CM run;
- treat 4 workers as a canary only after memory telemetry confirms headroom;
- keep `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, and `NUMEXPR_MAX_THREADS<=2`.

Fresh generation may be large, but CM train-sortino reward must be split into
bounded worker groups until the evaluator has a fuller array-native rewrite.
