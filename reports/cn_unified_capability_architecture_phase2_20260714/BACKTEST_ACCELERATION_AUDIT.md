# CN Unified Discovery acceleration audit

Audit date: 2026-07-14  
Execution host: `DESKTOP-77OPJ6F`  
Python: `D:\ChengboRemote\venvs\alpha311\Scripts\python.exe` (3.11.9)

## Package reality

| Package | 77o version | Active in this hot path |
|---|---:|---|
| numpy | 2.4.6 | yes, metric arrays and behavior sketches |
| pandas | 3.0.3 | yes, grouping, rolling, episode and PIT joins |
| pyarrow | 24.0.0 | yes, strict row-group streaming |
| numba | 0.65.1 | yes, selected rolling gates inside `real_market_validation` |
| bottleneck | 1.6.0 | installed; no explicit unified-runner call |
| numexpr | 2.14.1 | installed; no explicit unified-runner call |
| polars | 1.42.0 | installed; unused in the frozen evaluator |
| joblib | 1.5.3 | installed; unused because the safe worker limit is one |
| scikit-learn | 1.9.0 | installed; unused in the frozen evaluator |

Installed libraries are not counted as acceleration unless the active path
imports or calls them.

## Hot path and limiting resources

The Broad Event replay reads one complete shard at a time with pandas and is
the memory peak. Strict active-route evidence then streams all 576 physical
row groups with pyarrow and evaluates only fixed intraday checkpoints. PIT
fundamentals are dominated by many small partition reads and as-of joins.
The limiting resources are therefore mixed I/O and pandas group/rolling CPU,
with shard materialization setting the peak-memory constraint.

Before launch, 77o had no Python worker, 78.93 GiB free of 93.38 GiB physical
memory, 372.66 GiB free virtual memory and 559.21 GiB free on D:.

## Active acceleration and cache safety

- Physical strict data is streamed by row group; it is never concatenated as
  a 446-million-row panel.
- A shared expression cache is scoped to exactly one in-memory proxy frame or
  physical row group. Its key is the canonical expanded expression plus field
  lags; snapshot/split identity is implicit and safe because the cache cannot
  cross a physical chunk or process lifetime.
- Fundamental parquet caches live below the frozen contract hash. That hash
  binds repository code, registry, data release, split, seeds, budgets and
  access settings, so a different experiment cannot reuse them silently.
- Global exact dedup happens before budget accounting. Selection does not read
  validation, holdout, challenge, forward, replay labels or deployability.
- `groupby(sort=False)` is used only where group order is not a research
  signal. Time-sensitive frames are explicitly sorted first.

## Deliberate non-changes and parity

No approximate evaluator, date-alignment change, target change, Polars rewrite
or multi-process pandas fan-out was introduced. The expression cache reuses
the exact same Series returned by the same evaluator on the same frame, so it
does not change metrics. Synthetic execution tests cover candidate and matched
control expressions, including typed temporal/state operations.

`use_fast_context` is not used because this runner invokes the typed expression
engine directly and replacing it with the legacy validation context would
change the frozen semantics. `successive_halving` is disabled because budgets
are fixed and all admitted strict candidates must receive their registered
evidence. A global semaphore is unnecessary for this isolated run; the launch
contract enforces one Python worker.

## Launch contract

- worker limit: one heavy Python process;
- `OMP_NUM_THREADS=1`, `MKL_NUM_THREADS=1`, `NUMEXPR_MAX_THREADS=8`;
- Broad Event: sequential one-shard materialization;
- strict active routes: all 16 shards and all row groups, shared per-chunk
  expression cache;
- PIT fundamentals: frozen-contract-namespaced deterministic cache;
- no online budget/grammar/reward change and no cross-sprint cache or memory;
- abort on repository, registry, contract, preflight or data-release hash
  mismatch.
