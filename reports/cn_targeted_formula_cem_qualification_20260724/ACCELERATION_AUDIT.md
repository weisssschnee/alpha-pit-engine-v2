# Targeted Formula/CEM Qualification Acceleration Audit

## Runtime reality

- Host: `DESKTOP-77OPJ6F` (16 physical / 32 logical CPUs).
- Interpreter: `D:\ChengboRemote\venvs\alpha311\Scripts\python.exe`, Python 3.11.9.
- Packages: numpy 2.4.6, pandas 3.0.3, pyarrow 24.0.0, numba 0.65.1,
  bottleneck 1.6.0, numexpr 2.14.1, polars 1.42.0, joblib 1.5.3 and
  scikit-learn 1.9.0.
- Prelaunch state: no matching heavy Python/Phase3CM process; 86,256,287,744
  bytes free physical memory.

Installed packages are not treated as acceleration by themselves. The active
evaluation path is the existing
`TimeMajorBlockReader -> SharedMultiCandidateDAG -> Numba portfolio kernel ->
streaming reducer`. PyArrow supplies sidecar I/O. Polars is used for
materialization, not the evaluator inner loop. Numba and vectorized NumPy are
active in the evaluator hot path.

## Measured stock-session baseline

The immutable source campaign's existing stock-session checkpoint used the
same Phase3CM evaluator with two compute threads. It measured 2.003 effective
cores, 6.26% logical-host occupancy, 9.66% Task Manager-equivalent mean CPU,
3,616 matched pairs/hour and 643,657,728 bytes peak RSS. Its allocated
two-thread kernel was saturated; the host was not.

That mixed-route checkpoint ran beside a memory-heavy active-bar workload and
reached only 14,528,786,432 bytes free memory. The new qualification is
stock-session-only and must independently maintain at least 24 GiB free
memory. The old mixed-run minimum is not reused as a safety pass.

## Cache and semantics

- Shared evaluator cache: enabled, fixed cap 8 GiB.
- Key authority:
  `execution_plan_hash + dag_plan_hash + block_boundary +
  candidate_value_cohort`; the execution plan binds the expression,
  evaluator, sidecar snapshot, split, horizons, cost and portfolio mapping.
- Pair batch: at most 4.
- `use_fast_context`: not applicable; there is no alternate fast evaluator.
- `successive_halving`: disabled.
- Global heavy-process limit: one qualification process; checkpoints and arms
  execute serially.
- No approximate evaluator, reduced horizon, reduced data, altered reward or
  altered matched control is authorized.

The only acceleration-related code change is projecting the existing runtime
gate onto the stock-session backend actually selected by this route. It does
not change expressions, metrics, portfolio mapping or access boundaries.

## Launch contract

Run with two stock-session compute threads, pair batch four, fixed 8 GiB cache,
one heavy qualification process and immutable per-arm checkpoints. Record
effective cores, logical-host CPU, Task Manager-equivalent CPU, peak RSS,
minimum free memory, cache peak and matched pairs/hour for every checkpoint.

The measured two-thread stock-session kernel is sufficient for this bounded
216-pair maximum and avoids a speculative concurrency rewrite. It does not
satisfy the contract's 75% whole-host utilization gate or prove a
full-host-native SMT ceiling. Therefore the performance result may be
`PARTIAL`, and `PARTIAL` cannot authorize a later large search. A future
large-search launcher would need evidence-backed pair-level concurrency or
another already-qualified scheduling mechanism; this qualification does not
handcraft one.

## V2 measured result

Retry V2 completed 201 full-coordinate pairs: 57 OLD uniform, 72 expanded
uniform and 72 expanded CEM. The three arms measured 2.00/2.06/2.00 median
effective stock-session cores, 2,879.53/3,749.78/3,343.86 pairs/hour and
1,442.72/1,816.39/1,673.53 pairs per estimated effective CPU-hour. Whole-host
occupancy remained 6.24%/6.45%/6.24%, so the frozen 75% gate was not met.

The final performance result is `FAIL`, rather than `PARTIAL`, because two
duplicated stale recursive monitors from the historical V1 root drove sampled
free memory below 24 GiB before they were identified. They were unrelated to
the evaluator, had no children and together held about 77.82 GB working set.
Their termination restored free memory to 79.83 GiB without stopping the
campaign or altering old evidence. The incident remains immutable run-health
evidence and does not change any financial comparison.

Cache peak remained below 2.75 MiB, pair batch remained four, peak evaluator
RSS remained below 428 MiB, all nine Phase3CM receipts completed and all sealed
data reads remained zero. No separate performance replay is authorized.
