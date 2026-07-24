# V3 Acceleration Audit

`PERFORMANCE_CONTRACT=NOT_RUN_OLD_SUPPLY_HARD_BLOCKER`.

The official 77o process performed only Registry/discovery/sidecar/archive
binding and the structural catalog-capacity check. It completed in 2.48 seconds
and stopped at 30 authorized atomic pairs versus the required 144. Running the
sampled evaluator, measuring whole-host CPU occupancy, or invoking CEM would
have violated the frozen causal order and provided no useful performance
evidence.

The deployed Python environment retained the existing native stack: NumPy
2.4.6, pandas 3.0.3, PyArrow 24.0.0, Numba 0.65.1, Bottleneck 1.6.0, NumExpr
2.14.1, Polars 1.42.0, joblib 1.5.3, and scikit-learn 1.9.0. The unchanged
Phase3CM path retains its existing Numba kernels, shared DAG cache, and fixed
8 GiB cache cap, but none was invoked by this fail-fast gate.

A post-run nested PowerShell JSON diagnostic—not the official task—expanded to
about 15.7 GiB while serializing process objects. It was terminated by exact
PID/start-time verification and replaced with a flat verifier. The flat
verification completed immediately, all hashes matched, and free memory was
81.47 GiB. This is monitor-path run health only and has no route-health or
financial meaning.
