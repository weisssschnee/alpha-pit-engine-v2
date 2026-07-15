# CN Failure Attribution

Primary attribution: `COMPUTE_OR_IO_BOTTLENECK`.

| Route | Preflight pairs | Evaluated | Blocked | Primary attribution |
|---|---:|---:|---:|---|
| DISCLOSURE_EVENT | 4 | 4 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| FIRSTN_PATH | 5 | 4 | 1 | COMPUTE_OR_IO_BOTTLENECK |
| INTRADAY_STATE_TRANSITION | 4 | 4 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| MARKET_REGIME_CONDITION | 4 | 4 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| MINUTE_STATIC | 5 | 5 | 0 | COMPUTE_OR_IO_BOTTLENECK |
| SLOW_CROSS_SECTIONAL_LEVEL | 5 | 4 | 1 | COMPUTE_OR_IO_BOTTLENECK |
| SLOW_TEMPORAL_CHANGE | 5 | 5 | 0 | COMPUTE_OR_IO_BOTTLENECK |

The route rows above describe only the representative preflight. `NO_GROSS_EDGE`, `CONTROL_NOT_BEATEN`, `CROSS_SEED_INSTABILITY`, and `NO_ALPHA` are not assigned because Strict Stage A did not run. Structural aliasing remains measured separately and is not the stopping cause.
