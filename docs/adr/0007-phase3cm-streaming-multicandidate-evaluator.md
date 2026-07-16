# ADR 0007: Phase3CM streaming multi-candidate evaluator

Status: Accepted for engineering qualification; strict Stage A remains forbidden.

## Context

The candidate-parallel Phase3CM evaluator repeatedly materialized full candidate
signals and portfolio coordinates. That execution shape could not provide a
credible 2,048/4,096-pair resource projection and did not prove that installed
native libraries were active on the real full-market path.

## Decision

Phase3CM engineering qualification uses route-native, time-major development
sidecars and a streaming multi-candidate evaluator. The evaluator preserves the
global `trade_time` cross-sectional barrier while processing bounded calendar
blocks across all physical source shards.

The execution graph has two sharing levels:

- A Value cohort is keyed by backend, raw field surface, observable
  clock/maturity, and window profile. Candidates in one Value cohort may share
  raw reads and expression DAG nodes.
- A Mapping subcohort is keyed by support contract, eligibility contract,
  mapping family, and portfolio mode. Finite/support masks, cross-sectional
  ranks, selections, weights, turnover, and cost never cross a Mapping
  subcohort boundary.

Expression nodes are released after their last deterministic consumer. Pair
support, native batched portfolio kernels, and bounded streaming reducers retain
aggregate state instead of coordinate rows. Periodic atomic checkpoints contain
the real temporal, state/event, portfolio, reducer, completed-block, and
completed-pair-batch continuation payloads; hashes verify those payloads but do
not replace them.

Phase D may tune block size, pair-batch size, process count, thread count, and
cache caps from development-only telemetry. Phase E must use an immutable
execution-plan hash and fails closed at resource gates. At most two heavy
processes and 24 active native compute threads are permitted, with one primary
native thread pool per hot phase.

## Qualification requirements

The evaluator is not qualified by code presence or package installation. A
separately recorded 77o qualification must prove:

- stable-key sidecar parity and full-coordinate evaluator parity;
- exact uninterrupted-versus-resumed output parity;
- real native hot-path invocation and the effective-core CPU gate;
- bounded cache, reducer, checkpoint, and global RSS behavior;
- 1/4/8/16/32 scaling followed by a distinct frozen 32-pair Phase E run;
- zero validation, holdout, and 2026 reads.

## Consequences

- The historical candidate-parallel evaluator remains an existing authority;
  the streaming evaluator is `EXPERIMENTAL` until its explicit qualification
  evidence is accepted.
- Engineering qualification may establish resource readiness only. It cannot
  run strict Stage A, promote a candidate, write adaptive memory, or open 2026.
- If rank/mapping consumes at least 60 percent of compute time, the registered
  bottleneck is `BATCHED_PORTFOLIO_KERNEL_BOTTLENECK`; DAG work remains the
  primary optimization target only when Value DAG compute reaches that threshold.
