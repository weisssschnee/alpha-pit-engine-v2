# CN over-control and false-failure audit

- Date: 2026-08-03
- Scope: historical CN train search, A-share replay, mark-to-market replay and
  report-only OOS incidents on 77o
- Authority boundary: audit evidence only; no historical run is retroactively
  promoted or made immutable by this document

## Executive finding

The logs show three distinct classes that had previously been mixed together:

1. **Financially and semantically complete work rejected by operational
   performance gates.**  These are false run invalidations.  Low effective-core
   measurements and a transient free-memory reserve breach were allowed to
   discard completed financial work even though identities, scores, sealed
   reads and artifact hashes remained valid.
2. **Candidate-local A-share outcomes raised as batch-fatal errors.**  Terminal
   open non-liquidation, no executable fills and fractional corporate-action
   shares are evidence about one candidate, not proof that the cohort or
   evaluator crashed.
3. **Genuine authority, semantic or persistence failures.**  Missing optimizer
   feedback, identity/hash drift, wrong thread entitlement, duplicate launcher
   execution, incomplete environment binding and manifest namespace escape
   remain fail-closed defects.

The first two classes caused avoidable recomputation and misleading `RUN_INVALID`
or zero-finalist conclusions.  The third class must remain fatal.

## Evidence reviewed

- `.planning/STATE.md` accepted incident and closure records.
- `D:\ChengboRemote\runtime\run_health_incidents` small `incident.json`
  records and preserved-manifest hashes.
- `src/our_system_phase2/runtime/cn_large_tpe_search_campaign.py` checkpoint
  closure path.
- `src/our_system_phase2/runtime/cn_targeted_search_medium_campaign.py`
  runtime-utilization gate.
- `src/our_system_phase2/services/a_share_executable_replay.py` strict and MTM
  ending-book policies.
- `src/our_system_phase2/services/finalist_economic_admission.py` finalist
  predicates.
- ADR 0012 and ADR 0013.

## Runtime and acceleration inventory

- Official 77o Python:
  `D:\ChengboRemote\venvs\alpha311\Scripts\python.exe`.
- Package matrix: NumPy 2.4.6, pandas 3.0.3, PyArrow 24.0.0, Numba 0.65.1,
  Bottleneck 1.6.0, NumExpr 2.14.1, Polars 1.42.0, Joblib 1.5.3 and
  scikit-learn 1.9.0.
- The active Phase3CM hot path actually uses Polars block reads and multiple
  parallel Numba kernels in streaming expression, support and portfolio
  evaluation.  This is not an "installed but unused Numba" case.
- Historical telemetry identifies cross-sectional rank mapping as the repeated
  straggler.  The runner measures expression DAG, mapping, turnover/cost,
  streaming reduction and checkpoint/finalization separately.
- At this audit the node was idle with about 84.78 GB free memory and no Python
  financial process.  No live campaign was interrupted or restarted.
- The largest accepted winner search reported 7,379 `PAIR_EVALUATED` and 2,836
  productive candidates, with productive throughput 635.59/hour.  Several
  rejected checkpoints had healthy backend-level run status and 19-29 effective
  cores, showing that the fatal decision was driven by a subphase threshold or
  reserve policy rather than lack of completed financial work.
- This audit authorizes no scaled campaign, so a required-pairs/hour launch
  target and ETA are not applicable.  The bounded retest is 32 historical pairs
  plus at most ten new candidate/control pairs; its contract will record actual
  pair/hour, evaluator fill, peak RSS and minimum free memory, but will not use
  an efficiency floor as a financial-validity gate.

### Active acceleration mechanisms

- Polars sidecar/block reading.
- Numba `njit`/`prange` kernels with entitlement-bound thread counts.
- Pair batches 12/24 for active-bar/stock-session in the latest qualified
  runners.
- Expression/report caches with frozen data/split/execution bindings.
- Exact and behavior identity archives and without-replacement availability
  control.

No library installation, worker increase, cache increase or approximate
evaluator is required for the repair.  The limiting issue is control-flow
classification after computation, not a missing acceleration package.

## Incident classification

| Incident family | Observed evidence | Classification | Required treatment |
| --- | --- | --- | --- |
| Stock-session acceleration threshold after completed financial work | Dual-lane checkpoint 004 failed twice around 20 effective cores; compute-efficient checkpoint 004 failed twice because one mapping subphase was about 14.9 versus 16; qualified-successor checkpoint 005 failed twice at 14.36/15.72; winner checkpoint 008 first attempt missed by 0.036 core; terminal search first attempt also failed the same gate | **Operational over-control** | Persist the financial checkpoint when semantic, identity, access and artifact checks pass. Record efficiency as `DEGRADED` and block automatic scaling, not evidence closure. |
| Minimum-free-memory reserve breached after completed financial work | Full-compute checkpoint 002 completed both backends with 18.30 GB minimum free memory, passed acceleration, did not OOM and had zero sealed reads, but was discarded because the fixed reserve was 24 GiB | **Operational over-control** | Keep the 24 GiB check as preflight/admission and live safety control. A completed, hash-valid batch becomes `RESOURCE_HEADROOM_DEGRADED`; only OOM, incomplete output or semantic/hash failure invalidates it. |
| `FINAL_SESSION_UNLIQUIDATED_HOLDINGS` at an arbitrary replay cutoff | Daily NAV already marks all holdings at PIT close. Strict replay additionally requires a flat final open and raises when any sell is blocked on that one session | **Wrong alpha conclusion when used as a veto** | Continuous-book MTM owns alpha economics. Final-open liquidation becomes a separate liquidity stress diagnostic. It must not erase otherwise valid daily PnL. |
| Five-percent ending-holdings cap | ADR 0013 rejects an MTM candidate when either member ends above 5% invested even though an ongoing long-only strategy normally carries a book across an arbitrary cutoff | **Arbitrary admission over-control** | Remove the absolute 5% veto. Report ending exposure, primary/control exposure difference, blocked-sell value and liquidation lag separately. |
| Terminal liquidity, no-fill and fractional-share outcomes aborting a cohort | Historical terminal-liquidity, no-fill and fractional corporate-action incidents stopped replay before pair/root closure | **Candidate-local outcome promoted to batch failure** | Preserve typed immutable candidate evidence and continue the unchanged cohort. This is already partly implemented and must be regression protected. |
| Heterogeneous blocker/success payload inferred into one Parquet schema | Candidate computation completed but aggregate Parquet serialization failed on mixed scalar/structured columns | **Persistence defect, not financial invalidity** | Retain candidate JSON as authority and stable scalar envelope plus canonical payload hash. Already repaired under ADR 0012. |
| Remote `ConvertTo-Json` inspector memory leak | A read-only audit process consumed memory until stopped; Python financial work was not touched | **Monitoring-induced resource incident** | Keep inspectors bounded and ephemeral. Never aggregate large remote object graphs or run persistent inspectors. |
| Missing pair-native feedback / all failed Optuna tells | Financial observations existed but eligible search scores and COMPLETE tells were absent | **Genuine semantic failure** | Remain fail-closed; no optimizer state or financial result reuse. |
| Zero-ask route told without pending population | Optimizer API contract violated for Market/Disclosure zero routes | **Genuine code failure** | Remain fail-closed; explicit zero-route no-op receipt is required. |
| Thread entitlement mismatch, environment drift or missing Python path | Runtime requested 32 threads under a 24-thread lease, or required sidecar environment/import path was absent | **Genuine execution-authority failure** | Fail before financial work where possible; otherwise preserve and recover from the last immutable boundary. |
| Manifest path escape, source/input/hash/identity drift | Checkpoint-local manifests referenced campaign-level paths or authoritative bindings did not match | **Genuine persistence/authority failure** | Remain fail-closed. |
| Detached scheduled task executed twice / closure overwrite | Immediate start plus retained trigger caused a second invocation | **Genuine launcher/idempotency failure** | Triggerless single start and immutable closure overwrite refusal remain required. |

## Source-level cause of false run invalidation

The large-search checkpoint creates `runtime_utilization_gate.json` only after
financial evaluation, optimizer tells and state accumulation.  It then raises
`LARGE_TPE_RUNTIME_ACCELERATION_GATE_FAILED` whenever the aggregate gate status
is not exactly `PASS`.  The gate combines:

- semantic/execution integrity (backend presence, exact-once pairs, cache and
  timing coverage),
- performance diagnostics (effective cores, occupancy and sustained blocks),
- resource headroom (peak RSS and minimum free memory).

Combining these into one fatal status is the principal control defect.  A slow
or memory-tight completed calculation is not mathematically different from a
fast calculation.  It may be unsafe to repeat or scale, but it is not invalid
alpha evidence when all semantic and artifact checks pass.

## Corrected authority boundary for implementation

The replacement contract must separate three independent outcomes:

1. `SEMANTIC_INTEGRITY`: hard gate.  Identity, pair coverage, reward parity,
   split/sealed access, source/input hashes and artifact completeness must pass.
2. `RESOURCE_SAFETY`: hard admission gate before financial work; after a
   completed batch it is evidence and scale authorization, not a retroactive
   mathematical veto unless the run actually OOMs or produces incomplete data.
3. `COMPUTE_EFFICIENCY`: diagnostic only.  It controls tuning and whether a
   successor may scale, never whether a completed batch exists.

For alpha economics:

- real A-share next-session execution, T+1, suspension/limit fills, corporate
  actions and frozen costs remain mandatory;
- daily PIT-close NAV includes realized and unrealized PnL;
- the final book is marked at `FINAL_PIT_CLOSE` without a fabricated sale or
  terminal sell fee;
- cumulative net return and risk-adjusted return must be positive for the
  primary candidate, and matched primary-minus-control economics must also be
  positive;
- final-open liquidation and ending exposure remain reported stress evidence,
  not an arbitrary terminal-flat or 5% admission veto.

## Retest boundary

No rejected historical checkpoint is retroactively promoted and no incomplete
financial artifact is reused.  The bounded retest inputs are:

1. the already immutable 32-pair/64-member historical cohort, reused only to
   verify the corrected classification and summary contract; and
2. the ten development-productive candidates from the closed 288-ask
   primary-absolute search, evaluated for the first time under continuous-book
   train MTM.

Only candidates with positive primary cumulative net return, positive primary
risk-adjusted MTM reward and positive matched economics may be frozen for a
separately authorized unchanged-cohort report-only OOS evaluation.

## Audit decision

`HOLD_RESEARCH`

The audit proves material over-control and justifies an authority/code repair.
It does not itself prove alpha, authorize promotion, or authorize validation,
holdout or 2026 access.
