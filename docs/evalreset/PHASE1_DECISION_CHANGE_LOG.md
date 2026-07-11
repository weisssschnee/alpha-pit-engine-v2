# EVALRESET Phase A Decision and Change Log

## 2026-07-11 — Baseline freeze

- Accepted baseline commit `6b3ef3840083fe73fc0a4f4903ca2ad0060c1f88`.
- Created annotated tag `cn-true1min-evalreset-baseline-20260711` and isolated
  branch/worktree. The dirty `main` worktree remains untouched.
- Froze Phase B, positive CEM/UCB/MCTS memory updates, large search, new reward
  fields, and all 2026 forward-performance access.

## 2026-07-11 — Evaluation-role reset

- Classified 2025 validation and holdout as `spent` after candidate-level human
  exposure. Registered `development`, `challenge`, `sealed`, `spent`, and
  `forward` roles.
- Added evaluation access and OOS burn ledgers.
- Replaced permissive feedback projection with a fail-closed development/train
  guard at memory, feedback, and scheduler boundaries.

## 2026-07-11 — Historical Phase3FIX provenance

- Final classification: `PROVENANCE_UNVERIFIED`,
  `NON_REPRODUCIBLE_AS_EXECUTED`, `NOT_VALID_FOR_PROOF`.
- Matching key-file hashes are evidence about copied files only. They do not
  prove the historical environment, untracked files, release, worktree state,
  or invocation. No historical full replay will be used to manufacture proof.

## 2026-07-11 — Collapse wording correction

- Generation is the earliest observed location of structural redundancy:
  24,576 expressions, 131 AST skeletons, skeleton N_eff 33.55.
- This is not evidence of catastrophic or signal-level collapse. The first
  significant signal-level collapse remains unlocated.
- Skeleton and behaviour N_eff are different representations and may not be
  interpreted as a stage-to-stage decline.

## 2026-07-11 — Two-pass signal-sketch audit

- Approved the only Phase A heavy task: deterministic, development-only signal
  sketches followed by fidelity/stability validation.
- Coordinates are selected without returns, labels, validation, holdout,
  performance regimes, or 2026 data.
- Two independent coordinate sets cover trade month, intraday period, stock
  coverage interval, observed listing-age bucket, and neutral bar-activation
  density.
- Every candidate emits activation, rank/sign, quantized-value, SimHash,
  missingness, and coverage-profile sketches. Exact rank/value vectors are
  retained only for the fixed 384 admission candidates for fidelity testing.
- Before seeing full-run clusters, fidelity gates were fixed at: sketch/exact
  correlation ≥ 0.90, pair purity ≥ 0.90, A/B ARI and NMI ≥ 0.80, top-1 share
  difference ≤ 0.03, and boundary misclassification ≤ 0.10. Failure keeps the
  full-generation sketch diagnostic-only and blocks Phase A acceptance.
- A 128-candidate pilot using disjoint A/B stock universes failed the stability
  gates (ARI 0.1100, NMI 0.3616, top-1 difference 0.50) despite near-perfect
  quantization fidelity. That design was stopped before full completion and
  archived as `signal_sketch_pilot_disjoint_unstable`. The corrected design
  uses one shared stratified stock universe with independent A/B dates and
  intraday coordinates, so the stability test isolates coordinate sensitivity
  rather than small-universe composition.
- The first shared-universe pilot also failed (ARI 0.1262, NMI 0.4174, top-1
  difference 0.4844): one date per month left 103/128 B sketches fully missing
  versus 41/128 A. This exposed single-day activation aliasing. Without using
  feature values or performance to choose dates, the coordinate contract was
  widened to three interleaved fixed within-month quantiles per set:
  A=`1/7,3/7,5/7`, B=`2/7,4/7,6/7`.
- The formal 384 admission pass separates candidates with fewer than
  `max(128, 5% of coordinates)` finite observations in either set as
  `signal_coverage_limited`; they are not treated as one economic cluster. Of
  384, 186 were joint-coverage-qualified and 198 limited. Qualified candidates
  passed all preregistered gates: ARI 0.9814, NMI 0.9955, top-1 difference 0,
  sketch/exact correlation 0.99994, cluster-pair purity 1.0, and boundary error
  0. This authorizes, but does not pre-judge, the 24,576-candidate Pass 1.

## 2026-07-11 — Architecture deliverables become acceptance gates

- `CURRENT_ARCHITECTURE.md`, `ARCHITECTURE_BOUNDARY.md`, `EVOLUTION_MAP.md`,
  `graph.json`, `.planning/STATE.md`, this log, the run manifest, and artifact
  index are Phase A deliverables.
- Forbidden feedback edges are machine-readable graph edges and test targets.
- Phase A cannot be declared architecture-accepted or committed without every
  required asset and a clean worktree after the commit.

## 2026-07-11 — Full generation signal-sketch result

- Completed A/B sketches for all 24,576 generated candidates using development
  coordinates only. No label, return, validation, holdout, or 2026 forward
  performance entered coordinate selection or clustering.
- 10,529 candidates were joint-coverage-qualified and 14,047 remained
  coverage-limited. Qualified candidates formed 4,508 consensus clusters with
  N_eff 97.36 and top-1 share 9.43%.
- Full-run stability passed every preregistered gate: ARI 0.9803, NMI 0.9713,
  top-1 share difference 0.00057, sketch/exact correlation 0.99994,
  cluster-pair purity 1.0, and boundary misclassification 0.
- Proxy/admission top-1 shares were 10.55% and 10.22%; strict and final
  coverage-qualified shares were 2.61% and 2.78%. The stagewise review adds no
  post-hoc numeric threshold and finds no significant signal-level collapse or
  downstream concentration amplification.
- Partition 4 recorded one post-write checkpoint `OSError(22)` after both A/B
  rows for `phase3cp_14562` were written. The error log is retained; global
  resume processed zero missing candidates and rewrote the formal partition
  summary with `failure_count=0`.
- The cluster implementation now caches decoded representative sketches. This
  is an execution optimization only; thresholds, ordering, LSH buckets, and
  cluster decisions are unchanged.

## 2026-07-11 — Final verification

- Generated graph: 22 nodes, 18 edges, including all 6 required forbidden
  feedback edges.
- Refreshed artifact index: 14 of 14 formal artifacts exist and are hashed.
- Ledger validation: 9 access records, 4 burn records, and 0 forward-access
  violations.
- Full repository suite: `49 passed in 14.74s`.
- The single Phase A commit gate is satisfied by the commit containing this
  log. Phase B remains frozen pending explicit user acceptance.
