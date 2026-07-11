# CN true1min EVALRESET Phase 1 Baseline and Plan

Date: 2026-07-11

Decision: `PHASE1_COMMITTED_AWAITING_USER_ACCEPTANCE_SECOND_PHASE_FROZEN`

## Execution Order

1. Freeze and identify the Git/data/evaluation baseline.
2. Register evaluation roles and burn history.
3. Map all automated and human feedback paths.
4. Measure identity, lineage, structure, and train-only behaviour concentration.
5. Enforce a machine boundary against candidate-level non-development feedback.
6. Test and commit Phase 1 independently; do not start hypothesis-space
   redesign until Phase 1 is accepted.

## Git Baseline

- Baseline commit: `6b3ef3840083fe73fc0a4f4903ca2ad0060c1f88`
- Remote relation at takeover: `main...origin/main`, ahead `0`, behind `0`
- Annotated tag: `cn-true1min-evalreset-baseline-20260711`
- Work branch: `codex/evalreset-collapse-audit-20260711`
- Worktree: `G:\Project_V7_Rotation\alpha_pit_true1min_engine_evalreset_20260711`
- The original worktree remains untouched with 4 tracked modifications and 25
  untracked paths. Those assets are explicitly outside the baseline tag.

## Data Release Verification

Footer-only verification was run against the 2024-2025 assets; no 2026
performance was read.

| release | shards | rows | columns | schema variants | footer fingerprint |
|---|---:|---:|---:|---:|---|
| base | 16 | 598,061,503 | 49 | 1 | `3f0655fb829ea36d68f0263897bb2db7a0126a3fdf9edb37ff4d27f69a115819` |
| sidecar augmented | 16 | 598,061,503 | 121 | 1 | `04d4e94ebdab11a0dfbc406dcd931f79a0d551885471e61963e8b4392e0724f0` |

The augmented release preserves every shard row count. Its release manifest is
SHA256 `FBE766193E91BB629D327CCDB8BD0DABFECAA6AD03A59685DBF20EB76C4E7443`.
Mechanical `source_date < exec_date` lag is verified by the builder contract;
real announcement/vendor publication-time PIT remains unresolved for the 59
external context fields.

## Split Verification

- Manifest SHA256: `FAB9FB17642595456E10C4AD44357193F2DCDC1D39EDD785B8298FBE9CA22241`
- 485 unique sorted dates, 2024-01-02 through 2025-12-31.
- Development/train: 364 dates, optimizer usage `allowed`.
- Validation: 73 dates, manifest usage `report_only`, now role `spent`.
- Holdout: 48 dates, manifest usage `report_only`, now role `spent`.
- Challenge and sealed roles are registered but unassigned.
- 2026 remains separate forward data; performance was not accessed.

## Candidate Pack Verification

The original Phase3FIX artifacts were read from `DESKTOP-77OPJ6F` through the
LAN read-only route and copied to the external evidence directory
`G:\Project_V7_Rotation\cn_evalreset_evidence_20260711`.

| stage | rows | SHA256 |
|---|---:|---|
| generation | 24,576 | `0046B98FCF0CF2FB737F0244D782E59414BCC01C900282C3336D3D3214F9860E` |
| CA proxy | 1,536 | `81B52F4AEB4A4ED67F95D141B61A395FB33802FEBBA10F81AA052E623E738FD5` |
| CM admission | 384 | `A340EA6364620465C1754AA305E5E39048011B9C73FCB973975102E8B5B043F3` |
| fixed-calendar semantic-valid reward | 324 | `BF141D4983E52F3895F9FF116DBFF1BC909AB3A33DDA50FF108F23C14A50D4B8` |

The run summary says `PRE_CM_SEMANTIC_GATE_DISABLED`: 384 entered CM and the 60
semantic-degenerate candidates were quarantined only during exact recovery.

## Evaluator Verification

The active evaluator and recovery code are unchanged from baseline commit
`6b3ef38` and were fingerprinted:

| component | SHA256 |
|---|---|
| `phase3cm_train_portfolio_sortino_reward_audit.py` | `6A1AFDE20EE26143025E5BA72C561B3D5E20E5E6B35135A71AC4F88CC7FBFA60` |
| `recover_phase3cm_exact_reward_atoms.py` | `0595747CBBB57D5F4E4D8EBD6FB734F54C65EDFEBAB349BE583B826A0349E99D` |
| `phase3cp_real_cm_small_loop.py` | `A7993CEDB8E522CCA13B621313A42C7CADC03C7F7491E18F3BDC0211790F42D6` |

Focused contract regression passed: global fixed split, semantic-only no-label
integration, and exact atom recovery, `4 passed in 4.93s`.

Boundary: the fixed-calendar table is an exact re-aggregation of existing atom
rows, not a full evaluator replay from baseline source. The original Phase3FIX
remote workspace had no Git commit manifest. It is formally classified as
`PROVENANCE_UNVERIFIED`, `NON_REPRODUCIBLE_AS_EXECUTED`, and
`NOT_VALID_FOR_PROOF`. Matching key-file hashes do not prove an identical
environment, untracked-file set, data release, worktree state, or invocation.

## Structural Redundancy and Collapse Status

Generation is the earliest stage where structural redundancy is currently
observed. The 24,576 exact expressions occupy 131 AST skeletons; skeleton
`N_eff=33.55`, largest-skeleton share `9.31%`, and average multiplicity is
`187.60` trials per skeleton. This proves that nominal trials are not
independent syntax hypotheses. It does not establish a catastrophic or
signal-level collapse, and the proxy/admission effect remains undetermined.

At strict reward, 202 of 324 semantic-valid candidates have at least 250
fixed-calendar train dates suitable for behaviour comparison. They contain 200
exact daily behaviour identities and 84 deterministic all-pairs correlation
clusters at threshold 0.95; behaviour-cluster `N_eff=29.40`, largest share
`8.42%`, and effective trial multiplicity `6.87`. The other 122 candidates are
coverage-limited and are not forced into a cluster.

The deterministic development-only signal audit completed for all 24,576
candidates. Of these, 10,529 met the joint A/B coverage gate and formed 4,508
consensus clusters. Generation signal-cluster `N_eff=97.36`, top-1 share is
`9.43%` among signal-qualified candidates (`4.04%` of all generated
candidates), and top-3 share is `12.33%`.

All preregistered sketch gates passed: A/B ARI `0.9803`, NMI `0.9713`, top-1
share difference `0.00057`, sketch/exact correlation `0.99994`, cluster-pair
purity `1.0`, and boundary misclassification `0` across 33,481 fidelity pairs.
The 14,047 coverage-limited candidates remain separate and are not treated as
one economic signal cluster.

| stage | rows | signal-qualified | clusters | N_eff | top-1 eligible | cluster survival |
|---|---:|---:|---:|---:|---:|---:|
| generation | 24,576 | 10,529 | 4,508 | 97.36 | 9.43% | 100.00% |
| proxy | 1,536 | 654 | 501 | 74.18 | 10.55% | 11.11% |
| admission | 384 | 186 | 157 | 62.00 | 10.22% | 31.34% |
| strict reward | 324 | 153 | 144 | 130.78 | 2.61% | 91.72% |
| coverage-qualified | 202 | 144 | 137 | 126.44 | 2.78% | 95.14% |

Proxy and admission do not materially raise top-cluster share, and strict
reward becomes more dispersed. Cluster survival is not disproportionately
lower than the corresponding candidate-budget retention. Without adding a
post-hoc numeric threshold, the stagewise review finds no significant
signal-level collapse. Generation remains the earliest observed structural
redundancy point; AST and signal N_eff remain non-comparable representations.

## Baseline Test

Before implementation: `27 passed in 23.28s`.

After guard, ledger, collapse-audit, and fail-closed provenance fixes:
`41 passed in 8.86s`.

Final Phase A suite after the full signal-sketch audit, graph synchronization,
and delivery validators: `49 passed in 14.74s`.

Ledger validator result: 5 registered roles, 8 access records, 4 burn records,
0 forward-access violations, and 2 validation/holdout spent records.

Real 324-row Phase3CM projection smoke:

- feedback rows: 324
- feedback columns: 45
- forbidden candidate-level evaluation fields: 0
- guard marker: `evalreset_feedback_guard_v1`
- scheduler budget allocation from projected tables: 24,576 / 24,576
- searcher feedback smoke: `PHASE3CN_SEARCHER_FEEDBACK_GUARD_PASS_DIAGNOSTIC_ONLY`
- integrated CA -> CM fixture -> CN feedback smoke:
  `PHASE3CN_INTEGRATED_FEEDBACK_SMOKE_PASS_DIAGNOSTIC_ONLY`

## Phase 1 Acceptance Gates

- Role registry, access ledger, and OOS burn ledger are versioned and tested.
- Feedback graph includes automated and human decision edges.
- Collapse audit reports coverage and never substitutes family/motif for signal.
- Candidate-level validation/holdout/challenge/sealed/forward/OOS fields hard
  fail at feedback and scheduler boundaries.
- Generated feedback memory physically contains development/train fields only.
- Full test suite passes before commit; no push occurs before user acceptance.
- No new field, event/state reward, large search, positive memory update, or
  2026 performance access occurs during Phase 1.
