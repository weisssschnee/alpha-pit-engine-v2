# Disclosure Pre-Event Extension Retry V2

Status: `QUALIFICATION_COMPLETE_NOT_READY`

Promotion: forbidden

## Decision

```text
EXTENSION_CSRANK_STATIC_GATE = PASS
EXTENSION_CSRANK_BEHAVIOR_GATE = PASS
EXTENSION_ABS_STATIC_GATE = NOT_RUN_PRIOR_CANDIDATE_ACCEPTED
EXTENSION_ABS_BEHAVIOR_GATE = NOT_RUN_PRIOR_CANDIDATE_ACCEPTED
ACCEPTED_EXTENSION = DISCLOSURE_PRE_EVENT_PAYLOAD_CSRANK_V1
SAMPLED_FULL_CONTRACT_PARITY = NOT_AVAILABLE_NO_EXISTING_AUTHORITY
FORMULA_SPACE_INCREMENT = NOT_QUALIFIED
CEM_SEARCH_INCREMENT = NOT_QUALIFIED
PERFORMANCE_CONTRACT = FAIL
TARGET_FAMILY_LARGE_SEARCH_READINESS = SEMANTICS_BLOCKED
```

The accepted retry structure was
`EventWindow(CSRank(payload),event,5,0)`. It reused the current registry,
typed AST, Grammar/compiler, matched control, behavior archive and
full-coordinate development Phase3CM evaluator. It did not create a new
generator, optimizer platform, evaluator surrogate, search memory, Graph
authority or database.

## Extension gate

The frozen OLD authority was hash-verified and reused without recomputation:
272 post-archive exact-unique pairs and 126 behavior-unique pairs from the
same 256-candidate probe.

CSRank passed the static gate and retained 124 behavior-unique pairs from 256,
or 98.41% of OLD against the required 90% / 114-pair floor. The queue therefore
stopped before Abs exactly as frozen. Historical
`EventWindow(Sign(payload),event,5,0)` remains
`REJECTED_FORMULA_EXTENSION`; its evidence was preserved.

## Financial comparison

| Arm | Evaluated | Positive | Positive/hour | Median matched increment | Behavior families | AST shapes |
|---|---:|---:|---:|---:|---:|---:|
| A: Uniform OLD | 57 | 28 | 1,414.51 | -0.01036 | 3 | 1 |
| B: Uniform expanded | 72 | 29 | 1,510.33 | -0.10139 | 3 | 2 |
| C: CEM expanded | 72 | 26 | 1,207.51 | -0.11279 | 2 | 2 |

All arms met the frozen minimum financial support. Arm B improved positive
pairs/hour and AST diversity over A, but failed the median-increment and
behavior-discovery checks. Arm C updated nine decision contexts without
category collapse and retained checkpoint 2/3 supply, but failed the required
15% positive-throughput increment, median-increment and behavior-discovery
checks. Neither formula-space nor CEM policy increment qualified.

## Semantics and performance

The current authority scan inspected 60 run-plan/config JSON documents and
found no qualified current `development_sampled_evaluator`. No surrogate was
created. Fresh large-state construction nevertheless passed deterministic
parity with `state_origin=fresh_uniform_from_frozen_catalog`, generation zero,
zero reward observations and `source_campaign=none`. Large-search readiness
remains `SEMANTICS_BLOCKED`.

The route selected the existing two-thread stock-session backend. Median
effective cores were 2.00/2.06/2.00 and whole-host occupancy was only
6.24%/6.45%/6.24%, so the 75% host gate was not met. Cache peak stayed below
2.75 MiB, pair batch stayed at four and peak RSS stayed below 428 MiB.

Performance also failed the 24 GiB memory floor because two duplicated stale
recursive monitors from the old `5a3f4d8` campaign consumed about 77.82 GB
working set. They were confirmed childless, terminated without touching the
active campaign or old evidence, and free memory recovered from 2.48 GiB to
79.83 GiB. Closed checkpoint run-health evidence remains immutable; the
incident is infrastructure-only and does not mutate financial route health.

## Verification and disposition

- one launch event, zero scheduled triggers, one writer closed, process exit 0;
- 34 root artifacts hash-verified;
- 9/9 immutable batch manifests and 135 batch artifacts hash-verified;
- 9/9 Phase3CM access receipts completed and checkpoint chains matched;
- validation, holdout and 2026 reads: zero;
- promotion: forbidden.

One post-closure V2 monitor repeated the same recursive object-retention
mistake; it and two later old-monitor instances were removed after exact
command-line checks. Free memory returned to 82.08 GiB, with zero scheduled
monitor tasks and zero live monitor processes remaining. This happened after
the campaign closed and did not alter its artifacts or metrics.

The compact repository receipt is
`runtime/run_plans/cn_disclosure_pre_event_extension_retry_v2_20260724_receipt.json`.
Graph and Obsidian remain unchanged because this experiment did not establish
a durable active search authority or large-search-ready capability.

## Historical V1 disposition

The earlier Sign V1 probe remains immutable evidence: OLD retained 126
behavior-unique pairs while Sign-expanded retained 111/256, or 88.0952% of
OLD, below the frozen 90% gate. V1 therefore stopped before financial
evaluation. Its EventWindow streaming repair and test evidence remain valid,
but Sign cannot enter the decision catalog, CEM or production.
