# ADR 0014: Continuous-book MTM and operational evidence severity

- Status: Accepted
- Date: 2026-08-03
- Scope: CN train economic evaluation and post-financial runtime evidence

## Context

The CN engine is intended to discover alpha for a continuing A-share long-only
book.  It already values cash and open holdings at each PIT close, applies T+1,
suspension and price-limit rules, processes corporate actions, and charges the
accepted costs.  Requiring the book to become flat at an arbitrary dataset
cutoff therefore answered a liquidation-stress question rather than the alpha
question.  ADR 0013's additional 5% ending-holdings cap compounded that drift:
it could reject an otherwise positive continuous-book result solely because
the cutoff occurred while the strategy remained invested.

Incident review found a parallel authority error in large search.  Several
checkpoints completed financial evaluation with matching identities, complete
artifacts and zero sealed reads, but were discarded because measured effective
cores were just below an acceleration target or free memory briefly fell below
a conservative 24 GiB reserve.  These observations are important capacity
evidence, but after financial completion they are not evidence that the alpha
result is semantically wrong.  Recomputing those checkpoints wasted capacity
and exposed the system to more infrastructure variance.

## Decision

### 1. Continuous-book daily mark-to-market is train economic authority

The development replay authority is `FINAL_CLOSE_MARK_TO_MARKET`.  Open
holdings are valued at the observed final PIT close.  No terminal sale or
terminal sell fee is fabricated.  Ending holding count, value and NAV weight
remain mandatory diagnostics, but no absolute ending-weight cap is an alpha
admission gate.

For a signal observed at `SESSION_CLOSE_T`, a PIT-safe minute field that varies
within the session is materialized by its last observed value at or before that
session close.  Intraday variation alone is not incompatibility; future or
post-close observability, missing coverage, or identity drift remains fatal.

A train-only MTM pair may enter finalist consideration only when:

1. primary and control results are semantically complete;
2. the primary risk-adjusted MTM reward is greater than zero;
3. the primary-minus-control MTM reward increment is greater than zero;
4. the primary cumulative net return is greater than zero;
5. the primary-minus-control cumulative net-return increment is greater than
   zero; and
6. mechanism uniqueness and no-backfill rules hold.

No-fill is a valid zero-activity economic result, not a batch or candidate
execution failure.  It remains in diagnostics and cannot pass the positive
economic tests.

### 2. Forced liquidation is a separate stress diagnostic

`REQUIRE_FLAT_FINAL_OPEN`, terminal-liquidity events and a hypothetical forced
liquidation result may be reported to characterize exit risk.  They cannot
replace continuous-book MTM economics, block a report-only OOS evaluation, or
erase a semantically complete train result.

### 3. Post-financial runtime evidence does not decide alpha validity

After backend results are complete, runtime evidence is separated into:

- semantic integrity: expected outputs, checkpoint completion, cache/DAG
  evidence and exact-once pair identity;
- compute efficiency: parallel engagement, effective cores and occupancy; and
- resource headroom: peak RSS and observed free memory.

Only semantic-integrity failure invalidates the financial checkpoint.
Compute-efficiency or resource-headroom degradation is recorded and used to
tune future scheduling, batching and kernels; it does not trigger financial
recomputation or discard immutable results.

The node resource governor's pre-launch admission remains hard.  A task that
cannot acquire its declared lease or start with the required reserve must not
begin financial work.  Actual OOM, worker death, incomplete outputs, duplicate
evaluation, source/input/identity/hash drift, prohibited data access and
optimizer-feedback corruption remain fatal.

### 4. Search and validation authority boundaries remain unchanged

Phase3CM/ADR 0010 continues to own development search feedback.  MTM replay and
report-only OOS never write optimizer, scheduler or archive state.  Validation,
holdout and 2026 boundaries are unchanged.  This ADR removes false vetoes; it
does not lower evidence standards or authorize promotion.

## Consequences

- The engine evaluates the alpha actually requested: a continuing daily-valued
  long-only book, not an artificial end-of-file liquidation contest.
- Positive relative performance cannot rescue a loss-making primary because
  both risk-adjusted reward and cumulative return have absolute and matched
  positive tests.
- Large-search financial work survives harmless CPU scheduling and memory
  headroom variance once semantic closure succeeds.
- Runtime diagnostics remain visible for capacity planning without being
  mislabeled as economic or code failure.
- Historical cohorts may be reclassified from immutable MTM NAV evidence, but
  they are not promoted and OOS is not retroactively rewritten.

## Verification

1. daily NAV includes final PIT-close holdings and no terminal sale/fee;
2. finalist tests require positive primary and matched reward plus cumulative
   return, independent of terminal holdings weight;
3. no-fill closes as zero-activity economic evidence and fails positive gates;
4. low effective cores and low post-financial memory headroom yield diagnostic
   degradation while semantic runtime status remains `PASS`;
5. missing outputs, duplicate pair evaluation and missing checkpoint/cache
   evidence remain hard failures;
6. validation, holdout and 2026 reads remain zero during train replay; and
7. optimizer, scheduler, archive and promotion writes remain forbidden.

## Rollback

Rollback requires a replacement accepted authority decision.  Do not restore
an arbitrary terminal holdings cap or let post-financial utilization telemetry
invalidate semantically complete alpha results.
