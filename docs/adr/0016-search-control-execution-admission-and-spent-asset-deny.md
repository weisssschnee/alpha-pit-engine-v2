# ADR 0016: Search-control execution admission and spent-asset deny

- Status: Accepted
- Date: 2026-08-11
- Scope: CN high-cost execution admission, destructive evaluation-asset use and development-feedback provenance

## Context

The project has three control-plane drifts that must be repaired before any
later financial search. The fixed 2023 historical challenge is already spent
and economically negative, but an older unopened authorization, the evaluation
role registry, validators and launcher paths can still describe or accept it as
unopened. Project Control records `PREFLIGHT` and `POST_BATCH` decisions, but the
canonical CN route entry does not physically consume them. Phase C and the
allocator-repair canary also rebuild campaign-local optimizer observations from
earlier development results while recording only that serialized optimizer
state was not imported.

Rules exist to protect factual integrity and improve project progress. A
heuristic, governance verdict or admission gate cannot override observed
economic facts or decide which alpha wins. It may only decide whether an
identified run may start, continue or recover while preserving PIT, execution,
asset-use and evidence boundaries.

## Decision

1. The 2023 historical challenge asset is permanently `SPENT + NEGATIVE`.
   Existing authorization and outcome receipts remain immutable historical
   evidence. Current destructive-use authority is resolved from the role
   registry plus the access-started and outcome receipts. Any spent signal
   returns `PERMANENT_DENY_ALREADY_SPENT` before archive hashing, conversion,
   price-row access or output-root creation. An old authorization, old access
   receipt or fresh output root cannot reopen the asset.
2. The canonical `app.py` high-cost CN route seam consumes one immutable,
   self-hashed Project Control admission binding before importing or invoking a
   route implementation. The binding includes the source run-record path,
   canonical receipt hash, Project Control run id, project identity, requested
   action, target campaign/run identity and relevant repository SHA. Missing or
   drifting fields fail closed.
3. New freeze, launch and retry actions require a `PREFLIGHT == PROCEED`
   receipt with `automatic_execution_allowed == true`. An automatic successor
   additionally requires its parent's `POST_BATCH == CONTINUE` receipt with
   `automatic_continuation_allowed == true`; parent continuation and child
   preflight are an `AND`, never alternatives.
4. `TECHNICAL_RECOVERY_OF_ALREADY_AUTHORIZED_RUN` may reuse the original
   preflight only for the same immutable target run, same code SHA and a bound
   nonempty incident identity. A new retry, output identity or campaign must
   use a new preflight. Recovery is not a second execution entrance.
5. Project Control remains execution-admission authority only. It cannot alter
   candidate economics, reward, evaluator results, allocator policy, promotion
   evidence or sealed-asset state. No scheduler, executor or project database
   is added.
6. Development-feedback provenance distinguishes serialized optimizer-state
   import from replayed financial observations, candidate results, factor or
   behavior statistics, template classification, manual diagnosis and
   objective design after parent results. Replayed observations are
   cross-campaign development feedback even when serialized state import is
   false. Closed historical runtime artifacts and receipt hashes are not
   rewritten; new readers, schemas and non-authoritative projections state the
   facts honestly.
7. Forward-B remains `SEALED_UNSPENT + NOT_AUTHORIZED`. Phase D remains closed,
   valid adaptive development evidence with the revised allocator held, no OOS
   grade and no promotion.

## Consequences

- The project cannot reopen the spent 2023 asset through an old receipt, fresh
  directory or recovery label.
- High-cost canonical CN routes have a thin physical Project Control consumer
  without importing or rebuilding the Harness.
- A `false` optimizer-state-import flag can no longer erase observation replay
  from campaign provenance.
- Focused synthetic tests can verify admission and denial without reading
  validation, holdout, 2023, Forward-B or 2026 financial data.
- RAW Graph freshness remains independent. If RAW cannot be refreshed, it stays
  stale and advisory; CURRENT may be deterministically regenerated from the
  accepted overlay without manufacturing runtime assurance.

## Rollback

The physical admission bridge may be removed only by a later accepted ADR that
provides an equally fail-closed execution seam. The spent 2023 state and its
permanent no-reopen boundary are not rollbackable. Removing provenance fields
must not reclassify known observation replay as fresh or absent feedback.
