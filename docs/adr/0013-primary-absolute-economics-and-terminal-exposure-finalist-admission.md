# ADR 0013: Primary absolute economics and terminal-exposure finalist admission

- Status: Accepted
- Date: 2026-08-03
- Scope: CN train-only finalist admission after immutable A-share replay

## Context

ADR 0010 correctly separates Phase3CM development feedback from executable
finalist evidence. The accepted development score is conservative predictive
evidence: `min(primary predictive reward, matched train increment)`. It must not
be replaced by post-search replay or validation outcomes.

The downstream finalist freeze had a different defect. It required a completed
strict replay and positive primary-minus-control executable increment, but did
not require the primary candidate's own executable net reward to be positive.
A loss-making primary could therefore beat an even worse control and enter the
finalist set. Final-close mark-to-market evidence exposed the same ambiguity:
relative winners could still have negative standalone economics or carry a
materially unliquidated ending book.

The closed 32-pair diagnostic provides a useful boundary check. Five pairs had
positive matched MTM increment, but none had positive standalone primary MTM
reward. Two relative winners ended with about 0.1% holdings weight, while three
ended near 40%. Relative improvement is therefore informative, but is not an
economic admission rule by itself.

## Decision

### 1. Development search feedback is unchanged

Phase3CM and ADR 0010 remain the only development reward authority. Strict
replay, MTM replay and OOS results cannot write optimizer feedback, scheduler
state or search archives. This ADR changes only downstream finalist admission.

### 2. Strict executable finalist admission requires two positive tests

A pair may enter the strict train-only finalist freeze only when:

1. both members closed as `PAIR_REPLAY_COMPLETE`;
2. the primary standalone A-share executable net reward is greater than zero;
3. the primary-minus-control executable net increment is greater than zero;
4. the selected set has unique economic-mechanism identity; and
5. blocked or nonpositive rows are never used as backfill.

This prevents a worse control from manufacturing a finalist out of an
absolutely loss-making primary.

### 3. MTM admission is bounded and remains non-executable evidence

Final-close MTM is an alternate train-only admission path for a separately
authorized report-only OOS run. A pair must satisfy both positive tests above
using MTM rewards, and both primary and control ending-holdings weights must be
at most `0.05` of final NAV.

The cap applies to both members so control construction cannot manufacture a
relative increment through a materially different residual book. Passing it
does not establish strict flat-book executability. The ending book is valued at
the final PIT close with no fabricated terminal sale or terminal sell fee.

### 4. Admission is not promotion

Both paths remain development train-only evidence. They authorize neither an
automatic OOS run nor promotion, live trading, an economic claim, holdout/2026
access or another search. Any report-only OOS must be separately frozen and
must not feed back across lanes.

## Consequences

- Search throughput and accepted reward semantics remain stable.
- Finalist counts can legitimately fall to zero; the system reports the actual
  smaller count and does not backfill.
- Relative control outperformance remains useful evidence but cannot override
  standalone primary economics.
- Low-residual-book MTM candidates can be studied without being mislabeled as
  strictly executable.
- Historical diagnostic rows are not retroactively promoted. Under this rule,
  the closed 32-pair MTM cohort admits zero finalists.

## Verification

1. strict replay tests reject a positive-increment, negative-primary pair;
2. MTM tests require positive primary and matched rewards;
3. MTM tests enforce the 5% cap independently for primary and control;
4. immutable input identity/order, self-hashes and artifact hashes are checked;
5. mechanism uniqueness and no-backfill behavior are deterministic;
6. validation, holdout and 2026 reads remain zero during admission;
7. optimizer, feedback, scheduler, archive and promotion writes are forbidden.

## Rollback

Rollback requires a replacement accepted finalist-admission decision. Do not
restore relative-only admission, feed replay economics into development TPE, or
reinterpret MTM admission as strict execution readiness.
