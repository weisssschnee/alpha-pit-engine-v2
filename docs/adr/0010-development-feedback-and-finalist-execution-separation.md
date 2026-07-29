# ADR 0010: Separate development feedback from finalist execution evidence

Status: Accepted by user direction on 2026-07-29.

## Context

ADR 0009 correctly narrowed Phase3CM to development-predictive evidence and
added a fail-closed A-share execution replay. It then overextended the replay
boundary by requiring both executable member receipts before every train-only
optimizer observation could complete.

That coupling made a finalist economic gate a prerequisite for ordinary
development ask/tell. In the currently bound Phase3CM materializations,
execution metadata is not carried in every evaluator table, so the coupling
would disable otherwise valid development search rather than merely prevent an
economic claim.

This does not establish that the project PIT fabric is missing or invalid.
PIT observable-time, release, maturity, limit lifecycle, suspension and
universe components already exist in separate authorities. Finalist replay
must bind the required inputs from those authorities, but their absence from a
particular Phase3CM table is not a project-wide PIT failure.

## Decision

The existing authority chain has two explicit evidence lanes.

### Development search lane

Full-coordinate, train-only Phase3CM pair evaluation may supply optimizer
feedback under the existing development search authority:

```text
search_score =
min(primary Phase3CM composite reward,
    primary-minus-control Phase3CM matched increment)
```

The pair must be evaluated, pair-native feedback must be READY, the primary
standalone train decision must be `TRAIN_REWARD_FOLLOWUP_READY`, and the
existing identity, support, receipt, split, semantic and sealed-period guards
must pass. Executable replay receipts are not required for development trial
completion.

Every such observation remains explicitly:

```text
DEVELOPMENT_PREDICTIVE_SCORE_ONLY
DEVELOPMENT_SEARCH_ONLY
economic_claim_authorized = false
candidate_promotion_authorized = false
```

Validation, holdout and 2026 data remain forbidden from feedback, tells,
scheduler state, archives and persistent search memory.

### Finalist execution lane

The existing A-share replay remains the mandatory gate for any claim of
execution readiness, finalist economic eligibility or later promotion. Both
matched members must carry canonical `A_SHARE_TRADABILITY_REPLAY_V1` receipts
that revalidate candidate identity, exact identity, execution clock, same-bar
exclusion, T+1, limit-lock and suspension behavior, complete fees and the
promotion-grade universe binding, plus corporate-action cash/share and
terminal-liquidation proofs.

Executable replay metrics are recorded alongside predictive metrics but never
replace or mutate the development optimizer reward. Missing replay evidence
sets finalist execution eligibility false; it does not invalidate a clean
development observation.

The replay remains unable by itself to authorize promotion or an economic
claim. Corporate-action and terminal-liquidation semantics are now enforced
inside the same existing replay receipt, while their PIT inputs, report-only
validation and all other finalist requirements remain fail-closed gates.

## Consequences

- No new evaluator, platform, database, search policy or authority node is
  created.
- The executable replay kernel and immutable receipt verification introduced
  under ADR 0009 are retained.
- The optimizer hard-gate clauses in ADR 0009 are superseded by this decision.
- Historical closed campaign artifacts remain immutable development evidence;
  they are neither recomputed nor upgraded to economic evidence.
- No new search, financial replay, validation, holdout or 2026 access is
  authorized by this correction.
- The next project phase remains behavior-family deduplication and train
  stability selection over the closed candidate pool, followed by a separately
  frozen small finalist set and then the existing executable/report-only
  evidence path.
- Graphify RAW semantic refresh and any external LLM backend are documentation
  maintenance concerns only; neither is a runtime dependency of this boundary.
