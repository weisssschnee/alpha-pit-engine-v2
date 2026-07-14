# CN Field Universe → Search Exposure Audit (Phase 1)

Date: 2026-07-14  
Remote evidence baseline: `afd0666404fed935072f7f6d73e13db17b7f6f9f`  
Scope: current committed 121-field true1min release, Feature/State Fabric role inference, Sprint-1/2 generator wiring, and frozen Broad Event entry pack.

## Executive decision

Current CN capability must not be described as “121 fields have been searched.”

The defensible statement is:

> 121 columns are materialized in the active true1min release. Of the 59 lagged-context fields, only 12 are in the main generator context pool; slow-variable temporal routes are absent; declared market/context state fields are not actually consumed by the current state expressions. Broad Event is now a separate, valid multi-source system, but the full PIT financial-statement universe remains outside the Fabric.

Current status:

- `FIELD_ASSET_UNIVERSE_BROADER_THAN_121_CONFIRMED`
- `121_MATERIALIZATION_NOT_SEARCH_COMPLETENESS`
- `LAGGED_CONTEXT_DIRECT_EXPOSURE_12_OF_59`
- `SLOW_TEMPORAL_CONTEXT_NOT_EVALUATED`
- `DECLARED_CONTEXT_STATE_NOT_CONSUMED_BY_STATE_EXPRESSIONS`
- `FABRIC_ROLE_INFERENCE_LEXICAL_NOT_SEMANTIC`
- `FABRIC_ROLE_NOT_ENFORCED_AT_ROUTE_LEVEL`
- `BROAD_EVENT_RECOVERY_VALID_AND_SEPARATE`
- `FULL_PIT_FUNDAMENTALS_PENDING_CODEX_FABRIC_EXPANSION`

## Quantified exposure

- Lagged context fields: **59**
- Main generator context pool: **12**
- Direct exposure rate: **20.34%**
- Outside main context pool: **47**
- Temporal generator lagged-context fields: **0**
- Context/market state fields listed by generator: **4**
- Those fields actually referenced in state expressions: **0**

### By source family

| source_family          |   fields |   direct_exposed |   declared_state |   actual_state_consumed |   direct_exposure_rate |
|:-----------------------|---------:|-----------------:|-----------------:|------------------------:|-----------------------:|
| billboard              |       13 |                1 |                0 |                       0 |              0.0769231 |
| holder_structure       |        5 |                1 |                0 |                       0 |              0.2       |
| hotness                |        5 |                2 |                0 |                       0 |              0.4       |
| margin_financing       |        8 |                2 |                0 |                       0 |              0.25      |
| market_ecology         |        4 |                0 |                1 |                       0 |              0         |
| market_sentiment       |       15 |                0 |                2 |                       0 |              0         |
| valuation_market_state |        9 |                6 |                1 |                       0 |              0.666667  |

## Root causes

### 1. Role assignment is lexical

The current Fabric infers all `ctx_*` fields as `interaction-only` unless the name contains a small set of tokens (`is_st`, `prev_is_limit`, `sent_`, `zls_`). This is not an economic-semantic registry. It cannot distinguish valuation level, disclosure pulse, stock-level slow state, or market-level regime.

### 2. Materialization does not enforce route semantics

The Fabric blocks only `FieldRole.BLOCKED`; it applies observable-time and source-session guards, but does not prevent an `interaction-only` field from being directly ranked by a generator. Therefore role labels do not by themselves constrain formula construction.

### 3. Static generator contradicts `interaction-only`

The static generator draws `a` from `RAW_FIELDS + CONTEXT_FIELDS` and can emit `CSRank($a)` or `CSRank(Delta($a, window))`. PB, PS, market cap and other context fields can therefore be used directly despite the registry saying `interaction-only`.

### 4. Slow-variable temporal capability is absent

The temporal generator selects only minute raw fields, plus legacy intraday event state for duration/time-since operations. It does not generate PE/PB/PS changes, holder changes, financing trends, billboard persistence, or hotness dynamics.

### 5. Sprint-2 “State” evidence is narrower than its name

The state generator computes `state_source` from intraday return signs and candle location. Although it selects `state`, `other_state`, and `context_state`, those values are only written into metadata; current expressions do not reference them. The reported 465 state clusters therefore prove intraday price-state novelty, not market-sentiment/ZLS/fundamental-state capability.

### 6. Broad Event is now a real, separate capability

Broad Event recovery correctly evaluates multi-source episodes. It should remain separate from the old generic generator and enter unified discovery only through its frozen 11-mechanism pack.

## Immediate architecture implications

The next unified discovery system needs distinct typed routes:

1. `MINUTE_STATIC`
2. `SLOW_CROSS_SECTIONAL_LEVEL`
3. `SLOW_TEMPORAL_CHANGE`
4. `DISCLOSURE_EVENT`
5. `MARKET_REGIME_CONDITION`
6. `INTRADAY_STATE`
7. `BROAD_EVENT_FROZEN_ENTRY`

A field may generate multiple typed representations, but each representation requires its own observable-time, maturity, support unit and matched control.

## Broad Event entry-pack engineering priority

The attached Event CSV groups mechanisms by order of magnitude of the minimum cross-seed matched increment:

- Tier A: minimum increment ≥ 1e-3
- Tier B: 1e-4 ≤ minimum increment < 1e-3
- Tier C: minimum increment < 1e-4

This is only a quota/prioritization heuristic. It is **not** a significance threshold, Alpha proof, or promotion rule.

Tier counts:

| priority_tier                 |   mechanisms |
|:------------------------------|-------------:|
| A_ORDER_OF_MAGNITUDE_PRIORITY |            4 |
| B_MODERATE_EXPLORATORY        |            4 |
| C_TRACE_INCREMENT             |            3 |

## Division of work

### Codex

- enumerate full local PIT source universe;
- establish disclosure/revision/observable-time contracts;
- build versioned fundamental sidecars and lazy Fabric adapters;
- output per-field source-universe and 121-gap tables;
- perform no performance search.

### Web research lead

- maintain the cross-system semantic map;
- audit route exposure and role enforcement;
- stratify the frozen Event pack;
- merge Codex's source universe with this 121-field exposure matrix;
- author the Unified Discovery result contract after the Fundamental Fabric closes.

## Merge gate for Codex output

The Fundamental Fabric delivery is not complete unless its gap table can be joined to `CN_121_LAGGED_CONTEXT_EXPOSURE_MATRIX_20260714.csv` by a stable field/source identifier and reports:

- source table and raw field;
- report period;
- announcement/observable/revision time;
- typed semantic routes;
- current 121 presence;
- current generator exposure;
- support and missingness;
- unresolved PIT blockers.

## Limitations

This phase fully audits the committed 121-field release and generator wiring. The complete local upstream financial-statement field universe cannot be enumerated from GitHub alone because the old audit references local data panels and runtime registries that are not committed on this branch. Codex's newly deployed Fundamental Fabric task is therefore the authoritative source-universe enumerator.

## Artifacts

- `CN_121_LAGGED_CONTEXT_EXPOSURE_MATRIX_20260714.csv`
- `CN_BROAD_EVENT_PACK_ENGINEERING_TIERS_20260714.csv`
