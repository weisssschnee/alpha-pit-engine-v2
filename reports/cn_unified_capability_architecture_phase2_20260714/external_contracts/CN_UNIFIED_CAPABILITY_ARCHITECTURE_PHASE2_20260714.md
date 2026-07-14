# CN Unified Capability Architecture — Phase 2

Date: 2026-07-14  
Evidence baseline: `afd0666404fed935072f7f6d73e13db17b7f6f9f`  
Status: `DESIGN_READY_PENDING_CODEX_FUNDAMENTAL_SOURCE_UNIVERSE_JOIN`

## Delivered contracts

1. `CN_UNIFIED_CAPABILITY_REGISTRY_V0_2_20260714.json`
2. `CN_TYPED_ROUTE_COMPILER_CONTRACT_V1_20260714.json`
3. `CN_SEARCH_EXPOSURE_LEDGER_SCHEMA_V1_20260714.json`
4. `CN_UNIFIED_DISCOVERY_CAPABILITY_PREFLIGHT_V1_20260714.json`
5. `CN_BROAD_EVENT_ENTRY_POLICY_V2_20260714.json`

## Core correction

The system must no longer infer capability from field presence, generator metadata or behavior novelty.

The proof chain is:

`source identity → semantic/PIT qualification → typed route wiring → non-zero exposure → matched-control development evidence → frozen reproduction → independent challenge → forward`

## Route model

The registry defines eight distinct routes:

- `MINUTE_STATIC`
- `FIRSTN_PATH`
- `SLOW_CROSS_SECTIONAL_LEVEL`
- `SLOW_TEMPORAL_CHANGE`
- `DISCLOSURE_EVENT`
- `MARKET_REGIME_CONDITION`
- `INTRADAY_STATE_TRANSITION`
- `BROAD_EVENT_FROZEN_ENTRY`

Each route has its own entity scope, maturity, support unit, legal operators, forbidden operations, identity and matched-control contract.

## Integration point for Codex

Codex should not overwrite these contracts. It should supply stable `source_field_id` values and full PIT fundamental rows that can join the provisional `active121::<field_name>` entries.

Required join output:

- `source_field_id`
- `source_table`
- `source_field`
- `fabric_field_id`
- `entity_scope`
- `temporal_semantics`
- `observable_time_field`
- `revision_time_field`
- `pit_status`
- `recommended_routes`
- `blocked_reason`

After the join:

1. unresolved aliases fail closed;
2. hard-coded generator field arrays are replaced by registry queries;
3. dry generation emits the Exposure Ledger;
4. capability preflight runs without performance metrics;
5. only then may a full development capability CANARY be frozen.

## Event policy

The previous A/B/C event tiers remain engineering heuristics only. The updated policy prevents raw increments from becoming final quota weights.

- Tier A: primary full-shard calibration root, maximum two descendants after calibration.
- Tier B: exploratory full-shard calibration root, maximum one descendant.
- Tier C: archive and fixed-control use only.

No event mechanism receives adaptive budget before 16-shard replay, episode-clustered uncertainty, standardized effect, turnover/cost and concentration checks.

## Non-negotiable claim ceilings

- zero budget → `NOT_EXECUTED`
- wired but no strict exposure → `NOT_EVALUATED`
- development increment → not validation
- two seeds on the same data → frozen reproducibility, not OOS
- challenge → independent frozen release only
- forward → no feedback to search
