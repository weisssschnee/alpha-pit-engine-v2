# Current Architecture

Updated: 2026-07-13

State: `CN_BROAD_EVENT_SYSTEM_RECOVERY_AUTHORIZED_NOT_STARTED` on the physically
isolated 2024-2025 development-only release. Sealed evaluation, promotion and
cross-sprint memory remain frozen.

This projection is generated and checked from
`runtime/run_plans/evalreset_phase1_architecture_registry_v1.json` plus
`runtime/run_plans/nextgen_dark_architecture_registry_v1.json`. The current
machine-readable authority is `.planning/graphs/graph.json`.

```mermaid
flowchart LR
    DATA["IMPLEMENTED<br/>16-shard true1min source"] --> DEV["IMPLEMENTED<br/>physical development-only release"]
    SPLIT["IMPLEMENTED<br/>fixed calendar split"] --> DEV
    DEV --> LEDGER["IMPLEMENTED<br/>fail-closed access ledger<br/>forbidden counters = 0"]
    DEV --> EPOCHC["PARTIAL<br/>three-seed Epoch-C<br/>32,739 / 32,768 unique<br/>1,024 strict"]

    EVIDENCE["IMPLEMENTED<br/>explicit frozen Sprint-1 evidence"] --> SELECTOR["IMPLEMENTED<br/>DEVELOPMENT_ELIGIBLE<br/>STRICT_PRIORITY_ELIGIBLE 10%"]
    SELECTOR --> EPOCHC
    TEMPORAL["IMPLEMENTED<br/>typed temporal generator<br/>250 new clusters/seed"] --> EPOCHC
    STATE["PARTIAL<br/>state generator<br/>465 new clusters/seed<br/>no matched-static increment"] --> EPOCHC
    LEGACY_EVENT["DEPRECATED<br/>single cutoff-latched trigger<br/>zero Epoch-C budget"] -. "NOT EXECUTED" .-> EPOCHC
    FIELDS["IMPLEMENTED<br/>121-field registry + sidecars"] --> BROAD_EVENT["PLANNED<br/>Broad Event recovery<br/>semantics and audit first"]
    LEGACY_EVENT -->|"superseded interpretation"| BROAD_EVENT
    DEV --> BROAD_EVENT
    RX["IMPLEMENTED<br/>RX/UCB only primary adaptive<br/>3/3 strict wins"] --> EPOCHC

    EPOCHC --> PACK["FROZEN<br/>377 exact research identities<br/>three-way Jaccard 0.8488"]
    PACK -. "NO PROMOTION" .-> SEALED["FROZEN<br/>validation / holdout / 2026"]
    PACK -. "NO POSITIVE MEMORY" .-> MEMORY["FROZEN<br/>scheduler / permanent memory"]
    BROAD_EVENT -. "NO CANARY BEFORE PREFLIGHT" .-> EVENT_CANARY["PLANNED<br/>development-only Event CANARY"]
    EVENT_CANARY -. "NO PROMOTION / NO 2026" .-> SEALED
    PLATE["FROZEN<br/>plate/industry user-deferred"] -. "FORBIDDEN" .-> EPOCHC
```

## Current outcomes

- The strict-priority selector has both offline OOF lift and realized
  development strict lift. It is a frozen model, not online memory.
- Shared-backbone proxy and strict-priority rank correlations are 1.0 for all
  seed pairs; behaviour-cluster ARI is at least 0.9999857.
- RX/UCB beats its matched control at strict evaluation in all three seeds and
  remains the only primary adaptive challenger.
- Temporal and state lanes add behaviour coverage outside static. Broad Event
  capability remains unevaluated because Epoch-C assigned the lane zero
  proposal, admission and strict budget.
- The strict union has positive benchmark increment and is not dominated by a
  lane, primitive, family, parent or signal cluster.
- State does not beat matched static economically; novelty alone is not called
  alpha increment.

## Active blockers and boundaries

- Epoch-C is partial because 29 RX identities repeated across seed expansions.
  The future guard is implemented and tested, but the historical run remains
  32,739 / 32,768 globally unique.
- The 377-identity union is research-only. It is not a forward-authorization
  pack and cannot enter promotion or positive memory.
- Epoch-D remains unrun. State lacked matched-static increment; Event cannot be
  judged because the historical single-trigger audit had a semantic mismatch
  and the Epoch-C Event lane was not executed.
- Broad Event implementation and its fixed-budget development-only CANARY are
  authorized only after semantic, PIT, support, matched-control and access
  preflight gates pass.
- Validation, holdout, spent, sealed, plate/industry and 2026 remain outside
  the allowed development feedback graph.
