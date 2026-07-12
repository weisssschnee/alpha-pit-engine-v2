# Current Architecture

Updated: 2026-07-12

State: `CN_GENERATOR_RESEARCH_SPRINT1_ACTIVE` on a physically isolated
development-only release. Sealed evaluation, promotion and cross-sprint memory
remain frozen.

The current architecture contract is generated from the accepted Phase A
registry plus `runtime/run_plans/nextgen_dark_architecture_registry_v1.json`.
Every graph node carries implementation paths, input/output artifacts, data
role, feedback permission, test evidence, verification scope/SHA and blocker.

```mermaid
flowchart LR
    SRC["IMPLEMENTED<br/>approved 2024-2025 16-shard source"] --> DEV["IMPLEMENTED<br/>physical development-only release<br/>446,443,583 rows / 576 row groups"]
    SPLIT["IMPLEMENTED<br/>fixed split manifest"] --> DEV
    DEV --> LEDGER["IMPLEMENTED<br/>fail-closed loader + read ledger<br/>all forbidden counters = 0"]
    DEV --> FABRIC["IMPLEMENTED<br/>121-field Feature/State Fabric"]
    LEDGER --> CANARY["IMPLEMENTED<br/>formal B1S CANARY<br/>1,344 -> 1,187 -> 1,094 -> 279 -> 863 -> 64"]
    FABRIC --> STATIC["IMPLEMENTED<br/>static generator baseline"]
    FABRIC --> TEMP["IMPLEMENTED<br/>typed temporal/event/state primitives"]
    STATIC --> CANARY
    TEMP --> CANARY
    CANARY --> SPRINT["PARTIAL<br/>Generator Research Sprint-1<br/>max 3 development-only rounds"]

    SPRINT --> CAP["PLANNED<br/>Generator Capability CANARY"]
    CAP --> EPOCHA["CONDITIONAL<br/>Frozen Epoch-A / >=2 seeds"]
    EPOCHA --> EPOCHB["CONDITIONAL<br/>Epoch-B only by preregistered rule"]

    PLATE["FROZEN<br/>plate/industry user-deferred"] -. "FORBIDDEN" .-> SPRINT
    FWD["FROZEN<br/>validation / holdout / 2026"] -. "FORBIDDEN" .-> SPRINT
    MEMORY["FROZEN<br/>permanent scheduler/memory"] -. "FORBIDDEN" .-> SPRINT
    SPRINT -. "NO PROMOTION" .-> PACK["CONDITIONAL<br/>frozen candidate pack"]
```

## Active boundaries

- Loader validates release identity, physical file set, row-group role/date
  metadata, hashes and cache provenance before candidate generation.
- Sprint feedback is development-only and ephemeral. No candidate, reward or
  adaptive statistic may persist across the sprint boundary.
- Generator Capability CANARY and Epoch contracts must freeze all search and
  evaluation knobs before data reads.
- Novelty cannot compensate negative economic or benchmark increment.
- Plate/industry fields, validation, holdout and 2026 remain unavailable.
- The previous 64-candidate pack is diagnostic input only and is not promoted.
