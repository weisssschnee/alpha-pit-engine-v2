# Architecture Boundary

Updated: 2026-07-10

## Raw Graph

`graph.json`, `graph.html`, and `GRAPH_REPORT.md` are generated navigation
artifacts. They include historical scripts, reports, launchers, and retired
routes. The last valid raw build is dated 2026-07-05. A 2026-07-10 rebuild was
not fabricated because local preflight reported that `graphifyy` is not
installed.

## Curated Views

- `CURRENT_ARCHITECTURE.md`: authoritative active components and contracts.
- `EVOLUTION_MAP.md`: supersession and historical lineage.
- `ARCHITECTURE_BOUNDARY.md`: interpretation rules.

Raw graph presence does not make a component active. Current user instruction,
`AGENTS.md`, `MIGRATION_MANIFEST.md`, accepted run manifests, and the curated
architecture take precedence over historical graph nodes.
