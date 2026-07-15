# ADR 0005: Canonical GraphSkill RAW and CURRENT

## Decision

The repository adopts the current `gsd-graphify-runtime-fidelity` contract:

- `.planning/graphs/graph.json`, `graph.html` and `GRAPH_REPORT.md` are generated RAW navigation evidence.
- `config/architecture_overlay.json` is the only human-maintained architecture lifecycle and boundary input.
- `.planning/graphs/current.json` and `current.html` are generated CURRENT projections.
- Runtime assurance requires an explicitly selected matching execution trace. Static tests and reports do not silently become runtime proof.

The former `.planning/codegraph` and `.planning/architecture` namespaces are retired. Their history remains available in Git and reports, but they are not current control planes.

## Consequences

Historical Sprint and Phase nodes remain discoverable in RAW and their canonical reports. CURRENT contains only components and boundaries that can change the present research or execution conclusion. Graph freshness is source-based and maintained by the GraphSkill status/audit implementation rather than a project-specific freshness generator.
