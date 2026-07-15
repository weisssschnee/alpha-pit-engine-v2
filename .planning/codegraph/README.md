# Raw Codegraph

This directory contains the non-authoritative code-navigation graph generated
from repository snapshot `ea31ce4dc9053547330ac1a06c4403765ede222d` on the
77o machine with Graphify 0.9.6 in AST code-only mode. No LLM, research data,
validation, holdout or forward data were accessed. Generated raw graph artifacts
under `.planning/codegraph` were excluded from extraction to prevent recursive
self-indexing. The snapshot contains 3,563 nodes and 8,633 edges.

The raw graph contains historical and superseded code. It is useful for
navigation and dependency discovery, but it is not the current architecture
contract and cannot assert research status. The sole status authority is
`../architecture/architecture_registry.json`; maintained projections are in
`../architecture/`.

Build provenance and hashes are recorded in
`CODEGRAPH_BUILD_MANIFEST_ea31ce4dc9053547330ac1a06c4403765ede222d.json`.
Older SHA-bound snapshots remain immutable historical navigation artifacts.
