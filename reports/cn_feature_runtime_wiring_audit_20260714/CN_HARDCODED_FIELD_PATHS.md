# CN Hardcoded Field Paths

| Runtime path | Authority | Finding |
|---|---|---|
| `app.py` | `CURRENT_SEARCH_ROUTE` | Points to `phase3dv-budget-pool-self-deepen-pack`. |
| `phase3dv_budget_pool_self_deepen_pack.py` | `EVENT_FIELDS`, `VALUE_FIELDS`, `SENTIMENT_FIELDS`, `LIQUIDITY_FIELDS`, `UNDERUSED_CONTEXT_FIELDS`, `INTRADAY_FIELDS` | Hardcoded candidate pools; no `UnifiedCapabilityRegistry`. |
| `phase3cp_real_cm_small_loop.py` | `_available_fields` | Reads parquet schemas and filters atom availability; registry identity/PIT/route authority is not consulted. |
| `unified_discovery_generators.py` | `RegistryDrivenGenerator` | Correct unified authority, currently isolated to unified preflight/discovery. |
| `typed_route_compiler.py` | `TypedRouteCompiler` | Correct typed gate, currently isolated to unified candidates. |
| `nextgen_dark_development_canary.py` and `cn_b1s_development_canary.py` | fixed lane field lists | Historical canary infrastructure, not the current unified authority. |

The smallest safe integration is to make candidate submission into the legacy evaluator require a frozen unified-registry/typed-compiler receipt; physical schema presence must remain an availability check, never a search-eligibility authority.
