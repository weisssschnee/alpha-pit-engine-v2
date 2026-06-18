# Phase3CD AST Primitive Assumption Audit

Status: diagnostic design artifact. No search was launched and no official X0/R3 state was modified.

## Decision

- Fresh search validation showed current true1min runs are fresh but still concentrated in base minute fields.
- Current AST evaluator does not expose GroupRank/GroupMean/GroupResidual primitives, so new fields are not currently entering formula group-key geometry.
- The immediate blocker is ordinary continuous primitives consuming sparse event, discrete state, and coverage-sensitive fields.
- Limit diagnostic motifs already contain examples that would be unsafe if promoted into main AST budget without typed rewrites.

## Key Counts

- primitive assumptions: 14
- primitive/category matrix rows: 112
- group-key scan hits: 59
- unsafe limit diagnostic motif rows: 16
- registry sample rows: 508

## Required Next Gate Before Large Search

1. Block sparse event, discrete state, membership, timestamp, text, and label fields from ordinary AST inputs by default.
2. Add typed event/state/coverage primitive registry and require it for those fields.
3. Keep plate/industry/concept membership as audited context until PIT membership/churn and group geometry are tested.
4. Run a small new-field fresh canary only after candidate generation proves that new fields actually enter the AST candidate pack.

## Outputs

- `primitive_assumption_matrix.csv`
- `field_category_route_matrix.csv`
- `primitive_x_field_category_decision_matrix.csv`
- `group_key_usage_audit.csv`
- `unsafe_limit_motif_rewrite_queue.csv`
- `typed_primitive_spec.csv`
- `typed_primitive_registry.json`
- `registry_field_route_sample.csv`
- `phase3cd_ast_primitive_assumption_audit_summary.json`
