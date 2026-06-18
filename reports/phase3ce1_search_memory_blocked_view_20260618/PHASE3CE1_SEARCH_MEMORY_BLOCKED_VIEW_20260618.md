# Phase3CE1 Search Memory Blocked View

Status: diagnostic/runtime view. No historical memory deletion. No search launched. No official X0/R3 mutation.

## Counts

- source_memory_file_count: `5`
- memory_entry_count: `3677`
- positive_record_count: `3067`
- blocked_record_count: `610`
- blocked_expression_key_count: `313`
- blocked_skeleton_key_count: `16`

## Typed Gate Decisions

- `allow`: `3067`
- `blocked_unsafe_known_structure`: `344`
- `require_typed_rewrite`: `266`

## Policy

- Unsafe memory keys are not deleted.
- Unsafe expression/skeleton keys remain in `active_duplicate_block_keys`.
- Unsafe records are excluded from positive attribution.
- Construction-time validator remains the primary safety gate.

## Outputs

- `search_memory_blocked_view.json`
- `search_memory_blocked_view.csv`
- `search_memory_blocked_keys.csv`
- `search_memory_positive_records.csv`
