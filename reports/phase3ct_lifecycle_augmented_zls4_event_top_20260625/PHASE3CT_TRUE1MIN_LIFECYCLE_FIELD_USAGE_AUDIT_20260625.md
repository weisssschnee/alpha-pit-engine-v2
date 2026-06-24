# Phase3CT True1min Lifecycle Field Usage Audit

- created_at: 2026-06-24T16:21:04.006614+00:00
- decision: `HOLD_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT`
- shard_root: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\runtime\phase3cs_true1min_sidecar_augmented_shards_zls4_20260624`
- panel_count: 4
- schema_intersection_field_count: 86
- candidate_file_count: 1
- blocker_count: 2

## Lane Coverage

```json
{
  "blocked_key_or_label": 11,
  "direct_formula": 39,
  "event_state": 12,
  "lagged_context": 24
}
```

## Split Coverage

```json
{
  "direct_formula": {
    "test_nonnull_fields": 39,
    "test_nonzero_fields": 39,
    "train_nonnull_fields": 39,
    "train_nonzero_fields": 39,
    "validation_nonnull_fields": 39,
    "validation_nonzero_fields": 39
  },
  "event_state": {
    "test_nonnull_fields": 12,
    "test_nonzero_fields": 11,
    "train_nonnull_fields": 8,
    "train_nonzero_fields": 6,
    "validation_nonnull_fields": 12,
    "validation_nonzero_fields": 11
  },
  "lagged_context": {
    "test_nonnull_fields": 24,
    "test_nonzero_fields": 24,
    "train_nonnull_fields": 20,
    "train_nonzero_fields": 20,
    "validation_nonnull_fields": 20,
    "validation_nonzero_fields": 20
  }
}
```

## Candidate Usage

```json
{
  "effective_used_field_count": 0,
  "effective_used_lane_counts": {},
  "files": [
    {
      "blocked_or_future_rows": 0,
      "candidate_file": "D:\\ChengboRemote\\workspace\\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\\reports\\phase3cs_augmented_zls4_event_state_bp_20260624\\phase3bp_top_decisions.csv",
      "effective_signal_rows": 0,
      "missing_schema_rows": 0,
      "row_count": 48,
      "zero_signal_rows": 48
    }
  ],
  "top_effective_fields": [],
  "top_used_fields": [
    [
      "amount",
      48
    ],
    [
      "evt_uplimit_up_limit_keep_times",
      26
    ],
    [
      "evt_uplimit_type_code",
      22
    ],
    [
      "ctx_zls_strong",
      4
    ],
    [
      "ctx_sent_downlimit_num",
      4
    ],
    [
      "ctx_sent_ditian_num",
      4
    ],
    [
      "ctx_ths_hot_circulation_value",
      3
    ],
    [
      "ctx_sent_lb_2_num",
      3
    ],
    [
      "ctx_ths_hot_rank_diff",
      3
    ],
    [
      "ctx_sent_up_num",
      3
    ],
    [
      "ctx_zls_lbgd",
      3
    ],
    [
      "ctx_ths_hot_last_pct",
      2
    ],
    [
      "ctx_sent_lb_3_num",
      2
    ],
    [
      "ctx_sent_mian_num",
      2
    ],
    [
      "ctx_sent_lt5_num",
      2
    ],
    [
      "ctx_ths_hot_last_price",
      2
    ],
    [
      "ctx_ths_hot_rank",
      2
    ],
    [
      "ctx_sent_zb_num",
      2
    ],
    [
      "ctx_zls_df_num",
      1
    ],
    [
      "ctx_sent_down_num",
      1
    ],
    [
      "ctx_sent_damian_num",
      1
    ],
    [
      "ctx_sent_tiandi_num",
      1
    ],
    [
      "ctx_sent_gt5_num",
      1
    ],
    [
      "ctx_sent_max_lb_num",
      1
    ],
    [
      "ctx_zls_ztjs",
      1
    ]
  ],
  "used_field_count": 25,
  "used_lane_counts": {
    "direct_formula": 48,
    "event_state": 48,
    "lagged_context": 48
  }
}
```

## Blockers

- event_state_candidates_present_but_zero_effective_signal
- lagged_context_candidates_present_but_zero_effective_signal

## Boundary

- This audit is evidence plumbing, not alpha proof.
- `train/validation/test` here is a chronological field-coverage audit split, not a promotion split.
- X0/R3 are read-only.
- Candidate files are checked for schema/lane usage and empty effective signal symptoms.
