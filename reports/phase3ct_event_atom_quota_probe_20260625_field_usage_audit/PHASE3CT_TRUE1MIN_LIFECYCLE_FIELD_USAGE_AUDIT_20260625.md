# Phase3CT True1min Lifecycle Field Usage Audit

- created_at: 2026-06-24T16:25:53.801739+00:00
- decision: `HOLD_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT`
- shard_root: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\runtime\phase3cs_true1min_sidecar_augmented_shards_zls4_20260624`
- panel_count: 2
- schema_intersection_field_count: 86
- candidate_file_count: 1
- blocker_count: 1

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
  "effective_used_field_count": 2,
  "effective_used_lane_counts": {
    "event_state": 2
  },
  "files": [
    {
      "blocked_or_future_rows": 0,
      "candidate_file": "D:\\ChengboRemote\\workspace\\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\\reports\\phase3ct_event_atom_quota_probe_20260625\\phase3bp_top_decisions.csv",
      "effective_signal_rows": 2,
      "missing_schema_rows": 0,
      "row_count": 32,
      "zero_signal_rows": 30
    }
  ],
  "top_effective_fields": [
    [
      "evt_uplimit_auction_money",
      1
    ],
    [
      "evt_uplimit_auction_buy",
      1
    ]
  ],
  "top_used_fields": [
    [
      "amount",
      23
    ],
    [
      "evt_uplimit_up_limit_keep_times",
      14
    ],
    [
      "evt_uplimit_type_code",
      11
    ],
    [
      "evt_uplimit_auction_money",
      2
    ],
    [
      "evt_uplimit_auction_buy",
      2
    ],
    [
      "ctx_zls_strong",
      2
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
      "ctx_ths_hot_circulation_value",
      2
    ],
    [
      "ctx_sent_mian_num",
      2
    ],
    [
      "ctx_sent_downlimit_num",
      2
    ],
    [
      "ctx_sent_up_num",
      2
    ],
    [
      "ctx_zls_lbgd",
      2
    ],
    [
      "evt_uplimit_auction_offer",
      1
    ],
    [
      "evt_uplimit_fd_close",
      1
    ],
    [
      "evt_uplimit_auction_pre1max_ratio",
      1
    ],
    [
      "ctx_sent_lb_2_num",
      1
    ],
    [
      "ctx_sent_ditian_num",
      1
    ],
    [
      "ctx_sent_lt5_num",
      1
    ],
    [
      "ctx_ths_hot_rank_diff",
      1
    ],
    [
      "ctx_ths_hot_last_price",
      1
    ],
    [
      "ctx_ths_hot_rank",
      1
    ],
    [
      "ctx_sent_down_num",
      1
    ]
  ],
  "used_field_count": 23,
  "used_lane_counts": {
    "direct_formula": 23,
    "event_state": 32,
    "lagged_context": 23
  }
}
```

## Blockers

- lagged_context_candidates_present_but_zero_effective_signal

## Boundary

- This audit is evidence plumbing, not alpha proof.
- `train/validation/test` here is a chronological field-coverage audit split, not a promotion split.
- X0/R3 are read-only.
- Candidate files are checked for schema/lane usage and empty effective signal symptoms.
