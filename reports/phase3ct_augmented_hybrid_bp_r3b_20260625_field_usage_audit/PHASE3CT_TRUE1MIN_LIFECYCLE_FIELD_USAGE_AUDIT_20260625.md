# Phase3CT True1min Lifecycle Field Usage Audit

- created_at: 2026-06-25T15:42:16.252655+00:00
- decision: `PASS_TRUE1MIN_LIFECYCLE_FIELD_USAGE_AUDIT`
- shard_root: `D:\ChengboRemote\workspace\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\runtime\phase3cs_true1min_sidecar_augmented_shards_zls4_20260624`
- panel_count: 4
- schema_intersection_field_count: 86
- candidate_file_count: 2
- blocker_count: 0

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
  "effective_used_field_count": 46,
  "effective_used_lane_counts": {
    "direct_formula": 2741,
    "event_state": 68,
    "lagged_context": 296
  },
  "files": [
    {
      "blocked_or_future_rows": 0,
      "candidate_file": "D:\\ChengboRemote\\workspace\\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\\runtime\\phase3ct_augmented_hybrid_bp_r3b_20260625\\phase3bp_candidate_horizon_aggregate.csv",
      "effective_signal_rows": 640,
      "future_wrong_lag_followup_rows": 0,
      "future_wrong_lag_rows": 485,
      "missing_schema_rows": 0,
      "row_count": 768,
      "zero_signal_rows": 128
    },
    {
      "blocked_or_future_rows": 0,
      "candidate_file": "D:\\ChengboRemote\\workspace\\alpha_pit_true1min_engine_20260624_182909_bea5eacbe591\\reports\\phase3ct_augmented_hybrid_bp_r3b_20260625\\phase3bp_top_decisions.csv",
      "effective_signal_rows": 64,
      "future_wrong_lag_followup_rows": 0,
      "future_wrong_lag_rows": 40,
      "missing_schema_rows": 0,
      "row_count": 96,
      "zero_signal_rows": 32
    }
  ],
  "future_wrong_lag_followup_rows": 0,
  "future_wrong_lag_rows": 525,
  "top_effective_fields": [
    [
      "high",
      438
    ],
    [
      "low",
      438
    ],
    [
      "amount",
      412
    ],
    [
      "open",
      370
    ],
    [
      "close",
      210
    ],
    [
      "volume",
      127
    ],
    [
      "m1_first15_high",
      100
    ],
    [
      "m1_first15_low",
      100
    ],
    [
      "m1_first30_high",
      86
    ],
    [
      "m1_first30_low",
      86
    ],
    [
      "m1_first5_high",
      63
    ],
    [
      "m1_first5_low",
      63
    ],
    [
      "m1_first5_vol",
      43
    ],
    [
      "m1_first15_vol",
      42
    ],
    [
      "m1_first30_vol",
      42
    ],
    [
      "m1_first5_amount",
      39
    ],
    [
      "m1_first15_amount",
      39
    ],
    [
      "m1_first30_amount",
      38
    ],
    [
      "ctx_sent_downlimit_num",
      32
    ],
    [
      "ctx_sent_zb_num",
      25
    ],
    [
      "ctx_sent_lb_2_num",
      25
    ],
    [
      "ctx_sent_down_num",
      25
    ],
    [
      "ctx_sent_damian_num",
      23
    ],
    [
      "ctx_sent_lt5_num",
      22
    ],
    [
      "ctx_zls_lbgd",
      21
    ],
    [
      "ctx_sent_mian_num",
      19
    ],
    [
      "ctx_sent_up_num",
      18
    ],
    [
      "ctx_zls_ztjs",
      18
    ],
    [
      "ctx_sent_lb_h_num",
      17
    ],
    [
      "evt_uplimit_up_limit_keep_times",
      15
    ]
  ],
  "top_used_fields": [
    [
      "amount",
      547
    ],
    [
      "high",
      458
    ],
    [
      "low",
      458
    ],
    [
      "open",
      410
    ],
    [
      "close",
      230
    ],
    [
      "volume",
      127
    ],
    [
      "m1_first15_high",
      120
    ],
    [
      "m1_first15_low",
      120
    ],
    [
      "m1_first30_high",
      86
    ],
    [
      "m1_first30_low",
      86
    ],
    [
      "m1_first5_high",
      83
    ],
    [
      "m1_first5_low",
      83
    ],
    [
      "evt_uplimit_up_limit_keep_times",
      55
    ],
    [
      "m1_first5_vol",
      43
    ],
    [
      "ctx_sent_downlimit_num",
      42
    ],
    [
      "m1_first15_vol",
      42
    ],
    [
      "m1_first30_vol",
      42
    ],
    [
      "m1_first5_amount",
      39
    ],
    [
      "m1_first15_amount",
      39
    ],
    [
      "m1_first30_amount",
      38
    ],
    [
      "evt_uplimit_type_code",
      35
    ],
    [
      "ctx_sent_lb_2_num",
      30
    ],
    [
      "ctx_ths_hot_last_pct",
      30
    ],
    [
      "ctx_sent_mian_num",
      29
    ],
    [
      "ctx_sent_lt5_num",
      27
    ],
    [
      "ctx_sent_zb_num",
      25
    ],
    [
      "ctx_sent_down_num",
      25
    ],
    [
      "ctx_sent_damian_num",
      23
    ],
    [
      "ctx_zls_lbgd",
      21
    ],
    [
      "ctx_ths_hot_rank_diff",
      20
    ]
  ],
  "used_field_count": 52,
  "used_lane_counts": {
    "direct_formula": 3056,
    "event_state": 158,
    "lagged_context": 431
  }
}
```

## Blockers

- none

## Boundary

- This audit is evidence plumbing, not alpha proof.
- `train/validation/test` here is a chronological field-coverage audit split, not a promotion split.
- X0/R3 are read-only.
- Candidate files are checked for schema/lane usage and empty effective signal symptoms.
