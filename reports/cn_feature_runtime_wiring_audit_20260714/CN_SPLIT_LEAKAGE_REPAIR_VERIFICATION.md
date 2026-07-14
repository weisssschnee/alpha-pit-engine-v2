# CN Split Leakage Repair Verification

Status: `PARTIAL_RUNTIME_REACHABILITY_MISMATCH`

The fixed manifest is correct (485 dates: 364 train, 73 validation, 48 holdout), the primary launcher passes it, and final exact normalization can hard-fail unassigned dates. However, 2 worker/recovery paths remain reachable with local or absent manifest assignment before final normalization.

This audit did not read validation or holdout contents. It inspected routing and source reachability only. See `CN_GLOBAL_SPLIT_RUNTIME_REACHABILITY_AUDIT.csv` for component evidence.
