# CN Fundamental Qualification Phase 3

This package is intended to be copied into the CN repository after
`CN_PIT_FUNDAMENTAL_FABRIC_PARTIALLY_COMPLETED`.

## Files

- `cn_fundamental_field_qualification_policy_v1.json`
- `build_cn_fundamental_qualification.py`
- `cn_unified_registry_merge_contract_v1.json`

## Recommended repository destinations

```text
runtime/run_plans/cn_fundamental_field_qualification_policy_v1.json
scripts/build_cn_fundamental_qualification.py
runtime/run_plans/cn_unified_registry_merge_contract_v1.json
```

## Run

```powershell
G:\PythonProject\.venv\Scripts\python.exe scripts\build_cn_fundamental_qualification.py
```

This is a non-performance qualification stage. It must not read returns, labels,
validation, holdout, or 2026 data.

The heuristic unit and family classifications are deliberately conservative.
They create a review queue; they do not authorize all qualified rows as raw
generator roots.
