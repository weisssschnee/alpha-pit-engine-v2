from __future__ import annotations

from collections import Counter
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from our_system_phase2.runtime.nextgen_dark_development_canary import (
    evaluate_strict_pack,
    generate_proposals,
    main,
    select_canary_candidates,
)
from our_system_phase2.services.feature_state_fabric import FieldRegistry
from our_system_phase2.services.hypothesis_lanes import default_nextgen_lane_registry


REPO = Path(__file__).resolve().parents[1]


def _plan() -> dict:
    return json.loads(
        (REPO / "runtime/run_plans/nextgen_dark_canary_plan_v1.json").read_text(
            encoding="utf-8"
        )
    )


def _synthetic_frame(fields: set[str]) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    rows: list[dict] = []
    for code_number in range(24):
        context = {field: float(rng.normal()) for field in fields if field.startswith("ctx_")}
        firstn = {
            field: float(rng.normal()) for field in fields if field.startswith("m1_first")
        }
        for minute in range(40):
            trade_time = pd.Timestamp("2025-04-01 09:30") + pd.Timedelta(minutes=minute)
            row = {
                "code": f"{code_number:06d}",
                "trade_time": trade_time,
                "signal_time": trade_time,
                "ctx_source_session_upper_bound": pd.Timestamp("2025-03-31"),
            }
            for field in fields:
                if field.startswith("ctx_"):
                    row[field] = context[field]
                elif field.startswith("m1_first"):
                    row[field] = firstn[field]
                elif field == "evt_uplimit_active":
                    row[field] = float((minute + code_number) % 13 == 0)
                elif field == "evt_uplimit_type_code":
                    row[field] = float((minute // 10) % 3)
                elif field.startswith("evt_"):
                    row[field] = float(rng.random())
                elif field in {"volume", "amount_yuan"}:
                    row[field] = float(abs(rng.normal()) + 1.0)
                else:
                    row[field] = float(10.0 + rng.normal())
            rows.append(row)
    return pd.DataFrame(rows)


def test_fixed_proposal_admission_and_strict_budgets_are_deterministic() -> None:
    lanes = default_nextgen_lane_registry()
    first = generate_proposals(lanes)
    second = generate_proposals(lanes)
    admission, strict = select_canary_candidates(first, lanes, _plan())

    assert len(first) == 1344
    assert [row["exact_identity"] for row in first] == [
        row["exact_identity"] for row in second
    ]
    assert len({row["exact_identity"] for row in first}) == 1344
    assert admission["selected_count"] <= 168
    assert len(admission["global_topk_baseline"]) == _plan()[
        "global_topk_baseline_quota"
    ]
    assert admission["global_topk_baseline_count"] == _plan()[
        "global_topk_baseline_quota"
    ]
    assert len(strict) == 64
    assert Counter(row["lane_id"] for row in strict) == _plan()["strict_eval_allocation"]
    assert all("plate" not in row["expression"].lower() for row in first)
    assert all("industry" not in row["expression"].lower() for row in first)


def test_one_candidate_per_lane_materializes_on_pit_guarded_frame() -> None:
    lanes = default_nextgen_lane_registry()
    proposals = generate_proposals(lanes)
    _, strict = select_canary_candidates(proposals, lanes, _plan())
    one_per_lane = []
    for lane_id in _plan()["strict_eval_allocation"]:
        one_per_lane.append(next(row for row in strict if row["lane_id"] == lane_id))
    fields = {field for row in one_per_lane for field in row["fields_list"]} | {"close"}
    registry = FieldRegistry.read(
        REPO / "runtime/field_registry/nextgen_dark_field_registry_v2.json"
    )

    metrics, diagnostics = evaluate_strict_pack(
        _synthetic_frame(fields),
        one_per_lane,
        registry,
        horizons=(1,),
        min_obs_per_time=20,
    )

    assert len(metrics) == 7
    assert metrics["candidate_id"].nunique() == 7
    assert metrics["signal_nonnull"].gt(0).all()
    assert diagnostics["data_role"] == "development"
    assert diagnostics["forward_2026_accessed"] is False
    assert diagnostics["adaptive_reward_updated"] is False


def test_canary_rejects_missing_independent_authorization(tmp_path: Path) -> None:
    arguments = [
        "--panel-root",
        str(tmp_path / "panels"),
        "--split-manifest",
        str(tmp_path / "split.csv"),
        "--field-registry",
        str(tmp_path / "fields.json"),
        "--lane-registry",
        str(tmp_path / "lanes.json"),
        "--canary-plan",
        str(tmp_path / "plan.json"),
        "--benchmark-registry",
        str(tmp_path / "benchmarks.json"),
        "--augmentation-summary",
        str(tmp_path / "augmentation.json"),
        "--output-root",
        str(tmp_path / "output"),
        "--authorization",
        "wrong-token",
        "--frozen-sha",
        "0" * 40,
    ]

    with pytest.raises(PermissionError, match="authorization token"):
        main(arguments)
