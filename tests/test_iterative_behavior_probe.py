from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.runtime.cn_iterative_search_v1 import (
    _join_full_behavior_identities,
    _probe_pack,
)
from our_system_phase2.services.portfolio_behavior_archive import (
    bounded_label_free_behavior_probe,
)


def test_bounded_probe_reads_fields_without_label_sidecars(tmp_path) -> None:
    frame = pd.DataFrame(
        {
            "trade_time": pd.to_datetime(
                ["2024-01-02 09:31"] * 4 + ["2024-01-02 09:32"] * 4
            ),
            "code": ["a", "b", "c", "d"] * 2,
            "source_shard": np.zeros(8, dtype=np.uint16),
            "source_row_identity": np.arange(8, dtype=np.uint64),
            "duplicate_ordinal": np.zeros(8, dtype=np.uint32),
            "close": np.arange(1, 9, dtype=np.float64),
            "x": np.array([1, 2, 3, 4, 4, 3, 2, 1], dtype=np.float64),
        }
    )
    sidecar = tmp_path / "shard_00.parquet"
    frame.to_parquet(sidecar, index=False)
    primary = {
        "candidate_id": "primary",
        "pair_id": "pair-a",
        "pair_member_role": "PRIMARY",
        "route_id": "MINUTE_STATIC",
        "operator_family": "CSRank",
        "expression": "CSRank($x)",
    }
    control = {
        "candidate_id": "control",
        "pair_id": "pair-a",
        "pair_member_role": "CONTROL",
        "route_id": "MINUTE_STATIC",
        "operator_family": "CSRank",
        "expression": "CSRank(Sign($x))",
    }
    rows, audit = bounded_label_free_behavior_probe(
        candidates=[primary, control],
        field_sidecars=[sidecar],
        eligible_trade_dates=["2024-01-02"],
        coordinate_binding="binding-a",
        batch_id="batch_000",
        compute_threads=1,
        max_trade_times=2,
        min_obs=2,
        top_quantile=0.5,
    )

    assert rows[0]["behavior_status"] == "RESOLVED"
    assert rows[0]["behavior_probe_id"]
    assert rows[0]["signal_cluster_id"]
    assert audit["label_sidecar_paths_accepted"] == 0
    assert audit["validation_reads"] == 0


def test_full_behavior_row_closes_all_four_identities_by_pair() -> None:
    joined = _join_full_behavior_identities(
        [
            {
                "pair_id": "pair-a",
                "behavior_status": "RESOLVED",
                "structural_family_id": "",
                "signal_cluster_id": "",
                "portfolio_behavior_signature_id": "exact-a",
                "portfolio_behavior_family_id": "family-a",
            }
        ],
        [
            {
                "pair_id": "pair-a",
                "structural_family_id": "structural-a",
                "signal_cluster_id": "signal-a",
            }
        ],
    )
    assert joined[0]["structural_family_id"] == "structural-a"
    assert joined[0]["signal_cluster_id"] == "signal-a"
    assert joined[0]["portfolio_behavior_signature_id"] == "exact-a"
    assert joined[0]["portfolio_behavior_family_id"] == "family-a"


def test_probe_pack_uses_route_aware_coordinates(monkeypatch, tmp_path) -> None:
    calls = []

    def fake_probe(**kwargs):
        route_id = kwargs["candidates"][0]["route_id"]
        calls.append(
            {
                "route_id": route_id,
                "max_trade_dates": kwargs["max_trade_dates"],
                "max_trade_times": kwargs["max_trade_times"],
                "date_selection": kwargs["date_selection"],
            }
        )
        pair_id = kwargs["candidates"][0]["pair_id"]
        return ([{"pair_id": pair_id, "route_id": route_id, "behavior_status": "RESOLVED"}], {})

    monkeypatch.setattr(
        "our_system_phase2.runtime.cn_iterative_search_v1.bounded_label_free_behavior_probe",
        fake_probe,
    )
    rows = []
    for route_id in ("DISCLOSURE_EVENT", "MINUTE_STATIC"):
        for role in ("PRIMARY", "CONTROL"):
            rows.append(
                {
                    "pair_id": f"pair.{route_id}",
                    "candidate_id": f"{route_id}.{role}",
                    "pair_member_role": role,
                    "route_id": route_id,
                    "expression": "CSRank($x)",
                }
            )

    records, _ = _probe_pack(
        candidate_rows=rows,
        field_roots={"active_bar": tmp_path, "stock_session": tmp_path},
        train_dates=[f"2024-01-{day:02d}" for day in range(2, 20)],
        coordinate_binding="binding",
        batch_id="batch",
        compute_threads={"active_bar": 1, "stock_session": 1},
    )

    assert len(records) == 2
    assert sorted(calls, key=lambda row: row["route_id"]) == [
        {
            "route_id": "DISCLOSURE_EVENT",
            "max_trade_dates": 12,
            "max_trade_times": 1,
            "date_selection": "condition_activation",
        },
        {
            "route_id": "MINUTE_STATIC",
            "max_trade_dates": 4,
            "max_trade_times": 8,
            "date_selection": "calendar_stratified",
        },
    ]
