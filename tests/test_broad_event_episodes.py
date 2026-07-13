from __future__ import annotations

import numpy as np
import pandas as pd

from our_system_phase2.services.broad_event_episodes import (
    audit_episode_support,
    matched_control_contract,
    materialize_broad_event_episodes,
)
from our_system_phase2.runtime.cn_broad_event_canary import _attach_episode_outcomes, _candidate_rows


def _frame() -> pd.DataFrame:
    rows = []
    for day_index, day in enumerate(pd.date_range("2025-01-02", periods=12, freq="B")):
        for code_index, code in enumerate(("600001", "000001", "300001")):
            for minute in range(3):
                rows.append(
                    {
                        "code": code,
                        "trade_time": day + pd.Timedelta(hours=9, minutes=31 + minute),
                        "open": 10 + code_index,
                        "high": 10.1 + code_index,
                        "low": 9.9 + code_index,
                        "close": 10 + code_index + 0.01 * minute,
                        "evt_uplimit_active": float(minute >= 1 and day_index % 3 == 0),
                        "evt_uplimit_amount": float(100 + day_index),
                        "ctx_billboard_billboard_net_amt": float(day_index + code_index),
                        "ctx_rzrq_rzye": float(100 + 4 * day_index + code_index),
                        "ctx_holder_holder_num": float(1000 - day_index - code_index),
                        "ctx_ths_hot_rank": float(100 - 2 * day_index - code_index),
                        "ctx_ths_hot_rank_diff": float(12 if day_index % 2 else -12),
                        "ctx_sent_uplimit_num": float(10 + day_index),
                        "ctx_zls_ztjs": float(5 + day_index),
                    }
                )
    return pd.DataFrame(rows)


def test_episode_materialization_compresses_repeated_minute_and_market_rows() -> None:
    episodes, manifest = materialize_broad_event_episodes(_frame())
    assert manifest["minute_rows_used_as_inference_units"] is False
    assert episodes["episode_id"].is_unique
    assert episodes["admission_vote"].eq(1).all()
    market = episodes.loc[episodes["event_source"].eq("MARKET_ECOLOGY_TRANSITION")]
    assert market["entity_scope"].eq("MARKET").all()
    assert market["session"].is_unique
    vendor = episodes.loc[episodes["event_source"].eq("VENDOR_LIMIT_OCCURRENCE")]
    assert vendor.groupby(["entity_id", "session"]).size().max() == 1


def test_episode_audit_reports_episode_not_row_support() -> None:
    episodes, _ = materialize_broad_event_episodes(_frame())
    report = audit_episode_support(episodes)
    assert report["row_count_used_as_effective_sample_size"] is False
    assert report["episode_count"] == len(episodes)


def test_matched_controls_include_structural_and_episode_placebo() -> None:
    contract = matched_control_contract()
    assert contract["structural_control"]["same_episode_support_times"] is True
    assert contract["episode_placebo_control"]["same_episode_count"] is True
    assert contract["episode_placebo_control"]["ambiguous_rows_allowed_as_negative"] is False
    assert contract["one_episode_one_admission_vote"] is True


def test_episode_outcome_join_handles_nonconsecutive_symbol_episode_indices() -> None:
    frame = _frame()
    episodes, _ = materialize_broad_event_episodes(frame)
    episodes = episodes.sort_values("entity_scope", ascending=False).reset_index(drop=True)
    result = _attach_episode_outcomes(frame, episodes, [5])
    assert len(result) == len(episodes)
    assert "target_h5" in result
    assert "placebo_target_h5_o30" in result


def test_fixed_seeds_are_stratified_by_source_but_choose_distinct_mechanisms() -> None:
    frame = _frame()
    episodes, _ = materialize_broad_event_episodes(frame)
    observations = _attach_episode_outcomes(frame, episodes, [5, 15, 30])
    contract = {
        "required_event_sources": sorted(episodes["event_source"].unique().tolist()),
        "horizons_bars": [5, 15, 30],
        "budgets": {"event_conditioned": {"proposal": 96}},
    }
    # Use a divisible per-source budget for this compact fixture.
    proposal = len(contract["required_event_sources"]) * 12
    contract["budgets"]["event_conditioned"]["proposal"] = proposal
    first = pd.DataFrame(_candidate_rows(observations, contract, 1729))
    second = pd.DataFrame(_candidate_rows(observations, contract, 2718))
    first_event = first.loc[first["lane"].eq("event_conditioned")]
    second_event = second.loc[second["lane"].eq("event_conditioned")]
    assert first_event.groupby("source").size().eq(12).all()
    assert second_event.groupby("source").size().eq(12).all()
    assert set(first_event["canonical"]) != set(second_event["canonical"])
