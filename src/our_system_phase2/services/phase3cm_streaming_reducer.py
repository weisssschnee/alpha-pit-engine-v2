"""Minute-row-free reducer for Phase3CM portfolio and reward atoms."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping, Sequence

import numpy as np

from our_system_phase2.services.phase3cm_streaming_portfolio import (
    PortfolioBlockResult,
    STAT_FIELDS,
)


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


class StreamingPortfolioReducer:
    """Accumulate only bounded statistics and daily vectors."""

    def __init__(self, *, candidates: Sequence[Mapping[str, Any]], horizons: Sequence[int]) -> None:
        self.candidates = tuple(dict(candidate) for candidate in candidates)
        self.horizons = tuple(int(value) for value in horizons)
        if not self.candidates or not self.horizons:
            raise ValueError("candidates and horizons are required")
        self.candidate_identity = _stable_hash(
            [
                {
                    "candidate_id": row.get("candidate_id"),
                    "expression_hash": row.get("expression_hash"),
                }
                for row in self.candidates
            ]
        )
        self.stats = np.zeros(
            (len(self.candidates), len(self.horizons) + 1, len(STAT_FIELDS)),
            dtype=np.float64,
        )
        self.daily: dict[str, np.ndarray] = {}
        self.monthly: dict[str, np.ndarray] = {}
        self.completed_block_count = 0
        self.coordinate_rows_retained = 0

    def update(self, result: PortfolioBlockResult, *, day_labels: Sequence[str]) -> None:
        expected_stats = self.stats.shape
        if result.stats.shape != expected_stats:
            raise ValueError("portfolio stats shape drift")
        if result.daily.shape[:2] != expected_stats[:2] or result.daily.shape[2] != len(day_labels):
            raise ValueError("portfolio daily shape drift")
        if result.daily.shape[3] != len(STAT_FIELDS):
            raise ValueError("portfolio daily schema drift")
        if int(result.coordinate_rows_retained) != 0:
            raise ValueError("formal streaming reducer forbids coordinate-row retention")
        self.stats += result.stats
        for day_index, label in enumerate(day_labels):
            date = str(label)
            values = np.asarray(result.daily[:, :, day_index, :], dtype=np.float64)
            if date not in self.daily:
                self.daily[date] = np.zeros_like(values)
            self.daily[date] += values
            month = date[:7]
            if month not in self.monthly:
                self.monthly[month] = np.zeros_like(values)
            self.monthly[month] += values
        self.completed_block_count += 1

    def reward_atoms(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        field = {name: index for index, name in enumerate(STAT_FIELDS)}
        slots: tuple[int | str, ...] = (*self.horizons, "all")
        for candidate_index, candidate in enumerate(self.candidates):
            for slot_index, slot in enumerate(slots):
                for trade_date in sorted(self.daily):
                    values = self.daily[trade_date][candidate_index, slot_index]
                    if int(values[field["curve_count"]]) == 0:
                        continue
                    rows.append(
                        {
                            "candidate_id": candidate.get("candidate_id"),
                            "expression_hash": candidate.get("expression_hash"),
                            "split": "train",
                            "horizon_min": str(slot),
                            "trade_date": trade_date,
                            "curve_count": int(values[field["curve_count"]]),
                            "net_return_sum": float(values[field["net_return_sum"]]),
                            "raw_return_sum": float(values[field["raw_return_sum"]]),
                            "net_positive_count": int(values[field["net_positive_count"]]),
                            "downside_square_sum": float(values[field["downside_square_sum"]]),
                            "daily_net_return": float(values[field["net_return_sum"]]),
                            "market_mean_return_sum": float(values[field["market_mean_return_sum"]]),
                            "market_mean_return_count": int(values[field["market_mean_return_count"]]),
                            "turnover_sum": float(values[field["turnover_sum"]]),
                            "turnover_count": int(values[field["turnover_count"]]),
                            "rank_ic_sum": float(values[field["rank_ic_sum"]]),
                            "rank_ic_count": int(values[field["rank_ic_count"]]),
                            "rank_ic_positive_count": int(values[field["rank_ic_positive_count"]]),
                            "support_count": float(values[field["support_count"]]),
                            "selected_count": float(values[field["selected_count"]]),
                        }
                    )
        return rows

    def continuation_payload(self) -> dict[str, Any]:
        return {
            "candidate_identity": self.candidate_identity,
            "horizons": list(self.horizons),
            "stats": self.stats.copy(),
            "daily": {key: value.copy() for key, value in sorted(self.daily.items())},
            "monthly": {key: value.copy() for key, value in sorted(self.monthly.items())},
            "completed_block_count": int(self.completed_block_count),
            "coordinate_rows_retained": int(self.coordinate_rows_retained),
        }

    def restore_continuation_payload(self, payload: Mapping[str, Any]) -> None:
        if str(payload.get("candidate_identity")) != self.candidate_identity:
            raise ValueError("streaming reducer candidate identity drift")
        if tuple(int(value) for value in payload.get("horizons") or ()) != self.horizons:
            raise ValueError("streaming reducer horizon drift")
        stats = np.asarray(payload["stats"], dtype=np.float64)
        if stats.shape != self.stats.shape:
            raise ValueError("streaming reducer stats shape drift")
        if int(payload.get("coordinate_rows_retained") or 0) != 0:
            raise ValueError("checkpoint contains forbidden coordinate rows")
        self.stats[...] = stats
        self.daily = {
            str(key): np.asarray(value, dtype=np.float64).copy()
            for key, value in dict(payload.get("daily") or {}).items()
        }
        self.monthly = {
            str(key): np.asarray(value, dtype=np.float64).copy()
            for key, value in dict(payload.get("monthly") or {}).items()
        }
        self.completed_block_count = int(payload.get("completed_block_count") or 0)
        self.coordinate_rows_retained = 0

    def contract(self) -> dict[str, Any]:
        return {
            "schema_version": "cn_phase3cm_streaming_reducer_contract_v1",
            "candidate_identity": self.candidate_identity,
            "horizons": list(self.horizons),
            "stat_fields": list(STAT_FIELDS),
            "minute_coordinate_rows_retained": 0,
            "bounded_payloads": [
                "global sufficient statistics",
                "daily aggregate vector",
                "monthly aggregate vector",
                "portfolio continuation state",
            ],
            "data_role": "development",
            "validation_reads": 0,
            "holdout_reads": 0,
            "forward_2026_reads": 0,
        }
