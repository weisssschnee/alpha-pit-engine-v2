"""Deterministic, fail-closed A-share long-only execution replay.

This module is an implementation detail of the existing real-market replay
path. It is not a new evaluation authority. A caller may construct an
``A_SHARE_TRADABILITY_REPLAY_V1`` receipt only after this kernel completes
under an explicit fee schedule, PIT universe contract, and train-only access
receipt.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
from typing import Any, Mapping

import numpy as np
import pandas as pd

from our_system_phase2.services.development_only_data_access import (
    canonical_json_hash,
)


REPLAY_KERNEL_VERSION = "a_share_long_only_open_rebalance_v2"
ACCOUNTING_LEDGER_VERSION = "a_share_lot_cash_pnl_ledger_v1"
LOT_LEDGER_COLUMNS = (
    "lot_id",
    "code",
    "acquired_date",
    "acquired_session_ordinal",
    "sellable_date",
    "sellable_session_ordinal",
    "acquisition_price",
    "original_shares",
    "remaining_shares",
    "disposed_shares",
    "buy_fee_cny",
    "original_cost_basis_cny",
    "remaining_cost_basis_cny",
    "realized_cost_basis_cny",
    "corporate_action_share_delta",
)
LOT_CONSUMPTION_LEDGER_COLUMNS = (
    "session_date",
    "session_ordinal",
    "code",
    "lot_id",
    "acquired_date",
    "sellable_date",
    "shares",
    "price",
    "allocated_cost_basis_cny",
    "fill_reason",
    "allocated_sell_fee_cny",
    "realized_trade_pnl_cny",
)
REQUIRED_SESSION_COLUMNS = frozenset(
    {
        "date",
        "code",
        "open",
        "close",
        "signal",
        "security_type",
        "exchange",
        "universe_eligible",
        "listing_age_sessions",
        "is_st",
        "is_delisting",
        "suspended",
        "up_limit_price",
        "down_limit_price",
        "corporate_action_cash_per_share",
        "corporate_action_share_multiplier",
        "is_terminal_session",
        "terminal_liquidation_price",
    }
)

ENDING_BOOK_REQUIRE_FLAT_FINAL_OPEN = "REQUIRE_FLAT_FINAL_OPEN"
ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET = "FINAL_CLOSE_MARK_TO_MARKET"
ENDING_BOOK_POLICIES = frozenset(
    {
        ENDING_BOOK_REQUIRE_FLAT_FINAL_OPEN,
        ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET,
    }
)


class AShareCandidateReplayBlockerError(ValueError):
    """Base class for candidate-local outcomes that must not abort a cohort."""

    blocker_code: str

    def blocker_details(self) -> dict[str, Any]:
        return {"blocker_code": self.blocker_code}


class AShareTerminalLiquidationError(AShareCandidateReplayBlockerError):
    """A candidate-specific fail-closed terminal liquidity outcome."""

    blocker_code = "FINAL_SESSION_UNLIQUIDATED_HOLDINGS"

    def __init__(self, remaining_holdings: list[str]) -> None:
        self.remaining_holdings = tuple(sorted(str(code) for code in remaining_holdings))
        super().__init__(
            "final replay session could not liquidate every holding; "
            f"remaining={list(self.remaining_holdings)}"
        )

    def blocker_details(self) -> dict[str, Any]:
        return {
            **super().blocker_details(),
            "remaining_holdings": list(self.remaining_holdings),
        }


class AShareCorporateActionFractionalSharesError(
    AShareCandidateReplayBlockerError
):
    """Candidate-specific non-integer corporate-action holdings outcome."""

    blocker_code = "CORPORATE_ACTION_FRACTIONAL_SHARES"

    def __init__(
        self,
        *,
        code: str,
        session_date: str,
        opening_shares: int,
        multiplier: float,
        adjusted_shares: float,
    ) -> None:
        self.code = str(code)
        self.session_date = str(session_date)
        self.opening_shares = int(opening_shares)
        self.multiplier = float(multiplier)
        self.adjusted_shares = float(adjusted_shares)
        super().__init__(
            "corporate action produced fractional shares under "
            "FAIL_CLOSED_NON_INTEGER policy"
        )

    def blocker_details(self) -> dict[str, Any]:
        return {
            **super().blocker_details(),
            "security_code": self.code,
            "session_date": self.session_date,
            "opening_shares": self.opening_shares,
            "corporate_action_share_multiplier": self.multiplier,
            "adjusted_shares": self.adjusted_shares,
            "corporate_action_fractional_share_policy": (
                "FAIL_CLOSED_NON_INTEGER"
            ),
        }


@dataclass(frozen=True, slots=True)
class AShareFeeSchedule:
    """Explicit cash fee schedule in basis points and CNY.

    The commission rate is account-specific and therefore has no default.
    Exchange handling, transfer, and stamp-duty fields must be frozen by the
    caller for the replay dates.
    """

    commission_bps: float
    minimum_commission_cny: float
    exchange_handling_bps: float
    transfer_fee_bps: float
    sell_stamp_duty_bps: float
    effective_start: str
    effective_end: str
    source_reference: str

    def validate(self) -> None:
        numeric = {
            "commission_bps": self.commission_bps,
            "minimum_commission_cny": self.minimum_commission_cny,
            "exchange_handling_bps": self.exchange_handling_bps,
            "transfer_fee_bps": self.transfer_fee_bps,
            "sell_stamp_duty_bps": self.sell_stamp_duty_bps,
        }
        for name, value in numeric.items():
            if not math.isfinite(float(value)) or float(value) < 0:
                raise ValueError(f"{name} must be finite and nonnegative")
        try:
            effective_start = pd.Timestamp(self.effective_start).normalize()
            effective_end = pd.Timestamp(self.effective_end).normalize()
        except (TypeError, ValueError) as exc:
            raise ValueError("fee schedule effective dates are invalid") from exc
        if pd.isna(effective_start) or pd.isna(effective_end):
            raise ValueError("fee schedule effective dates are invalid")
        if effective_start > effective_end:
            raise ValueError("fee schedule effective_start exceeds effective_end")
        if not self.source_reference:
            raise ValueError("fee schedule source_reference is required")

    @property
    def payload_sha256(self) -> str:
        self.validate()
        return canonical_json_hash(
            {"schema_version": "a_share_fee_schedule_v1", **asdict(self)}
        )

    def fee(self, notional: float, *, side: str) -> float:
        value = float(notional)
        if value <= 0:
            return 0.0
        normalized_side = str(side).upper()
        if normalized_side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported fee side: {side}")
        commission = max(
            value * float(self.commission_bps) / 10_000.0,
            float(self.minimum_commission_cny),
        )
        shared = value * (
            float(self.exchange_handling_bps) + float(self.transfer_fee_bps)
        ) / 10_000.0
        stamp = (
            value * float(self.sell_stamp_duty_bps) / 10_000.0
            if normalized_side == "SELL"
            else 0.0
        )
        return commission + shared + stamp


@dataclass(frozen=True, slots=True)
class AShareUniversePolicy:
    minimum_listing_sessions: int
    allowed_exchanges: tuple[str, ...] = ("SSE", "SZSE")
    security_type: str = "A_SHARE"
    exclude_st: bool = True
    exclude_delisting: bool = True
    pit_membership: bool = True
    survivorship_free: bool = True
    delisting_history_included: bool = True
    source_reference: str = ""

    def validate(self) -> None:
        if int(self.minimum_listing_sessions) < 1:
            raise ValueError("minimum_listing_sessions must be positive")
        if not self.allowed_exchanges:
            raise ValueError("allowed_exchanges must not be empty")
        if not self.security_type:
            raise ValueError("security_type must not be empty")
        if not self.source_reference:
            raise ValueError("universe source_reference is required")
        if not (
            self.pit_membership
            and self.survivorship_free
            and self.delisting_history_included
            and self.exclude_st
            and self.exclude_delisting
        ):
            raise ValueError(
                "promotion-grade replay requires PIT, survivorship-free, "
                "delisting-inclusive, non-ST and non-delisting universe rules"
            )

    @property
    def payload_sha256(self) -> str:
        self.validate()
        payload = asdict(self)
        payload["allowed_exchanges"] = list(self.allowed_exchanges)
        return canonical_json_hash(
            {"schema_version": "a_share_universe_policy_v1", **payload}
        )


@dataclass(frozen=True, slots=True)
class AShareExecutionPolicy:
    signal_clock: str = "SESSION_CLOSE_T"
    execution_clock: str = "NEXT_SESSION_OPEN_T_PLUS_1"
    rebalance_frequency: str = "EACH_SESSION"
    long_only: bool = True
    top_quantile: float = 0.20
    initial_cash_cny: float = 1_000_000.0
    default_lot_size: int = 100
    price_tick: float = 0.01

    def validate(self) -> None:
        if self.signal_clock != "SESSION_CLOSE_T":
            raise ValueError("signal_clock must be SESSION_CLOSE_T")
        if self.execution_clock != "NEXT_SESSION_OPEN_T_PLUS_1":
            raise ValueError("execution_clock must be NEXT_SESSION_OPEN_T_PLUS_1")
        if self.rebalance_frequency != "EACH_SESSION":
            raise ValueError("rebalance_frequency must be EACH_SESSION")
        if not self.long_only:
            raise ValueError("A-share replay is long-only")
        if not 0 < float(self.top_quantile) <= 1:
            raise ValueError("top_quantile must be in (0, 1]")
        if float(self.initial_cash_cny) <= 0:
            raise ValueError("initial_cash_cny must be positive")
        if int(self.default_lot_size) <= 0:
            raise ValueError("default_lot_size must be positive")
        if float(self.price_tick) <= 0:
            raise ValueError("price_tick must be positive")

    @property
    def payload_sha256(self) -> str:
        self.validate()
        return canonical_json_hash(
            {"schema_version": "a_share_execution_policy_v1", **asdict(self)}
        )


@dataclass(frozen=True, slots=True)
class AShareCorporateActionPolicy:
    """Frozen timing and accounting semantics for finalist replay.

    Input rows must already be PIT-aligned. Cash is credited on its declared
    payment session and share multipliers are applied on their declared
    effective session, both before that session's sell/buy rebalance. Only
    positions carried into the session receive the adjustment.
    """

    cash_credit_clock: str = "PAYMENT_SESSION_OPEN"
    share_adjustment_clock: str = "EFFECTIVE_SESSION_OPEN"
    entitlement_basis: str = "OPENING_HOLDINGS_ONLY"
    fractional_share_policy: str = "FAIL_CLOSED_NON_INTEGER"
    delisting_liquidation_clock: str = "TERMINAL_SESSION_OPEN"
    delisting_recovery_policy: str = "ZERO_RECOVERY_FAIL_CLOSED"
    replay_end_liquidation_clock: str = "FINAL_SESSION_OPEN"
    require_flat_at_replay_end: bool = True
    source_reference: str = ""

    def validate(self) -> None:
        expected = {
            "cash_credit_clock": "PAYMENT_SESSION_OPEN",
            "share_adjustment_clock": "EFFECTIVE_SESSION_OPEN",
            "entitlement_basis": "OPENING_HOLDINGS_ONLY",
            "fractional_share_policy": "FAIL_CLOSED_NON_INTEGER",
            "delisting_liquidation_clock": "TERMINAL_SESSION_OPEN",
            "delisting_recovery_policy": "ZERO_RECOVERY_FAIL_CLOSED",
            "replay_end_liquidation_clock": "FINAL_SESSION_OPEN",
        }
        for field, value in expected.items():
            if str(getattr(self, field)) != value:
                raise ValueError(f"{field} must be {value}")
        if not self.require_flat_at_replay_end:
            raise ValueError("finalist replay must require a flat ending book")
        if not self.source_reference:
            raise ValueError("corporate-action policy source_reference is required")

    @property
    def payload_sha256(self) -> str:
        self.validate()
        return canonical_json_hash(
            {
                "schema_version": "a_share_corporate_action_policy_v1",
                **asdict(self),
            }
        )


def _sortino(values: pd.Series) -> float | None:
    clean = pd.to_numeric(values, errors="coerce").dropna()
    if clean.empty:
        return None
    downside = np.minimum(clean.to_numpy(dtype=float), 0.0)
    downside_deviation = float(np.sqrt(np.mean(np.square(downside))))
    mean = float(clean.mean())
    if downside_deviation <= 0:
        return mean * math.sqrt(252.0)
    return mean / downside_deviation * math.sqrt(252.0)


def _truthy(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False).astype(bool)
    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().any():
        return numeric.fillna(0.0).ne(0.0)
    return values.astype(str).str.strip().str.lower().isin(
        {"1", "true", "yes", "y"}
    )


def _prepare_sessions(
    frame: pd.DataFrame,
    *,
    universe_policy: AShareUniversePolicy,
) -> pd.DataFrame:
    missing = sorted(REQUIRED_SESSION_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"A-share replay frame missing columns: {missing}")
    out = frame.copy()
    out["date"] = pd.to_datetime(out["date"], errors="raise").dt.normalize()
    out["code"] = out["code"].astype(str)
    if out.duplicated(["date", "code"]).any():
        raise ValueError("A-share replay frame has duplicate date/code rows")
    for column in (
        "open",
        "close",
        "signal",
        "listing_age_sessions",
        "up_limit_price",
        "down_limit_price",
        "corporate_action_cash_per_share",
        "corporate_action_share_multiplier",
        "terminal_liquidation_price",
    ):
        out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in (
        "universe_eligible",
        "is_st",
        "is_delisting",
        "suspended",
        "is_terminal_session",
    ):
        out[column] = _truthy(out[column])
    if "lot_size" in out:
        out["lot_size"] = pd.to_numeric(out["lot_size"], errors="coerce")
    eligible = (
        out["security_type"].astype(str).eq(universe_policy.security_type)
        & out["exchange"].astype(str).isin(universe_policy.allowed_exchanges)
        & out["universe_eligible"]
        & out["listing_age_sessions"].ge(
            int(universe_policy.minimum_listing_sessions)
        )
        & ~out["is_st"]
        & ~out["is_delisting"]
    )
    out["promotion_universe_eligible"] = eligible.fillna(False)
    cash = out["corporate_action_cash_per_share"]
    multiplier = out["corporate_action_share_multiplier"]
    if cash.isna().any() or cash.lt(0).any():
        raise ValueError(
            "corporate_action_cash_per_share must be explicit and nonnegative"
        )
    if multiplier.isna().any() or multiplier.le(0).any():
        raise ValueError(
            "corporate_action_share_multiplier must be explicit and positive"
        )
    terminal = out["is_terminal_session"]
    if (terminal & ~out["is_delisting"]).any():
        raise ValueError("terminal sessions must also declare is_delisting")
    terminal_price = out["terminal_liquidation_price"]
    if (terminal & (terminal_price.isna() | terminal_price.lt(0))).any():
        raise ValueError(
            "terminal sessions require an explicit nonnegative "
            "terminal_liquidation_price"
        )
    terminal_counts = out.loc[terminal].groupby("code", sort=False).size()
    if terminal_counts.gt(1).any():
        raise ValueError("a security may declare at most one terminal session")
    out = out.sort_values(["date", "code"], kind="mergesort").reset_index(
        drop=True
    )
    terminal_dates = out.loc[out["is_terminal_session"], ["code", "date"]]
    if not terminal_dates.empty:
        last_dates = out.groupby("code", sort=False)["date"].max()
        for row in terminal_dates.itertuples(index=False):
            if pd.Timestamp(row.date) != pd.Timestamp(last_dates.loc[row.code]):
                raise ValueError(
                    "terminal session must be the final explicit row for its security"
                )
    return out


def _blocked_buy(row: Mapping[str, Any], *, tick: float) -> bool:
    if bool(row["suspended"]):
        return True
    open_price = float(row["open"])
    limit = float(row["up_limit_price"])
    return not math.isfinite(open_price) or not math.isfinite(limit) or (
        open_price >= limit - tick / 2.0
    )


def _blocked_sell(row: Mapping[str, Any], *, tick: float) -> bool:
    if bool(row["suspended"]):
        return True
    open_price = float(row["open"])
    limit = float(row["down_limit_price"])
    return not math.isfinite(open_price) or not math.isfinite(limit) or (
        open_price <= limit + tick / 2.0
    )


def _ledger_tolerance(value: float) -> float:
    return max(1e-6, abs(float(value)) * 1e-12)


def _active_lots(
    lots: list[dict[str, Any]],
    *,
    code: str | None = None,
) -> list[dict[str, Any]]:
    selected = [lot for lot in lots if int(lot["remaining_shares"]) > 0]
    if code is not None:
        selected = [lot for lot in selected if str(lot["code"]) == str(code)]
    return sorted(
        selected,
        key=lambda lot: (
            int(lot["acquired_session_ordinal"]),
            str(lot["lot_id"]),
        ),
    )


def _lot_quantity(
    lots: list[dict[str, Any]],
    *,
    code: str,
) -> int:
    return sum(
        int(lot["remaining_shares"])
        for lot in _active_lots(lots, code=code)
    )


def _apply_share_multiplier_to_lots(
    lots: list[dict[str, Any]],
    *,
    code: str,
    opening_shares: int,
    adjusted_shares: int,
    multiplier: float,
) -> None:
    selected = _active_lots(lots, code=code)
    if sum(int(lot["remaining_shares"]) for lot in selected) != int(
        opening_shares
    ):
        raise RuntimeError(f"opening lot quantity drift for {code}")
    raw = [int(lot["remaining_shares"]) * float(multiplier) for lot in selected]
    allocated = [int(math.floor(value)) for value in raw]
    remainder = int(adjusted_shares) - sum(allocated)
    if remainder < 0 or remainder > len(selected):
        raise RuntimeError(f"corporate-action lot allocation failed for {code}")
    ranked = sorted(
        range(len(selected)),
        key=lambda index: (
            -(raw[index] - allocated[index]),
            str(selected[index]["lot_id"]),
        ),
    )
    for index in ranked[:remainder]:
        allocated[index] += 1
    for lot, shares in zip(selected, allocated, strict=True):
        previous = int(lot["remaining_shares"])
        lot["remaining_shares"] = int(shares)
        lot["corporate_action_share_delta"] = int(
            lot["corporate_action_share_delta"]
        ) + int(shares) - previous
    if _lot_quantity(lots, code=code) != int(adjusted_shares):
        raise RuntimeError(f"adjusted lot quantity drift for {code}")


def _consume_fifo_lots(
    lots: list[dict[str, Any]],
    *,
    code: str,
    shares: int,
    session_ordinal: int,
    session_date: str,
    price: float,
    sell_fee_cny: float,
    fill_reason: str,
) -> tuple[float, float, list[dict[str, Any]]]:
    remaining = int(shares)
    consumed_cost_basis = 0.0
    consumptions: list[dict[str, Any]] = []
    for lot in _active_lots(lots, code=code):
        if remaining <= 0:
            break
        if int(lot["sellable_session_ordinal"]) > int(session_ordinal):
            raise RuntimeError(
                f"T+1 lot sold before sellable session: {lot['lot_id']}"
            )
        available = int(lot["remaining_shares"])
        take = min(remaining, available)
        lot_cost_basis = float(lot["remaining_cost_basis_cny"])
        cost_basis = (
            lot_cost_basis
            if take == available
            else lot_cost_basis * take / available
        )
        lot["remaining_shares"] = available - take
        lot["remaining_cost_basis_cny"] = lot_cost_basis - cost_basis
        lot["disposed_shares"] = int(lot["disposed_shares"]) + take
        lot["realized_cost_basis_cny"] = float(
            lot["realized_cost_basis_cny"]
        ) + cost_basis
        consumed_cost_basis += cost_basis
        remaining -= take
        consumptions.append(
            {
                "session_date": session_date,
                "session_ordinal": int(session_ordinal),
                "code": str(code),
                "lot_id": str(lot["lot_id"]),
                "acquired_date": str(lot["acquired_date"]),
                "sellable_date": str(lot["sellable_date"]),
                "shares": int(take),
                "price": float(price),
                "allocated_cost_basis_cny": float(cost_basis),
                "fill_reason": str(fill_reason),
            }
        )
    if remaining != 0:
        raise RuntimeError(
            f"lot quantity insufficient for {code}: missing={remaining}"
        )
    allocated_sell_fee = 0.0
    sold_shares = int(shares)
    for index, row in enumerate(consumptions):
        fee = (
            float(sell_fee_cny) - allocated_sell_fee
            if index == len(consumptions) - 1
            else float(sell_fee_cny) * int(row["shares"]) / sold_shares
        )
        allocated_sell_fee += fee
        row["allocated_sell_fee_cny"] = float(fee)
        row["realized_trade_pnl_cny"] = float(
            int(row["shares"]) * float(price)
            - float(row["allocated_cost_basis_cny"])
            - fee
        )
    realized_trade_pnl = float(shares) * float(price) - float(
        sell_fee_cny
    ) - consumed_cost_basis
    return consumed_cost_basis, realized_trade_pnl, consumptions


def run_a_share_long_only_replay(
    frame: pd.DataFrame,
    *,
    fee_schedule: AShareFeeSchedule,
    universe_policy: AShareUniversePolicy,
    execution_policy: AShareExecutionPolicy,
    corporate_action_policy: AShareCorporateActionPolicy,
    ending_book_policy: str = ENDING_BOOK_REQUIRE_FLAT_FINAL_OPEN,
) -> dict[str, Any]:
    """Replay close-t signals at next-session open with real inventory.

    Sell orders blocked by suspension or an opening down-limit remain in the
    portfolio and are retried at later rebalances. Buy orders at an opening
    up-limit or suspension are not filled. Trades are lot-rounded, long-only,
    cash-funded, and charged the complete frozen fee schedule.
    """

    fee_schedule.validate()
    universe_policy.validate()
    execution_policy.validate()
    corporate_action_policy.validate()
    if ending_book_policy not in ENDING_BOOK_POLICIES:
        raise ValueError(
            "ending_book_policy must be one of "
            f"{sorted(ENDING_BOOK_POLICIES)}"
        )
    sessions = _prepare_sessions(frame, universe_policy=universe_policy)
    dates = list(pd.Index(sessions["date"].drop_duplicates()).sort_values())
    if len(dates) < 3:
        raise ValueError("A-share replay requires at least three sessions")
    effective_start = pd.Timestamp(fee_schedule.effective_start).normalize()
    effective_end = pd.Timestamp(fee_schedule.effective_end).normalize()
    if pd.Timestamp(dates[0]) < effective_start or pd.Timestamp(dates[-1]) > effective_end:
        raise ValueError(
            "fee schedule does not cover replay dates: "
            f"{pd.Timestamp(dates[0]).date()}..{pd.Timestamp(dates[-1]).date()} "
            f"outside {effective_start.date()}..{effective_end.date()}"
        )

    by_date = {
        pd.Timestamp(date): day.set_index("code", drop=False)
        for date, day in sessions.groupby("date", sort=True)
    }
    cash = float(execution_policy.initial_cash_cny)
    holdings: dict[str, int] = {}
    lots: list[dict[str, Any]] = []
    lot_consumptions: list[dict[str, Any]] = []
    daily_ledger_rows: list[dict[str, Any]] = []
    lot_sequence = 0
    last_close: dict[str, float] = {}
    fills: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    blocked_buy_count = 0
    blocked_sell_count = 0
    total_fees = 0.0
    corporate_action_cash_cny = 0.0
    corporate_action_share_delta = 0
    terminal_liquidation_count = 0
    cumulative_realized_trade_pnl_cny = 0.0
    maximum_cash_identity_error_cny = 0.0
    maximum_nav_identity_error_cny = 0.0
    maximum_pnl_identity_error_cny = 0.0
    maximum_lot_quantity_error = 0

    previous_nav = float(execution_policy.initial_cash_cny)
    for ordinal, date in enumerate(dates):
        day = by_date[pd.Timestamp(date)]
        session_date = pd.Timestamp(date).date().isoformat()
        opening_cash_cny = float(cash)
        session_corporate_action_cash_cny = 0.0
        session_sell_notional_cny = 0.0
        session_sell_fees_cny = 0.0
        session_buy_notional_cny = 0.0
        session_buy_fees_cny = 0.0
        session_realized_trade_pnl_cny = 0.0

        # Apply already PIT-aligned cash/share events to positions carried into
        # the session. Same-session purchases cannot receive the adjustment.
        for code in sorted(list(holdings)):
            if code not in day.index:
                raise ValueError(
                    "held code missing from session panel; suspension/delisting "
                    f"rows must be explicit: {code} on {pd.Timestamp(date).date()}"
                )
            row = day.loc[code]
            opening_shares = int(holdings[code])
            cash_per_share = float(row["corporate_action_cash_per_share"])
            if cash_per_share:
                credit = opening_shares * cash_per_share
                cash += credit
                corporate_action_cash_cny += credit
                session_corporate_action_cash_cny += credit
            multiplier = float(row["corporate_action_share_multiplier"])
            adjusted = opening_shares * multiplier
            rounded = round(adjusted)
            if not math.isclose(adjusted, rounded, rel_tol=0.0, abs_tol=1e-9):
                raise AShareCorporateActionFractionalSharesError(
                    code=str(code),
                    session_date=str(pd.Timestamp(date).date()),
                    opening_shares=opening_shares,
                    multiplier=multiplier,
                    adjusted_shares=adjusted,
                )
            adjusted_shares = int(rounded)
            if adjusted_shares <= 0:
                raise ValueError("corporate action produced nonpositive shares")
            holdings[code] = adjusted_shares
            corporate_action_share_delta += adjusted_shares - opening_shares
            _apply_share_multiplier_to_lots(
                lots,
                code=str(code),
                opening_shares=opening_shares,
                adjusted_shares=adjusted_shares,
                multiplier=multiplier,
            )

        # Delisting terminal rows are explicit cash exits, not disappearing
        # panel rows. They occur before the ordinary session rebalance.
        for code in sorted(list(holdings)):
            row = day.loc[code]
            if not bool(row["is_terminal_session"]):
                continue
            shares = int(holdings[code])
            price = float(row["terminal_liquidation_price"])
            notional = shares * price
            fee = fee_schedule.fee(notional, side="SELL")
            _, realized_trade_pnl, consumptions = _consume_fifo_lots(
                lots,
                code=str(code),
                shares=shares,
                session_ordinal=ordinal,
                session_date=session_date,
                price=price,
                sell_fee_cny=fee,
                fill_reason="DELISTING_TERMINAL_LIQUIDATION",
            )
            lot_consumptions.extend(consumptions)
            cumulative_realized_trade_pnl_cny += realized_trade_pnl
            session_realized_trade_pnl_cny += realized_trade_pnl
            holdings.pop(code)
            cash += notional - fee
            total_fees += fee
            session_sell_notional_cny += notional
            session_sell_fees_cny += fee
            terminal_liquidation_count += 1
            fills.append(
                {
                    "date": pd.Timestamp(date).date().isoformat(),
                    "code": code,
                    "side": "SELL",
                    "shares": shares,
                    "price": price,
                    "notional": notional,
                    "fee": fee,
                    "fill_reason": "DELISTING_TERMINAL_LIQUIDATION",
                }
            )

        for code, row in day.iterrows():
            close = float(row["close"])
            if math.isfinite(close) and close > 0:
                last_close[str(code)] = close

        desired_codes: list[str] = []
        if ordinal > 0 and ordinal < len(dates) - 1:
            signal_day = by_date[pd.Timestamp(dates[ordinal - 1])]
            pool = signal_day[
                signal_day["promotion_universe_eligible"]
                & signal_day["signal"].notna()
            ]
            if not pool.empty and pool["signal"].nunique(dropna=True) >= 2:
                count = max(
                    1,
                    int(
                        math.ceil(
                            len(pool) * float(execution_policy.top_quantile)
                        )
                    ),
                )
                desired_codes = (
                    pool.reset_index(drop=True).sort_values(
                        ["signal", "code"], ascending=[False, True]
                    )
                    .head(count)["code"]
                    .astype(str)
                    .tolist()
                )
        desired_set = set(desired_codes)
        final_close_mark_only = (
            ending_book_policy
            == ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET
            and ordinal == len(dates) - 1
        )

        open_nav = cash
        for code, shares in holdings.items():
            if code not in day.index:
                raise ValueError(
                    "held code missing from session panel; suspension/delisting "
                    f"rows must be explicit: {code} on {pd.Timestamp(date).date()}"
                )
            price = float(day.at[code, "open"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError(f"missing executable/mark price for held code {code}")
            open_nav += shares * price
        target_value = (
            open_nav / len(desired_codes) if desired_codes else 0.0
        )

        # Sell first. All positions were acquired on an earlier session because
        # buys occur only after this loop, so same-session sales are impossible.
        for code in (
            [] if final_close_mark_only else sorted(list(holdings))
        ):
            current = int(holdings.get(code, 0))
            row = day.loc[code] if code in day.index else None
            if row is None:
                blocked_sell_count += 1
                continue
            open_price = float(row["open"])
            lot = int(
                row.get("lot_size")
                if pd.notna(row.get("lot_size"))
                else execution_policy.default_lot_size
            )
            if lot <= 0:
                raise ValueError(f"invalid lot size for {code}: {lot}")
            target_shares = (
                int(target_value / open_price // lot * lot)
                if code in desired_set and open_price > 0
                else 0
            )
            sell_shares = max(0, current - target_shares)
            if sell_shares <= 0:
                continue
            if _blocked_sell(row, tick=float(execution_policy.price_tick)):
                blocked_sell_count += 1
                continue
            notional = sell_shares * open_price
            fee = fee_schedule.fee(notional, side="SELL")
            _, realized_trade_pnl, consumptions = _consume_fifo_lots(
                lots,
                code=str(code),
                shares=sell_shares,
                session_ordinal=ordinal,
                session_date=session_date,
                price=open_price,
                sell_fee_cny=fee,
                fill_reason="REBALANCE",
            )
            lot_consumptions.extend(consumptions)
            cumulative_realized_trade_pnl_cny += realized_trade_pnl
            session_realized_trade_pnl_cny += realized_trade_pnl
            cash += notional - fee
            total_fees += fee
            session_sell_notional_cny += notional
            session_sell_fees_cny += fee
            remaining = current - sell_shares
            if remaining:
                holdings[code] = remaining
            else:
                holdings.pop(code, None)
            fills.append(
                {
                    "date": pd.Timestamp(date).date().isoformat(),
                    "code": code,
                    "side": "SELL",
                    "shares": sell_shares,
                    "price": open_price,
                    "notional": notional,
                    "fee": fee,
                    "fill_reason": "REBALANCE",
                }
            )

        buy_orders: list[tuple[str, int, float, int]] = []
        for code in ([] if final_close_mark_only else desired_codes):
            if code not in day.index:
                blocked_buy_count += 1
                continue
            row = day.loc[code]
            if not bool(row["promotion_universe_eligible"]):
                blocked_buy_count += 1
                continue
            if _blocked_buy(row, tick=float(execution_policy.price_tick)):
                blocked_buy_count += 1
                continue
            open_price = float(row["open"])
            lot = int(
                row.get("lot_size")
                if pd.notna(row.get("lot_size"))
                else execution_policy.default_lot_size
            )
            if lot <= 0:
                raise ValueError(f"invalid lot size for {code}: {lot}")
            target_shares = int(target_value / open_price // lot * lot)
            buy_shares = max(0, target_shares - int(holdings.get(code, 0)))
            if buy_shares > 0:
                buy_orders.append((code, buy_shares, open_price, lot))

        required_cash = sum(
            shares * price
            + fee_schedule.fee(shares * price, side="BUY")
            for _, shares, price, _ in buy_orders
        )
        scale = min(1.0, cash / required_cash) if required_cash > 0 else 0.0
        for code, raw_shares, open_price, lot in buy_orders:
            shares = int(raw_shares * scale // lot * lot)
            while shares > 0:
                notional = shares * open_price
                fee = fee_schedule.fee(notional, side="BUY")
                if notional + fee <= cash + 1e-9:
                    break
                shares -= lot
            if shares <= 0:
                continue
            notional = shares * open_price
            fee = fee_schedule.fee(notional, side="BUY")
            cash -= notional + fee
            total_fees += fee
            session_buy_notional_cny += notional
            session_buy_fees_cny += fee
            holdings[code] = int(holdings.get(code, 0)) + shares
            if ordinal + 1 >= len(dates):
                raise RuntimeError("final session must not create a new lot")
            lot_sequence += 1
            lots.append(
                {
                    "lot_id": f"lot-{lot_sequence:08d}",
                    "code": str(code),
                    "acquired_date": session_date,
                    "acquired_session_ordinal": int(ordinal),
                    "sellable_date": pd.Timestamp(
                        dates[ordinal + 1]
                    ).date().isoformat(),
                    "sellable_session_ordinal": int(ordinal + 1),
                    "acquisition_price": float(open_price),
                    "original_shares": int(shares),
                    "remaining_shares": int(shares),
                    "disposed_shares": 0,
                    "buy_fee_cny": float(fee),
                    "original_cost_basis_cny": float(notional + fee),
                    "remaining_cost_basis_cny": float(notional + fee),
                    "realized_cost_basis_cny": 0.0,
                    "corporate_action_share_delta": 0,
                }
            )
            fills.append(
                {
                    "date": pd.Timestamp(date).date().isoformat(),
                    "code": code,
                    "side": "BUY",
                    "shares": shares,
                    "price": open_price,
                    "notional": notional,
                    "fee": fee,
                    "fill_reason": "REBALANCE",
                }
            )

        close_nav = cash
        marked_holdings_market_value_cny = 0.0
        for code, shares in holdings.items():
            if code not in day.index:
                raise ValueError(
                    "held code missing from close panel; suspension/delisting "
                    f"rows must be explicit: {code} on {pd.Timestamp(date).date()}"
                )
            price = float(day.at[code, "close"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError(f"missing close mark for held code {code}")
            market_value = shares * price
            marked_holdings_market_value_cny += market_value
            close_nav += market_value
        active_lots = _active_lots(lots)
        lot_codes = {str(lot["code"]) for lot in active_lots}
        quantity_errors = {
            code: int(holdings.get(code, 0))
            - _lot_quantity(lots, code=code)
            for code in set(holdings) | lot_codes
        }
        lot_quantity_error = max(
            [abs(error) for error in quantity_errors.values()] or [0]
        )
        maximum_lot_quantity_error = max(
            maximum_lot_quantity_error,
            int(lot_quantity_error),
        )
        for lot in lots:
            lot_balance = (
                int(lot["original_shares"])
                + int(lot["corporate_action_share_delta"])
                - int(lot["disposed_shares"])
                - int(lot["remaining_shares"])
            )
            if lot_balance != 0:
                raise RuntimeError(
                    f"lot share conservation failed: {lot['lot_id']}"
                )
        remaining_cost_basis_cny = sum(
            float(lot["remaining_cost_basis_cny"])
            for lot in active_lots
        )
        unrealized_pnl_cny = (
            marked_holdings_market_value_cny - remaining_cost_basis_cny
        )
        net_pnl_cny = close_nav - float(execution_policy.initial_cash_cny)
        pnl_identity_error_cny = (
            cumulative_realized_trade_pnl_cny
            + corporate_action_cash_cny
            + unrealized_pnl_cny
            - net_pnl_cny
        )
        cash_identity_error_cny = cash - (
            opening_cash_cny
            + session_corporate_action_cash_cny
            + session_sell_notional_cny
            - session_sell_fees_cny
            - session_buy_notional_cny
            - session_buy_fees_cny
        )
        nav_identity_error_cny = close_nav - (
            cash + marked_holdings_market_value_cny
        )
        maximum_cash_identity_error_cny = max(
            maximum_cash_identity_error_cny,
            abs(cash_identity_error_cny),
        )
        maximum_nav_identity_error_cny = max(
            maximum_nav_identity_error_cny,
            abs(nav_identity_error_cny),
        )
        maximum_pnl_identity_error_cny = max(
            maximum_pnl_identity_error_cny,
            abs(pnl_identity_error_cny),
        )
        tolerance_cny = _ledger_tolerance(close_nav)
        if abs(cash_identity_error_cny) > tolerance_cny:
            raise RuntimeError(
                f"cash ledger did not reconcile on {session_date}"
            )
        if abs(nav_identity_error_cny) > tolerance_cny:
            raise RuntimeError(
                f"NAV ledger did not reconcile on {session_date}"
            )
        if abs(pnl_identity_error_cny) > tolerance_cny:
            raise RuntimeError(
                f"PnL ledger did not reconcile on {session_date}"
            )
        if lot_quantity_error != 0:
            raise RuntimeError(
                f"lot quantity ledger did not reconcile on {session_date}"
            )
        if cash < -tolerance_cny:
            raise RuntimeError(f"negative trading cash on {session_date}")
        frozen_shares = sum(
            int(lot["remaining_shares"])
            for lot in active_lots
            if int(lot["sellable_session_ordinal"]) > ordinal
        )
        sellable_shares = sum(
            int(lot["remaining_shares"])
            for lot in active_lots
            if int(lot["sellable_session_ordinal"]) <= ordinal
        )
        share_weighted_age_numerator = sum(
            int(lot["remaining_shares"])
            * (ordinal - int(lot["acquired_session_ordinal"]))
            for lot in active_lots
        )
        active_share_count = sum(
            int(lot["remaining_shares"]) for lot in active_lots
        )
        maximum_position_age_sessions = max(
            [
                ordinal - int(lot["acquired_session_ordinal"])
                for lot in active_lots
            ]
            or [0]
        )
        daily_ledger_rows.append(
            {
                "date": session_date,
                "session_ordinal": int(ordinal),
                "opening_trading_cash_cny": float(opening_cash_cny),
                "corporate_action_cash_cny": float(
                    session_corporate_action_cash_cny
                ),
                "sell_notional_cny": float(session_sell_notional_cny),
                "sell_fees_cny": float(session_sell_fees_cny),
                "buy_notional_cny": float(session_buy_notional_cny),
                "buy_fees_cny": float(session_buy_fees_cny),
                "closing_trading_cash_cny": float(cash),
                "marked_holdings_market_value_cny": float(
                    marked_holdings_market_value_cny
                ),
                "nav_cny": float(close_nav),
                "session_realized_trade_pnl_cny": float(
                    session_realized_trade_pnl_cny
                ),
                "cumulative_realized_trade_pnl_cny": float(
                    cumulative_realized_trade_pnl_cny
                ),
                "cumulative_corporate_action_cash_pnl_cny": float(
                    corporate_action_cash_cny
                ),
                "remaining_cost_basis_cny": float(
                    remaining_cost_basis_cny
                ),
                "unrealized_pnl_cny": float(unrealized_pnl_cny),
                "net_pnl_cny": float(net_pnl_cny),
                "active_lot_count": int(len(active_lots)),
                "sellable_share_count": int(sellable_shares),
                "frozen_share_count": int(frozen_shares),
                "share_weighted_average_position_age_sessions": (
                    float(share_weighted_age_numerator / active_share_count)
                    if active_share_count > 0
                    else None
                ),
                "maximum_position_age_sessions": int(
                    maximum_position_age_sessions
                ),
                "cash_identity_error_cny": float(
                    cash_identity_error_cny
                ),
                "nav_identity_error_cny": float(nav_identity_error_cny),
                "pnl_identity_error_cny": float(pnl_identity_error_cny),
                "lot_quantity_error": int(lot_quantity_error),
                "reconciliation_tolerance_cny": float(tolerance_cny),
            }
        )
        daily_return = close_nav / previous_nav - 1.0
        daily_rows.append(
            {
                "date": pd.Timestamp(date).date().isoformat(),
                "nav": close_nav,
                "daily_net_return": daily_return,
                "cash": cash,
                "holding_count": len(holdings),
                "desired_count": len(desired_codes),
            }
        )
        previous_nav = close_nav

    if (
        ending_book_policy == ENDING_BOOK_REQUIRE_FLAT_FINAL_OPEN
        and corporate_action_policy.require_flat_at_replay_end
        and holdings
    ):
        raise AShareTerminalLiquidationError(list(holdings))

    daily = pd.DataFrame(daily_rows)
    daily_ledger = pd.DataFrame(daily_ledger_rows)
    # Keep empty ledgers serializable and schema-stable.  A no-fill candidate
    # must still emit an auditable zero-row Parquet artifact rather than an
    # implementation-dependent frame with no columns.
    lot_ledger = pd.DataFrame(lots, columns=LOT_LEDGER_COLUMNS)
    lot_consumption_ledger = pd.DataFrame(
        lot_consumptions,
        columns=LOT_CONSUMPTION_LEDGER_COLUMNS,
    )
    net_returns = pd.to_numeric(daily["daily_net_return"], errors="coerce")
    reward = _sortino(net_returns)
    if reward is None or not math.isfinite(reward):
        raise ValueError("A-share replay did not produce a finite net reward")
    fill_frame = pd.DataFrame(fills)
    traded_notional = (
        float(pd.to_numeric(fill_frame["notional"], errors="coerce").sum())
        if not fill_frame.empty
        else 0.0
    )
    average_nav = float(pd.to_numeric(daily["nav"], errors="coerce").mean())
    mean_one_way_turnover = (
        traded_notional / (2.0 * average_nav * max(1, len(daily) - 1))
        if average_nav > 0
        else None
    )
    final_day = by_date[pd.Timestamp(dates[-1])]
    ending_holdings = []
    ending_lots = []
    ending_holdings_market_value_cny = 0.0
    for code, shares in sorted(holdings.items()):
        mark_price = float(final_day.at[code, "close"])
        market_value = int(shares) * mark_price
        ending_holdings_market_value_cny += market_value
        ending_holdings.append(
            {
                "code": str(code),
                "shares": int(shares),
                "final_pit_close": mark_price,
                "market_value_cny": market_value,
            }
        )
    final_ordinal = len(dates) - 1
    for lot in _active_lots(lots):
        code = str(lot["code"])
        mark_price = float(final_day.at[code, "close"])
        remaining_shares = int(lot["remaining_shares"])
        market_value = remaining_shares * mark_price
        remaining_cost_basis = float(lot["remaining_cost_basis_cny"])
        ending_lots.append(
            {
                "lot_id": str(lot["lot_id"]),
                "code": code,
                "acquired_date": str(lot["acquired_date"]),
                "sellable_date": str(lot["sellable_date"]),
                "remaining_shares": remaining_shares,
                "remaining_cost_basis_cny": remaining_cost_basis,
                "final_pit_close": mark_price,
                "market_value_cny": market_value,
                "unrealized_pnl_cny": market_value - remaining_cost_basis,
                "position_age_sessions": int(
                    final_ordinal - int(lot["acquired_session_ordinal"])
                ),
                "sellable_at_final_session": bool(
                    int(lot["sellable_session_ordinal"]) <= final_ordinal
                ),
            }
        )
    ending_nav_cny = float(daily.iloc[-1]["nav"])
    ending_unrealized_pnl_cny = float(
        daily_ledger.iloc[-1]["unrealized_pnl_cny"]
    )
    cumulative_net_pnl_cny = ending_nav_cny - float(
        execution_policy.initial_cash_cny
    )
    position_ages = pd.to_numeric(
        daily_ledger["share_weighted_average_position_age_sessions"],
        errors="coerce",
    ).dropna()
    return {
        "replay_kernel_version": REPLAY_KERNEL_VERSION,
        "execution_policy": asdict(execution_policy),
        "execution_policy_sha256": execution_policy.payload_sha256,
        "corporate_action_policy": asdict(corporate_action_policy),
        "corporate_action_policy_sha256": (
            corporate_action_policy.payload_sha256
        ),
        "fee_schedule": asdict(fee_schedule),
        "fee_schedule_sha256": fee_schedule.payload_sha256,
        "universe_policy": {
            **asdict(universe_policy),
            "allowed_exchanges": list(universe_policy.allowed_exchanges),
        },
        "universe_policy_sha256": universe_policy.payload_sha256,
        "a_share_executable_net_reward": float(reward),
        "accounting_ledger_version": ACCOUNTING_LEDGER_VERSION,
        "accounting_ledger_contract": {
            "lot_disposal_method": "FIFO",
            "sellability_clock": "NEXT_TRADING_SESSION_OPEN",
            "buy_fee_accounting": "CAPITALIZED_IN_LOT_COST_BASIS",
            "sell_fee_accounting": "DEDUCTED_AT_REALIZATION",
            "corporate_action_cash_accounting": "SEPARATE_PNL_COMPONENT",
            "corporate_action_share_accounting": (
                "QUANTITY_ADJUSTMENT_TOTAL_COST_BASIS_UNCHANGED"
            ),
            "cash_scope": "TRADING_AVAILABLE_CASH_ONLY",
            "withdrawable_cash": "NOT_MODELED",
            "mark_source": "SESSION_FINAL_PIT_CLOSE",
        },
        "ending_book_policy": ending_book_policy,
        "final_close_mark_to_market_diagnostic": (
            ending_book_policy
            == ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET
        ),
        "daily_observation_count": int(len(daily)),
        "trade_count": int(len(fill_frame)),
        "fill_count": int(len(fill_frame)),
        "buy_fill_count": int(
            (fill_frame.get("side", pd.Series(dtype=str)) == "BUY").sum()
        ),
        "sell_fill_count": int(
            (fill_frame.get("side", pd.Series(dtype=str)) == "SELL").sum()
        ),
        "blocked_buy_count": int(blocked_buy_count),
        "blocked_sell_count": int(blocked_sell_count),
        "total_fees_cny": float(total_fees),
        "corporate_action_cash_cny": float(corporate_action_cash_cny),
        "corporate_action_share_delta": int(corporate_action_share_delta),
        "terminal_liquidation_count": int(terminal_liquidation_count),
        "traded_notional_cny": traded_notional,
        "a_share_mean_one_way_turnover": mean_one_way_turnover,
        "ending_nav_cny": ending_nav_cny,
        "ending_cash_cny": float(cash),
        "trading_available_cash_cny": float(cash),
        "ending_holding_count": int(len(holdings)),
        "ending_holdings": ending_holdings,
        "ending_lots": ending_lots,
        "ending_holdings_market_value_cny": float(
            ending_holdings_market_value_cny
        ),
        "ending_holdings_weight": (
            float(ending_holdings_market_value_cny / ending_nav_cny)
            if ending_nav_cny > 0
            else None
        ),
        "cumulative_net_pnl_cny": float(cumulative_net_pnl_cny),
        "cumulative_realized_trade_pnl_cny": float(
            cumulative_realized_trade_pnl_cny
        ),
        "cumulative_corporate_action_cash_pnl_cny": float(
            corporate_action_cash_cny
        ),
        "ending_unrealized_pnl_cny": ending_unrealized_pnl_cny,
        "share_weighted_average_position_age_sessions": (
            float(position_ages.mean()) if not position_ages.empty else None
        ),
        "maximum_position_age_sessions": int(
            pd.to_numeric(
                daily_ledger["maximum_position_age_sessions"],
                errors="coerce",
            ).max()
        ),
        "accounting_invariants": {
            "status": "PASS",
            "maximum_cash_identity_error_cny": float(
                maximum_cash_identity_error_cny
            ),
            "maximum_nav_identity_error_cny": float(
                maximum_nav_identity_error_cny
            ),
            "maximum_pnl_identity_error_cny": float(
                maximum_pnl_identity_error_cny
            ),
            "maximum_lot_quantity_error": int(
                maximum_lot_quantity_error
            ),
            "negative_cash_observed": False,
            "same_session_lot_sale_observed": False,
        },
        "daily": daily,
        "daily_accounting_ledger": daily_ledger,
        "fills": fill_frame,
        "lot_ledger": lot_ledger,
        "lot_consumption_ledger": lot_consumption_ledger,
        "proofs": {
            "execution_clock_enforced": True,
            "same_bar_execution_excluded": True,
            "t_plus_one_enforced": True,
            "limit_lock_fill_enforced": True,
            "suspension_fill_enforced": True,
            "full_fee_schedule_enforced": True,
            "promotion_grade_universe_enforced": True,
            "corporate_action_cash_share_enforced": True,
            "terminal_liquidation_enforced": (
                ending_book_policy
                == ENDING_BOOK_REQUIRE_FLAT_FINAL_OPEN
            ),
            **(
                {"final_close_mark_to_market_enforced": True}
                if ending_book_policy
                == ENDING_BOOK_FINAL_CLOSE_MARK_TO_MARKET
                else {}
            ),
        },
    }
