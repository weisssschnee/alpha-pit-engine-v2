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


class AShareTerminalLiquidationError(ValueError):
    """A candidate-specific fail-closed terminal liquidity outcome."""

    def __init__(self, remaining_holdings: list[str]) -> None:
        self.remaining_holdings = tuple(sorted(str(code) for code in remaining_holdings))
        super().__init__(
            "final replay session could not liquidate every holding; "
            f"remaining={list(self.remaining_holdings)}"
        )


class AShareCorporateActionFractionalSharesError(ValueError):
    """Candidate-specific non-integer corporate-action holdings outcome."""

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
    last_close: dict[str, float] = {}
    fills: list[dict[str, Any]] = []
    daily_rows: list[dict[str, Any]] = []
    blocked_buy_count = 0
    blocked_sell_count = 0
    total_fees = 0.0
    corporate_action_cash_cny = 0.0
    corporate_action_share_delta = 0
    terminal_liquidation_count = 0

    previous_nav = float(execution_policy.initial_cash_cny)
    for ordinal, date in enumerate(dates):
        day = by_date[pd.Timestamp(date)]

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

        # Delisting terminal rows are explicit cash exits, not disappearing
        # panel rows. They occur before the ordinary session rebalance.
        for code in sorted(list(holdings)):
            row = day.loc[code]
            if not bool(row["is_terminal_session"]):
                continue
            shares = int(holdings.pop(code))
            price = float(row["terminal_liquidation_price"])
            notional = shares * price
            fee = fee_schedule.fee(notional, side="SELL")
            cash += notional - fee
            total_fees += fee
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
            cash += notional - fee
            total_fees += fee
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
            holdings[code] = int(holdings.get(code, 0)) + shares
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
        for code, shares in holdings.items():
            if code not in day.index:
                raise ValueError(
                    "held code missing from close panel; suspension/delisting "
                    f"rows must be explicit: {code} on {pd.Timestamp(date).date()}"
                )
            price = float(day.at[code, "close"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError(f"missing close mark for held code {code}")
            close_nav += shares * price
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
    ending_nav_cny = float(daily.iloc[-1]["nav"])
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
        "ending_holding_count": int(len(holdings)),
        "ending_holdings": ending_holdings,
        "ending_holdings_market_value_cny": float(
            ending_holdings_market_value_cny
        ),
        "ending_holdings_weight": (
            float(ending_holdings_market_value_cny / ending_nav_cny)
            if ending_nav_cny > 0
            else None
        ),
        "daily": daily,
        "fills": fill_frame,
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
