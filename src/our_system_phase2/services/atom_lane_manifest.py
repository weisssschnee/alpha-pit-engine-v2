"""True-1min atom and lane manifest.

This module is the single source for deciding how fields may enter the
true-1min formula generator. It separates ordinary minute fields from event
state fields and lagged/coverage-sensitive sidecars before candidate
materialization.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any, Iterable


EPS = "0.000001"
FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")


DIRECT_MINUTE_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vol",
    "amount",
    "amount_yuan",
    "vwap",
    "pct_chg",
    "amplitude_pct",
    "change",
    "pre_close",
    "ret",
    "ret_1m",
    "intraday_ret_from_open",
    "turnover_rate",
}

FIRSTN_PREFIXES = (
    "m1_first5",
    "m1_first10",
    "m1_first15",
    "m1_first20",
    "m1_first30",
)

EVENT_STATE_FIELDS = {
    "limit_up_event",
    "limit_down_event",
    "limit_up_streak",
    "limit_down_streak",
    "limit_up_break",
    "limit_down_repair",
    "limit_flip_up_to_down",
    "limit_flip_down_to_up",
    "high_board_rank",
    "is_market_high_board",
    "is_limit_up",
    "is_limit_down",
    "up_limit_keep_times",
    "up_limit_type",
    "lb_2_num",
    "lb_3_num",
    "max_lb_num",
    "zb_num",
    "mian_num",
    "tiandi_num",
    "ditian_num",
    "damian_num",
}

for _window in range(2, 16):
    EVENT_STATE_FIELDS.add(f"limit_up_any_close_not_open_in_t{_window}")
    EVENT_STATE_FIELDS.add(f"limit_up_any_open_not_close_in_t{_window}")
    EVENT_STATE_FIELDS.add(f"limit_up_close_count_t{_window}")
    EVENT_STATE_FIELDS.add(f"limit_up_open_count_t{_window}")
    EVENT_STATE_FIELDS.add(f"limit_up_touch_count_t{_window}")
    EVENT_STATE_FIELDS.add(f"limit_up_open_not_close_count_t{_window}")
    EVENT_STATE_FIELDS.add(f"limit_up_touch_not_close_count_t{_window}")


EVENT_CUTOFF_ONLY_FIELDS = {
    "up_limit_time",
    "update_time",
    "trade_time",
    "date",
    "exec_date",
    "signal_time",
    "dataset_route_id",
    "code",
    "symbol",
    "name",
}

AUCTION_EVENT_CONTEXT_FIELDS = {
    "auction_buy",
    "auction_money",
    "auction_offer",
    "auction_turnover",
    "auction_pre1max_ratio",
    "fd_close",
    "fd_max",
    "fengdan_rate",
    "seal_money",
    "seal_rate",
    "seal_circulation_rate",
}

LAGGED_CONTEXT_PATTERNS = (
    "ctx_",
    "fund_",
    "rzrq",
    "billboard",
    "holder",
    "share",
    "dividend",
    "market_cap",
    "float_market_cap",
    "float_share",
    "total_share",
    "pe",
    "pb",
    "ps",
    "volume_ratio",
    "turnover_ratio",
    "rzye",
    "rzyezb",
    "rqye",
    "rqyl",
    "plate_score",
    "money_leader",
)


@dataclass(frozen=True)
class AtomSpec:
    name: str
    lane: str
    expr: str
    side: str
    transform_mode: str
    role: str
    required_fields: tuple[str, ...]
    field_class: str
    note: str = ""

    def to_row(self) -> dict[str, Any]:
        return asdict(self)


def expression_fields(expression: str) -> tuple[str, ...]:
    return tuple(sorted(set(FIELD_RE.findall(expression or ""))))


def normalize_available_fields(fields: Iterable[str] | None) -> set[str] | None:
    if fields is None:
        return None
    return {str(field).strip().lstrip("$") for field in fields if str(field).strip()}


def field_lane(field: str) -> str:
    name = str(field or "").strip().lower().lstrip("$")
    if not name:
        return "unknown_review"
    if (
        name in EVENT_CUTOFF_ONLY_FIELDS
        or name.endswith("_bars")
        or name.endswith("cutoff_minute")
        or name.endswith("_time_minute")
        or any(token in name for token in ("next_open", "next_close", "next_return", "label", "future"))
    ):
        return "blocked_key_or_label"
    if name.startswith("ctx_"):
        return "lagged_context"
    if name in EVENT_STATE_FIELDS or any(token in name for token in ("limit", "uplimit", "open_board", "break_board", "high_board", "lb_")):
        return "event_state"
    if name in AUCTION_EVENT_CONTEXT_FIELDS or any(token in name for token in ("auction", "fengdan", "seal")):
        return "event_state"
    if name.startswith("m1_first"):
        return "direct_formula"
    if name in DIRECT_MINUTE_FIELDS:
        return "direct_formula"
    if any(pattern in name for pattern in LAGGED_CONTEXT_PATTERNS):
        return "lagged_context"
    if any(token in name for token in ("industry", "sector", "plate", "concept", "theme", "board_code")):
        return "membership_context"
    return "unknown_review"


def _field_available(field: str, available_fields: set[str] | None) -> bool:
    return available_fields is None or field in available_fields


def _add(rows: list[AtomSpec], atom: AtomSpec, available_fields: set[str] | None) -> None:
    if all(_field_available(field, available_fields) for field in atom.required_fields):
        rows.append(atom)


def build_search_atoms(available_fields: Iterable[str] | None = None) -> list[dict[str, Any]]:
    available = normalize_available_fields(available_fields)
    rows: list[AtomSpec] = []
    windows = (2, 3, 5, 8, 10, 15, 20, 30)
    pairs = ((2, 5), (3, 8), (5, 15), (8, 20), (10, 30))

    if all(_field_available(field, available) for field in ("open", "high", "low", "close")):
        range_norm = f"Div(Sub($high,$low),Add(Abs($open),{EPS}))"
        bar_loc = f"Div(Sub($close,$low),Add(Abs(Sub($high,$low)),{EPS}))"
        close_ret = f"Div(Sub($close,Delay($close,1)),Add(Abs(Delay($close,1)),{EPS}))"
        for window in windows:
            _add(
                rows,
                AtomSpec(
                    name=f"close_ret_delta_{window}",
                    lane="rx_intraday_return",
                    expr=f"Delta({close_ret},{window})",
                    side="event",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=("close",),
                    field_class="direct_formula",
                    note="close-derived return avoids requiring precomputed ret_1m",
                ),
                available,
            )
            _add(
                rows,
                AtomSpec(
                    name=f"range_vol_{window}",
                    lane="rx_range_location",
                    expr=f"Std({range_norm},{window})",
                    side="state",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=("open", "high", "low"),
                    field_class="direct_formula",
                ),
                available,
            )
            _add(
                rows,
                AtomSpec(
                    name=f"bar_loc_shift_{window}",
                    lane="rx_range_location",
                    expr=f"Sub({bar_loc},Mean({bar_loc},{window}))",
                    side="event",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=("open", "high", "low", "close"),
                    field_class="direct_formula",
                ),
                available,
            )

    for field, lane in (
        ("ret_1m", "rx_intraday_return"),
        ("intraday_ret_from_open", "rx_intraday_return"),
        ("pct_chg", "rx_intraday_return"),
        ("change", "rx_intraday_return"),
        ("amplitude_pct", "rx_range_location"),
    ):
        for window in windows:
            _add(
                rows,
                AtomSpec(
                    name=f"{field}_delta_{window}",
                    lane=lane,
                    expr=f"Delta(${field},{window})",
                    side="event",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=(field,),
                    field_class="direct_formula",
                ),
                available,
            )

    if all(_field_available(field, available) for field in ("close", "pre_close")):
        pre_close_gap = f"Div(Sub($close,$pre_close),Add(Abs($pre_close),{EPS}))"
        for window in windows:
            _add(
                rows,
                AtomSpec(
                    name=f"pre_close_gap_shift_{window}",
                    lane="rx_intraday_return",
                    expr=f"Sub({pre_close_gap},Mean({pre_close_gap},{window}))",
                    side="event",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=("close", "pre_close"),
                    field_class="direct_formula",
                    note="close vs pre-close minute state",
                ),
                available,
            )

    for field in ("amount", "amount_yuan", "volume", "vol", "vwap"):
        if not _field_available(field, available):
            continue
        canonical_lane = "rx_flow_amount_volume" if field != "vwap" else "rx_intraday_price_location"
        for window in windows:
            _add(
                rows,
                AtomSpec(
                    name=f"{field}_delta_{window}",
                    lane=canonical_lane,
                    expr=f"Delta(${field},{window})",
                    side="state",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=(field,),
                    field_class="direct_formula",
                ),
                available,
            )
        for short, long in pairs:
            _add(
                rows,
                AtomSpec(
                    name=f"{field}_curve_{short}_{long}",
                    lane=canonical_lane,
                    expr=f"Div(Mean(${field},{short}),Add(Abs(Mean(${field},{long})),{EPS}))",
                    side="state",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=(field,),
                    field_class="direct_formula",
                ),
                available,
            )

    if all(_field_available(field, available) for field in ("open", "vwap")):
        for window in windows:
            vwap_gap = f"Div(Sub($vwap,$open),Add(Abs($open),{EPS}))"
            _add(
                rows,
                AtomSpec(
                    name=f"vwap_open_gap_shift_{window}",
                    lane="rx_intraday_price_location",
                    expr=f"Sub({vwap_gap},Mean({vwap_gap},{window}))",
                    side="event",
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=("open", "vwap"),
                    field_class="direct_formula",
                    note="minute vwap vs open location",
                ),
                available,
            )

    for prefix in FIRSTN_PREFIXES:
        firstn_specs = (
            (f"{prefix}_amount", "rx_opening_amount", f"Div(${prefix}_amount,Add(Abs($amount),{EPS}))", ("amount", f"{prefix}_amount"), "event"),
            (f"{prefix}_vol", "rx_opening_amount", f"Div(${prefix}_vol,Add(Abs($volume),{EPS}))", ("volume", f"{prefix}_vol"), "state"),
            (f"{prefix}_range", "rx_opening_range", f"Div(${prefix}_range,Add(Abs($open),{EPS}))", ("open", f"{prefix}_range"), "event"),
            (f"{prefix}_vwap_open", "rx_opening_divergence", f"${prefix}_vwap_return_vs_open", (f"{prefix}_vwap_return_vs_open",), "event"),
            (f"{prefix}_actual_range", "rx_opening_range", f"Div(Sub(${prefix}_high,${prefix}_low),Add(Abs($open),{EPS}))", ("open", f"{prefix}_high", f"{prefix}_low"), "event"),
            (f"{prefix}_last_return_vs_open", "rx_opening_divergence", f"${prefix}_last_return_vs_open", (f"{prefix}_last_return_vs_open",), "event"),
            (f"{prefix}_vwap_gap_vs_open", "rx_opening_divergence", f"Div(Sub(${prefix}_vwap,$open),Add(Abs($open),{EPS}))", ("open", f"{prefix}_vwap"), "event"),
            (f"{prefix}_last_close_gap", "rx_opening_divergence", f"Div(Sub(${prefix}_last_close,$open),Add(Abs($open),{EPS}))", ("open", f"{prefix}_last_close"), "event"),
        )
        for name, lane, expr, required, side in firstn_specs:
            _add(
                rows,
                AtomSpec(
                    name=name,
                    lane=lane,
                    expr=expr,
                    side=side,
                    transform_mode="ordinary",
                    role="formula_search",
                    required_fields=tuple(required),
                    field_class="direct_formula",
                ),
                available,
            )

    event_candidates = set(EVENT_STATE_FIELDS | AUCTION_EVENT_CONTEXT_FIELDS)
    if available is not None:
        event_candidates |= {field for field in available if field_lane(field) == "event_state"}
    for field in sorted(event_candidates):
        if not _field_available(field, available):
            continue
        if field in {"high_board_rank", "up_limit_keep_times", "lb_2_num", "lb_3_num", "max_lb_num"} or field.endswith("_num"):
            primitives = (
                (f"{field}_state_dwell_5", f"StateDwell(${field},5)", "state_lifecycle"),
                (f"{field}_window_state_count_10", f"WindowStateCount(${field},10)", "state_lifecycle"),
            )
        elif field.endswith("_active"):
            primitives = (
                (f"{field}_event_count_5", f"EventCount(${field},5)", "event_state"),
                (f"{field}_event_age", f"EventAge(${field})", "event_state"),
            )
        elif field.endswith("_age_min"):
            primitives = (
                (f"{field}_state_dwell_5", f"StateDwell(${field},5)", "state_lifecycle"),
                (f"{field}_window_state_count_10", f"WindowStateCount(${field},10)", "state_lifecycle"),
            )
        else:
            primitives = (
                (f"{field}_state_dwell_5", f"StateDwell(${field},5)", "event_payload_state"),
                (f"{field}_window_state_count_10", f"WindowStateCount(${field},10)", "event_payload_state"),
            )
        for name, expr, lane in primitives:
            _add(
                rows,
                AtomSpec(
                    name=name,
                    lane=lane,
                    expr=expr,
                    side="event",
                    transform_mode="typed_rank",
                    role="event_state_search",
                    required_fields=(field,),
                    field_class="event_state",
                    note="event/state field must not enter ordinary continuous primitives",
                ),
                available,
            )

    context_candidates = {
        "turnover_ratio",
        "volume_ratio",
        "turnover_rate",
        "market_cap",
        "float_market_cap",
        "float_market_cap_yuan",
        "final_float_market_cap",
        "pe_ttm",
        "pb",
        "ps_ttm",
        "rzyezb",
        "rzye",
        "rqye",
        "billboard_net_amt",
        "billboard_buy_amt",
        "billboard_sell_amt",
        "holder_num",
    }
    if available is not None:
        context_candidates |= {field for field in available if field_lane(field) == "lagged_context"}
    for field in sorted(context_candidates):
        if not _field_available(field, available):
            continue
        _add(
            rows,
            AtomSpec(
                name=f"{field}_masked_zscore",
                lane="coverage_guarded_context",
                expr=f"MaskedZScore(${field},60,0.8)",
                side="state",
                transform_mode="typed_rank",
                role="lagged_context_search",
                required_fields=(field,),
                field_class="lagged_context",
                note="coverage-sensitive sidecar requires valid-ratio guard",
            ),
            available,
        )
        _add(
            rows,
            AtomSpec(
                name=f"{field}_valid_ratio",
                lane="coverage_guarded_context",
                expr=f"ValidRatioGate(${field},60,0.8)",
                side="state",
                transform_mode="typed_rank",
                role="lagged_context_search",
                required_fields=(field,),
                field_class="lagged_context",
                note="coverage-sensitive sidecar availability atom",
            ),
            available,
        )
        if _field_available("amount", available):
            _add(
                rows,
                AtomSpec(
                    name=f"{field}_safe_residual_amount",
                    lane="coverage_guarded_context",
                    expr=f"SafeCSResidual(${field},$amount,20,5,0.8)",
                    side="state",
                    transform_mode="typed_rank",
                    role="lagged_context_search",
                    required_fields=(field, "amount"),
                    field_class="lagged_context",
                    note="coverage-sensitive sidecar residualized only after valid cross-section checks",
                ),
                available,
            )

    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for atom in rows:
        if atom.expr in seen:
            continue
        seen.add(atom.expr)
        out.append(atom.to_row())
    return out


def atom_inventory_summary(available_fields: Iterable[str] | None = None) -> dict[str, Any]:
    available = normalize_available_fields(available_fields)
    atoms = build_search_atoms(available)
    by_role: dict[str, int] = {}
    by_class: dict[str, int] = {}
    by_lane: dict[str, int] = {}
    consumed_fields: set[str] = set()
    for atom in atoms:
        by_role[str(atom["role"])] = by_role.get(str(atom["role"]), 0) + 1
        by_class[str(atom["field_class"])] = by_class.get(str(atom["field_class"]), 0) + 1
        by_lane[str(atom["lane"])] = by_lane.get(str(atom["lane"]), 0) + 1
        consumed_fields.update(atom.get("required_fields") or ())
    field_routes = {}
    if available is not None:
        field_routes = {field: field_lane(field) for field in sorted(available)}
    return {
        "available_field_count": len(available) if available is not None else None,
        "atom_count": len(atoms),
        "by_role": dict(sorted(by_role.items())),
        "by_field_class": dict(sorted(by_class.items())),
        "by_lane": dict(sorted(by_lane.items())),
        "consumed_field_count": len(consumed_fields),
        "consumed_fields": sorted(consumed_fields),
        "field_routes": field_routes,
        "blocked_or_event_only_fields": sorted(
            field for field, route in field_routes.items() if route in {"blocked_key_or_label", "event_state", "membership_context"}
        ),
        "unconsumed_available_fields": sorted(set(field_routes) - consumed_fields) if available is not None else [],
    }
