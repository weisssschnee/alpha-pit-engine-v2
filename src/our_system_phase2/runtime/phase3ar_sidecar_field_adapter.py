"""Attach Phase3AR sidecar fields to the true 1min Phase3AQ panel.

This module recovers Phase3AQ blocked formulas only when their fields have a
clear PIT or cutoff contract. It does not promote candidates and does not
change X0/R3. The output packs remain canary/search-prep inputs.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.real_market_validation import evaluate_panel_expression


REPO = Path(__file__).resolve().parents[3]
DATA_ROOT = Path(r"G:\Project_V7_Rotation\data\cn_public_enrichment")
DEFAULT_CANARY_PANEL = Path(
    "runtime/phase3aq_true_1min_formula_adapter_20260610/canary/"
    "phase3aq_true_1min_formula_canary.parquet"
)
DEFAULT_BLOCKED_ROWS = Path(
    "runtime/phase3aq_formula_packs_sanitized_20260610/"
    "phase3aq_blocked_or_sidecar_required_formula_rows.json"
)
DEFAULT_AQ_CONTRACT = Path("runtime/phase3aq_true_1min_formula_adapter_20260610/phase3aq_true_1min_field_contract.csv")
DEFAULT_CONTEXT_ROOT = Path("runtime/nonminute_context_panels/cn_nonminute_pit_context_panel_v1_20260602")
DEFAULT_EVENT_PANEL = Path(
    "runtime/minute_feature_panels/cn_minute_limit_event_alignment_v2_20260602/"
    "cn_minute_limit_event_alignment_v1.parquet"
)
DEFAULT_EVENT_CONTRACT = Path(
    "runtime/minute_feature_panels/cn_minute_limit_event_alignment_v2_20260602/"
    "cn_minute_limit_event_alignment_contract.csv"
)
DEFAULT_EVENT_DERIVED_PANEL = Path("runtime/derived_features/cn_event_daily_features_v1_20260531.parquet")
DEFAULT_HFQ_ROOT = DATA_ROOT / "cn_local_minute_daily_silver_v1_20260531" / "hfq_daily_2024_2025"
DEFAULT_HFQ_2026 = DATA_ROOT / "cn_local_minute_daily_silver_v1_20260531" / "hfq_daily_2026" / "hfq_daily_2026.parquet"
DEFAULT_FULLA_ROOT = DATA_ROOT / "cn_fundamental_akshare_fullA_partitioned_pit_v1_20260603" / "silver_partitioned"
DEFAULT_SENTIMENT_ROOT = DATA_ROOT / "cn_zzshare_limit_sentiment_pack_v1_20260602" / "silver_parquet"
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3ar_sidecar_field_adapter_20260610")
DEFAULT_REPORT_ROOT = Path("reports/phase3ar_sidecar_field_adapter_20260610")

FIELD_RE = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)")
EVENT_TIME_RE = re.compile(r"_(?:by|at)_(\d{4})$", re.IGNORECASE)
KEY_FIELDS = {"code", "date", "exec_date", "signal_date", "trade_time"}
FORBIDDEN_FIELDS = {"daily_ret"}
CURRENT_DAY_AGGREGATE_FIELDS = {"m1_amount_day", "m1_vol_day", "m1_bars_day", "m1_day_close", "m1_day_high", "m1_day_low"}
UPLIMIT_EVENT_NO_CUTOFF_FIELDS = {
    "evt_uplimit_amount",
    "evt_uplimit_auction_buy",
    "evt_uplimit_auction_money",
    "evt_uplimit_auction_offer",
    "evt_uplimit_auction_pre1max_ratio",
    "evt_uplimit_auction_turnover",
    "evt_uplimit_fd_close",
    "evt_uplimit_fd_max",
}
UPLIMIT_EVENT_LAG_COMPAT_FIELDS = {
    "evt_uplimit_amount": "amount",
    "evt_uplimit_auction_buy": "auction_buy",
    "evt_uplimit_auction_money": "auction_money",
    "evt_uplimit_auction_offer": "auction_offer",
    "evt_uplimit_auction_pre1max_ratio": "auction_pre1max_ratio",
    "evt_uplimit_auction_turnover": "auction_turnover",
    "evt_uplimit_fd_close": "fd_close",
    "evt_uplimit_fd_max": "fd_max",
}
EVENT_DERIVED_LAG_FIELDS = {
    "high_board_rank",
    "is_market_high_board",
    *{f"limit_up_any_close_not_open_in_t{window}" for window in range(2, 11)},
    *{f"limit_up_any_open_not_close_in_t{window}" for window in range(2, 11)},
}
CAP_FIELD_ALIASES = {
    "final_total_market_cap": "market_cap_yuan",
    "final_float_market_cap": "float_market_cap_yuan",
    "float_share": "float_shares",
    "turnover_ratio": "turnover_ratio",
    "turnover_ratio_real": "turnover_ratio",
}
FULLA_DATASETS = {
    "bs": "balance_sheet_report_em",
    "ps": "profit_sheet_report_em",
    "cf": "cash_flow_sheet_report_em",
}
FULLA_RAW_ALIASES = {
    "TOTAL_OPERATE_INCOME": ["TOTAL_OPERATE_INCOME", "OPERATE_INCOME"],
    "TOTAL_OPERATE_COST": ["TOTAL_OPERATE_COST", "OPERATE_EXPENSE"],
    "OPERATE_COST": ["OPERATE_COST", "OTHER_BUSINESS_COST"],
    "MANAGE_EXPENSE": ["MANAGE_EXPENSE", "BUSINESS_MANAGE_EXPENSE"],
    "SALE_EXPENSE": ["SALE_EXPENSE", "BUSINESS_MANAGE_EXPENSE"],
    "RESEARCH_EXPENSE": ["RESEARCH_EXPENSE", "ME_RESEARCH_EXPENSE"],
    "FINANCE_EXPENSE": ["FINANCE_EXPENSE"],
    "TOTAL_CURRENT_ASSETS": ["TOTAL_CURRENT_ASSETS", "CURRENT_ASSET_BALANCE"],
    "TOTAL_CURRENT_LIAB": ["TOTAL_CURRENT_LIAB", "CURRENT_LIAB_BALANCE"],
    "TOTAL_EQUITY": ["TOTAL_EQUITY", "EQUITY_BALANCE"],
    "MONETARYFUNDS": ["MONETARYFUNDS", "MONETARY_FUNDS", "CASH_DEPOSIT_PBC"],
    "INVENTORY": ["INVENTORY"],
    "ASSET_IMPAIRMENT_INCOME": ["ASSET_IMPAIRMENT_INCOME", "ASSET_IMPAIRMENT_LOSS"],
    "CREDIT_IMPAIRMENT_INCOME": ["CREDIT_IMPAIRMENT_INCOME", "CREDIT_IMPAIRMENT_LOSS"],
}
FUND_DERIVED_SOURCE_FIELDS = {
    "ctx_fund_ps_operate_profit_margin": ("ps", ["OPERATE_PROFIT", "OPERATE_INCOME"]),
    "ctx_fund_ps_netprofit_margin": ("ps", ["NETPROFIT", "OPERATE_INCOME"]),
    "ctx_fund_ps_research_to_income": ("ps", ["RESEARCH_EXPENSE", "OPERATE_INCOME"]),
    "ctx_fund_bs_cash_to_assets": ("bs", ["MONETARYFUNDS", "TOTAL_ASSETS"]),
    "ctx_fund_bs_debt_to_assets": ("bs", ["TOTAL_LIABILITIES", "TOTAL_ASSETS"]),
    "ctx_fund_bs_goodwill_to_assets": ("bs", ["GOODWILL", "TOTAL_ASSETS"]),
    "ctx_fund_bs_inventory_to_assets": ("bs", ["INVENTORY", "TOTAL_ASSETS"]),
    "ctx_fund_cf_end_cce": ("cf", ["END_CCE"]),
    "ctx_fund_cf_netcash_finance": ("cf", ["NETCASH_FINANCE"]),
    "ctx_fund_cf_netcash_invest": ("cf", ["NETCASH_INVEST"]),
    "ctx_fund_cf_total_operate_inflow": ("cf", ["TOTAL_OPERATE_INFLOW"]),
    "ctx_fund_cf_total_operate_outflow": ("cf", ["TOTAL_OPERATE_OUTFLOW"]),
    "ctx_fund_cf_operate_cash_to_netprofit": ("cf", ["NETCASH_OPERATE", "NETPROFIT"]),
    "fund_cash_to_assets": ("bs", ["MONETARYFUNDS", "TOTAL_ASSETS"]),
    "fund_current_ratio": ("bs", ["TOTAL_CURRENT_ASSETS", "TOTAL_CURRENT_LIAB"]),
    "fund_debt_to_assets": ("bs", ["TOTAL_LIABILITIES", "TOTAL_ASSETS"]),
    "fund_netprofit_margin": ("ps", ["NETPROFIT", "OPERATE_INCOME"]),
}
CROSS_FUND_DERIVED_SOURCE_FIELDS = {
    "fund_ocf_to_assets": {
        "bs": {"__fund_ocf_to_assets_total_assets": ["TOTAL_ASSETS"]},
        "cf": {"__fund_ocf_to_assets_netcash_operate": ["NETCASH_OPERATE"]},
    },
}
OPEN_SENTIMENT_FIELDS = {
    "ctx_sent_uplimit_num": "uplimit_num",
    "ctx_sent_downlimit_num": "downlimit_num",
    "ctx_sent_max_lb_num": "max_lb_num",
    "ctx_sent_zb_num": "zb_num",
    "ctx_sent_damian_num": "damian_num",
    "ctx_sent_up_num": "up_num",
    "ctx_sent_down_num": "down_num",
    "ctx_sent_lb_2_num": "lb_2_num",
    "ctx_sent_lb_3_num": "lb_3_num",
    "ctx_sent_tiandi_num": "tiandi_num",
    "ctx_sent_ditian_num": "ditian_num",
    "ctx_sent_gt5_num": "gt5_num",
    "ctx_sent_second_lb_num": "second_lb_num",
    "ctx_sent_mian_num": "mian_num",
    "ctx_sent_fb_num": "fb_num",
    "ctx_sent_bigleg_num": "bigleg_num",
    "ctx_sent_uplimit_n_num": "uplimit_n_num",
    "ctx_sent_lt5_num": "lt5_num",
    "ctx_sent_lb_h_num": "lb_h_num",
}
ZLS_OPEN_SENTIMENT_FIELDS = {
    "ctx_zls_uplimit_num_lag1": "uplimit_num",
    "ctx_zls_zb_num_lag1": "zb_num",
    "ctx_zls_lb_2_num_lag1": "lb_2_num",
    "ctx_zls_lb_3_num_lag1": "lb_3_num",
    "ctx_zls_max_lb_num_lag1": "max_lb_num",
    "ctx_zls_downlimit_num_lag1": "downlimit_num",
    "ctx_zls_down_num_lag1": "down_num",
    "ctx_zls_up_num_lag1": "up_num",
    "ctx_zls_lb_h_num_lag1": "lb_h_num",
}
ZLS_HOT_DAY_FIELDS = {
    "ctx_zls_lbgd_lag1": "lbgd",
    "ctx_zls_strong_lag1": "strong",
    "ctx_zls_ztjs_lag1": "ztjs",
}
THS_HOT_FIELDS = {
    "ctx_ths_hot_rank": "rank",
    "ctx_ths_hot_rank_diff": "rank_diff",
    "ctx_ths_hot_last_pct": "last_pct",
    "ctx_ths_hot_circulation_value": "circulation_value",
    "ctx_ths_hot_last_price": "last_price",
    "ctx_zls_rank_lag1": "rank",
    "ctx_zls_rank_diff_lag1": "rank_diff",
    "ctx_zls_last_pct_lag1": "last_pct",
    "ctx_zls_circulation_value_lag1": "circulation_value",
}
ZLS_EVENT_FIELDS = {
    "ctx_zls_evt_amount_lag1": "amount",
    "ctx_zls_evt_fd_close_lag1": "fd_close",
    "ctx_zls_evt_auction_money_lag1": "auction_money",
    "ctx_zls_evt_auction_turnover_lag1": "auction_turnover",
    "ctx_zls_evt_fd_max_lag1": "fd_max",
    "ctx_zls_evt_up_limit_keep_times_lag1": "up_limit_keep_times",
    "ctx_zls_evt_auction_offer_lag1": "auction_offer",
}


def _normalize_cn_code(value: Any) -> str:
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL"}:
        return ""
    stem = text.split(".", 1)[0]
    digits = re.sub(r"\D", "", stem)
    if len(digits) >= 6:
        digits = digits[-6:]
        if digits.startswith(("0", "2", "3")):
            return f"{digits}.SZ"
        if digits.startswith(("4", "8", "9")):
            return f"{digits}.BJ"
        return f"{digits}.SH"
    return text


def _compact_cn_code(value: Any) -> str:
    code = _normalize_cn_code(value)
    if "." not in code:
        return code
    digits, suffix = code.split(".", 1)
    return f"{suffix}{digits}"


def _numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _schema_names(path: Path) -> set[str]:
    if not path.exists():
        return set()
    if path.is_dir():
        names: set[str] = set()
        for part in sorted(path.rglob("*.parquet")):
            names.update(pq.ParquetFile(part).schema_arrow.names)
        return names
    return set(pq.ParquetFile(path).schema_arrow.names)


def _fields(expression: str) -> list[str]:
    return sorted(set(FIELD_RE.findall(expression or "")))


def _contract_lookup(path: Path) -> dict[str, dict[str, str]]:
    out: dict[str, dict[str, str]] = {}
    for row in _read_csv(path):
        name = row.get("field_name") or row.get("field") or row.get("name")
        if name:
            out[name] = row
    return out


def _blocked_candidate_rows(path: Path) -> list[dict[str, Any]]:
    payload = _read_json(path)
    return [dict(row) for row in payload.get("candidate_rows") or [] if isinstance(row, dict)]


def _context_partitions(root: Path, years: set[int]) -> list[Path]:
    paths: list[Path] = []
    for year in sorted(years):
        part = root / f"year={year}" / "part.parquet"
        if part.exists():
            paths.append(part)
    return paths


def _canary_codes(canary_keys: pd.DataFrame) -> set[str]:
    return {code for code in canary_keys["code"].map(_normalize_cn_code).dropna().astype(str).unique() if code}


def _daily_keys(canary_keys: pd.DataFrame) -> pd.DataFrame:
    keys = canary_keys[["code", "exec_date"]].drop_duplicates().copy()
    keys["_exec_dt"] = pd.to_datetime(keys["exec_date"], errors="coerce")
    return keys


def _expand_daily_sidecar(canary_keys: pd.DataFrame, daily_sidecar: pd.DataFrame) -> pd.DataFrame:
    daily_sidecar = daily_sidecar.drop(columns=["_exec_dt", "source_date", "available_date"], errors="ignore")
    add_cols = [col for col in daily_sidecar.columns if col not in {"code", "exec_date"}]
    if not add_cols:
        return canary_keys.copy()
    daily_sidecar = daily_sidecar.drop_duplicates(subset=["code", "exec_date"], keep="last")
    before = len(canary_keys)
    out = canary_keys.merge(daily_sidecar[["code", "exec_date", *add_cols]], on=["code", "exec_date"], how="left")
    if len(out) != before:
        raise RuntimeError("daily sidecar expansion changed row count")
    return out


def _append_nan_fields(frame: pd.DataFrame, fields: list[str] | set[str]) -> pd.DataFrame:
    names = list(fields)
    if not names:
        return frame.copy()
    return pd.concat(
        [
            frame.reset_index(drop=True),
            pd.DataFrame(np.nan, index=range(len(frame)), columns=names),
        ],
        axis=1,
    )


def _load_context_sidecar(root: Path, fields: set[str], canary_keys: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return canary_keys.copy(), {"loaded_fields": [], "source_paths": []}
    years = set(pd.to_datetime(canary_keys["exec_date"], errors="coerce").dt.year.dropna().astype(int).unique())
    schema = _schema_names(root)
    wanted = ["date", "code", *sorted(field for field in fields if field in schema)]
    parts: list[pd.DataFrame] = []
    paths = _context_partitions(root, years)
    for path in paths:
        cols = [col for col in wanted if col in pq.ParquetFile(path).schema_arrow.names]
        if {"date", "code"}.issubset(cols):
            parts.append(pd.read_parquet(path, columns=cols))
    base = canary_keys.copy()
    if not parts:
        return base, {"loaded_fields": [], "source_paths": [str(path) for path in paths]}
    context = pd.concat(parts, ignore_index=True)
    context["exec_date"] = pd.to_datetime(context["date"], errors="coerce").dt.date.astype(str)
    context["code"] = context["code"].map(_normalize_cn_code)
    context = context[context["code"].isin(_canary_codes(canary_keys))]
    context = context.drop(columns=["date"], errors="ignore")
    daily = _daily_keys(base).merge(context, on=["exec_date", "code"], how="left")
    daily = daily.drop(columns=["_exec_dt"], errors="ignore").drop_duplicates(subset=["code", "exec_date"], keep="last")
    value_cols = [col for col in daily.columns if col not in {"code", "exec_date"}]
    for col in value_cols:
        daily[col] = pd.to_numeric(daily[col], errors="coerce").astype("float32")
    return daily, {
        "loaded_fields": [field for field in fields if field in context.columns],
        "source_paths": [str(path) for path in paths],
        "grain": "code_exec_date_daily_sidecar",
    }


def _load_event_sidecar(event_panel: Path, fields: set[str], canary_keys: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return canary_keys.copy(), {"loaded_fields": [], "source_path": str(event_panel)}
    schema = _schema_names(event_panel)
    wanted = ["exec_date", "code", *sorted(field for field in fields if field in schema)]
    if not {"exec_date", "code"}.issubset(wanted) or not event_panel.exists():
        return canary_keys.copy(), {"loaded_fields": [], "source_path": str(event_panel)}
    event = pd.read_parquet(event_panel, columns=[col for col in wanted if col in schema])
    event["exec_date"] = event["exec_date"].astype(str)
    event["code"] = event["code"].map(_normalize_cn_code)
    event = event[event["code"].isin(_canary_codes(canary_keys))]
    base = canary_keys.copy()
    daily = _daily_keys(base).merge(event, on=["exec_date", "code"], how="left")
    out = _expand_daily_sidecar(base, daily)
    if len(out) != len(base):
        raise RuntimeError("event sidecar join changed row count")
    hhmm = pd.to_datetime(out["trade_time"], errors="coerce").dt.strftime("%H%M")
    for field in fields:
        match = EVENT_TIME_RE.search(field)
        if field in out.columns and match:
            out.loc[hhmm < match.group(1), field] = np.nan
    return out, {"loaded_fields": [field for field in fields if field in out.columns], "source_path": str(event_panel)}


def _hfq_paths(root: Path, hfq_2026: Path, years: set[int]) -> list[Path]:
    paths: list[Path] = []
    for year in sorted(years | {min(years) - 1 if years else 0}):
        if year <= 0:
            continue
        part = root / f"year={year}" / "part.parquet"
        if part.exists():
            paths.append(part)
    if any(year >= 2026 for year in years) and hfq_2026.exists():
        paths.append(hfq_2026)
    return paths


def _load_cap_sidecar(
    hfq_root: Path,
    hfq_2026: Path,
    fields: set[str],
    canary_keys: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return canary_keys.copy(), {"loaded_fields": [], "source_paths": []}
    years = set(pd.to_datetime(canary_keys["exec_date"], errors="coerce").dt.year.dropna().astype(int).unique())
    paths = _hfq_paths(hfq_root, hfq_2026, years)
    raw_cols = sorted({CAP_FIELD_ALIASES[field] for field in fields if field in CAP_FIELD_ALIASES})
    parts: list[pd.DataFrame] = []
    for path in paths:
        schema = _schema_names(path)
        cols = [col for col in ["date", "code", *raw_cols] if col in schema]
        if {"date", "code"}.issubset(cols):
            parts.append(pd.read_parquet(path, columns=cols))
    base = canary_keys.copy()
    if not parts:
        return base, {"loaded_fields": [], "source_paths": [str(path) for path in paths]}
    daily = pd.concat(parts, ignore_index=True)
    daily["source_date"] = pd.to_datetime(daily["date"], errors="coerce")
    daily["code"] = daily["code"].map(_normalize_cn_code)
    daily = daily[daily["code"].isin(_canary_codes(canary_keys))]
    daily = daily.sort_values(["code", "source_date"])
    for out_name, raw_name in CAP_FIELD_ALIASES.items():
        if out_name in fields and raw_name in daily.columns:
            daily[out_name] = pd.to_numeric(daily[raw_name], errors="coerce")
    keep_cols = ["code", "source_date", *sorted(field for field in fields if field in daily.columns)]
    daily = daily[keep_cols].dropna(subset=["source_date", "code"])
    out_daily = _previous_dense_daily_calendar_to_daily(base, daily)
    out_daily = out_daily[["code", "exec_date", *sorted(field for field in fields if field in out_daily.columns)]]
    out_daily = out_daily.drop_duplicates(subset=["code", "exec_date"], keep="last")
    value_cols = [col for col in out_daily.columns if col not in {"code", "exec_date"}]
    for col in value_cols:
        out_daily[col] = pd.to_numeric(out_daily[col], errors="coerce").astype("float32")
    return out_daily, {
        "loaded_fields": [field for field in fields if field in out_daily.columns],
        "source_paths": [str(path) for path in paths],
        "grain": "code_exec_date_daily_sidecar",
    }


def _fulla_source_candidates(raw_name: str) -> list[str]:
    upper = raw_name.upper()
    candidates = [upper]
    candidates.extend(FULLA_RAW_ALIASES.get(upper, []))
    return list(dict.fromkeys(candidates))


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = pd.to_numeric(denominator, errors="coerce").replace(0, np.nan)
    return pd.to_numeric(numerator, errors="coerce") / denominator


def _compute_ctx_fund_field(df: pd.DataFrame, field: str) -> pd.Series | None:
    if field == "ctx_fund_ps_operate_profit_margin" and {"OPERATE_PROFIT", "OPERATE_INCOME"}.issubset(df.columns):
        return _safe_divide(df["OPERATE_PROFIT"], df["OPERATE_INCOME"])
    if field in {"ctx_fund_ps_netprofit_margin", "fund_netprofit_margin"} and {"NETPROFIT", "OPERATE_INCOME"}.issubset(df.columns):
        return _safe_divide(df["NETPROFIT"], df["OPERATE_INCOME"])
    if field == "ctx_fund_ps_research_to_income" and {"RESEARCH_EXPENSE", "OPERATE_INCOME"}.issubset(df.columns):
        return _safe_divide(df["RESEARCH_EXPENSE"], df["OPERATE_INCOME"])
    if field in {"ctx_fund_bs_cash_to_assets", "fund_cash_to_assets"} and {"MONETARYFUNDS", "TOTAL_ASSETS"}.issubset(df.columns):
        return _safe_divide(df["MONETARYFUNDS"], df["TOTAL_ASSETS"])
    if field in {"ctx_fund_bs_debt_to_assets", "fund_debt_to_assets"} and {"TOTAL_LIABILITIES", "TOTAL_ASSETS"}.issubset(df.columns):
        return _safe_divide(df["TOTAL_LIABILITIES"], df["TOTAL_ASSETS"])
    if field == "fund_current_ratio" and {"TOTAL_CURRENT_ASSETS", "TOTAL_CURRENT_LIAB"}.issubset(df.columns):
        return _safe_divide(df["TOTAL_CURRENT_ASSETS"], df["TOTAL_CURRENT_LIAB"])
    if field == "ctx_fund_bs_goodwill_to_assets" and {"GOODWILL", "TOTAL_ASSETS"}.issubset(df.columns):
        return _safe_divide(df["GOODWILL"], df["TOTAL_ASSETS"])
    if field == "ctx_fund_bs_inventory_to_assets" and {"INVENTORY", "TOTAL_ASSETS"}.issubset(df.columns):
        return _safe_divide(df["INVENTORY"], df["TOTAL_ASSETS"])
    if field == "ctx_fund_cf_operate_cash_to_netprofit" and {"NETCASH_OPERATE", "NETPROFIT"}.issubset(df.columns):
        return _safe_divide(df["NETCASH_OPERATE"], df["NETPROFIT"])
    passthrough = {
        "ctx_fund_cf_end_cce": "END_CCE",
        "ctx_fund_cf_netcash_finance": "NETCASH_FINANCE",
        "ctx_fund_cf_netcash_invest": "NETCASH_INVEST",
        "ctx_fund_cf_total_operate_inflow": "TOTAL_OPERATE_INFLOW",
        "ctx_fund_cf_total_operate_outflow": "TOTAL_OPERATE_OUTFLOW",
    }
    raw_col = passthrough.get(field)
    if raw_col and raw_col in df.columns:
        return _numeric(df[raw_col])
    return None


def _ctx_fund_source_frame(df: pd.DataFrame, field: str) -> pd.DataFrame:
    _, raw_names = FUND_DERIVED_SOURCE_FIELDS[field]
    out = pd.DataFrame(index=df.index)
    for raw_name in raw_names:
        source_col = next((candidate for candidate in _fulla_source_candidates(raw_name) if candidate in df.columns), "")
        if source_col:
            out[raw_name] = df[source_col]
    return out


def _load_fulla_sidecar(
    fulla_root: Path,
    fields: set[str],
    canary_keys: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return canary_keys.copy(), {"loaded_fields": [], "source_paths": [], "missing_fields": []}
    wanted: dict[str, dict[str, list[str]]] = {tag: {} for tag in FULLA_DATASETS}
    for field in sorted(fields):
        match = re.match(r"^ctx_fulla_(bs|ps|cf)_(.+)$", field)
        if match:
            tag, raw_name = match.group(1), match.group(2)
            wanted[tag][field] = _fulla_source_candidates(raw_name)
            continue
        if field in FUND_DERIVED_SOURCE_FIELDS:
            tag, raw_names = FUND_DERIVED_SOURCE_FIELDS[field]
            expanded: list[str] = []
            for raw_name in raw_names:
                expanded.extend(_fulla_source_candidates(raw_name))
            wanted[tag][field] = list(dict.fromkeys(expanded))
            continue
        if field in CROSS_FUND_DERIVED_SOURCE_FIELDS:
            for tag, dependency_map in CROSS_FUND_DERIVED_SOURCE_FIELDS[field].items():
                for internal_field, raw_names in dependency_map.items():
                    expanded = []
                    for raw_name in raw_names:
                        expanded.extend(_fulla_source_candidates(raw_name))
                    wanted[tag][internal_field] = list(dict.fromkeys(expanded))

    base = _daily_keys(canary_keys)
    codes = sorted(set(base["code"].map(_compact_cn_code)))
    loaded_fields: set[str] = set()
    missing_fields: set[str] = set(fields)
    source_paths: list[str] = []
    dataset_panels: list[pd.DataFrame] = []
    for tag, dataset in FULLA_DATASETS.items():
        if not wanted.get(tag):
            continue
        per_code_parts: list[pd.DataFrame] = []
        dataset_root = fulla_root / dataset
        for compact in codes:
            path = dataset_root / f"{compact}.parquet"
            if not path.exists():
                continue
            schema = _schema_names(path)
            raw_cols_by_field: dict[str, list[str]] = {}
            for field, candidates in wanted[tag].items():
                raw_cols = [candidate for candidate in candidates if candidate in schema]
                if raw_cols:
                    raw_cols_by_field[field] = raw_cols
            raw_cols_needed = sorted({raw_col for raw_cols in raw_cols_by_field.values() for raw_col in raw_cols})
            if not raw_cols_by_field:
                continue
            cols = [col for col in ["source_code6", "SECURITY_CODE", "NOTICE_DATE", "UPDATE_DATE", "REPORT_DATE", *raw_cols_needed] if col in schema]
            if "NOTICE_DATE" not in cols and "UPDATE_DATE" not in cols and "REPORT_DATE" not in cols:
                continue
            df = pd.read_parquet(path, columns=list(dict.fromkeys(cols)))
            code_source = df["source_code6"] if "source_code6" in df.columns else df.get("SECURITY_CODE", compact)
            df["code"] = pd.Series(code_source).map(_normalize_cn_code)
            available = pd.to_datetime(df.get("NOTICE_DATE"), errors="coerce")
            if available.isna().all() and "UPDATE_DATE" in df.columns:
                available = pd.to_datetime(df["UPDATE_DATE"], errors="coerce")
            if available.isna().all() and "REPORT_DATE" in df.columns:
                available = pd.to_datetime(df["REPORT_DATE"], errors="coerce") + pd.Timedelta(days=90)
            df["available_date"] = available
            value_cols: dict[str, pd.Series] = {}
            for field, raw_cols in raw_cols_by_field.items():
                if field.startswith("ctx_fund_"):
                    value = _compute_ctx_fund_field(_ctx_fund_source_frame(df, field), field)
                    if value is not None:
                        value_cols[field] = value
                else:
                    value_cols[field] = _numeric(df[raw_cols[0]])
            if value_cols:
                df = pd.concat([df, pd.DataFrame(value_cols, index=df.index)], axis=1)
            keep = ["code", "available_date", *value_cols.keys()]
            df = df[keep].dropna(subset=["code", "available_date"])
            if not df.empty:
                loaded_fields.update(value_cols.keys())
                missing_fields.difference_update(value_cols.keys())
                source_paths.append(str(path))
                per_code_parts.append(df)
        if not per_code_parts:
            continue
        daily = pd.concat(per_code_parts, ignore_index=True).sort_values(["code", "available_date"])
        keep_fields = sorted(field for field in wanted[tag] if field in daily.columns)
        code_chunks: list[pd.DataFrame] = []
        for code, left in base.sort_values("_exec_dt").groupby("code", sort=False):
            right = daily[daily["code"] == code].sort_values("available_date")
            if right.empty:
                chunk = _append_nan_fields(left, keep_fields)
            else:
                chunk = pd.merge_asof(
                    left.sort_values("_exec_dt"),
                    right.drop(columns=["code"]),
                    left_on="_exec_dt",
                    right_on="available_date",
                    direction="backward",
                    allow_exact_matches=False,
                )
            code_chunks.append(chunk[["code", "exec_date", *keep_fields]])
        dataset_panels.append(pd.concat(code_chunks, ignore_index=True))

    if not dataset_panels:
        return canary_keys.copy(), {"loaded_fields": [], "source_paths": source_paths, "missing_fields": sorted(missing_fields)}
    out_daily = base.drop(columns=["_exec_dt"], errors="ignore")
    for dataset_panel in dataset_panels:
        add_cols = [col for col in dataset_panel.columns if col not in {"code", "exec_date"} and col not in out_daily.columns]
        if add_cols:
            out_daily = out_daily.merge(dataset_panel[["code", "exec_date", *add_cols]], on=["code", "exec_date"], how="left")
    if "fund_ocf_to_assets" in fields:
        numerator = "__fund_ocf_to_assets_netcash_operate"
        denominator = "__fund_ocf_to_assets_total_assets"
        if {numerator, denominator}.issubset(out_daily.columns):
            out_daily["fund_ocf_to_assets"] = _safe_divide(out_daily[numerator], out_daily[denominator])
            loaded_fields.add("fund_ocf_to_assets")
            missing_fields.discard("fund_ocf_to_assets")
        out_daily = out_daily.drop(columns=[numerator, denominator], errors="ignore")
    value_cols = [col for col in out_daily.columns if col not in {"code", "exec_date"}]
    for col in value_cols:
        out_daily[col] = pd.to_numeric(out_daily[col], errors="coerce").astype("float32")
    return out_daily, {
        "loaded_fields": sorted(loaded_fields),
        "source_paths": sorted(set(source_paths)),
        "missing_fields": sorted(missing_fields),
        "pit_rule": "NOTICE_DATE previous available day; exact same exec_date is not used",
        "grain": "code_exec_date_daily_sidecar",
    }


def _load_daily_market_context(path: Path, date_col: str, field_map: dict[str, str]) -> pd.DataFrame:
    if not path.exists() or not field_map:
        return pd.DataFrame()
    schema = _schema_names(path)
    cols = [col for col in [date_col, *field_map.values()] if col in schema]
    if date_col not in cols:
        return pd.DataFrame()
    df = pd.read_parquet(path, columns=list(dict.fromkeys(cols)))
    df["source_date"] = pd.to_datetime(df[date_col], errors="coerce")
    for field, raw_col in field_map.items():
        if raw_col in df.columns:
            df[field] = _numeric(df[raw_col])
    keep = ["source_date", *[field for field in field_map if field in df.columns]]
    return df[keep].dropna(subset=["source_date"]).drop_duplicates(subset=["source_date"], keep="last")


def _load_stock_daily_context(path: Path, date_col: str, code_col: str, field_map: dict[str, str]) -> pd.DataFrame:
    if not path.exists() or not field_map:
        return pd.DataFrame()
    schema = _schema_names(path)
    cols = [col for col in [date_col, code_col, *field_map.values()] if col in schema]
    if date_col not in cols or code_col not in cols:
        return pd.DataFrame()
    df = pd.read_parquet(path, columns=list(dict.fromkeys(cols)))
    df["source_date"] = pd.to_datetime(df[date_col], errors="coerce")
    df["code"] = df[code_col].map(_normalize_cn_code)
    for field, raw_col in field_map.items():
        if raw_col in df.columns:
            df[field] = _numeric(df[raw_col])
    keep = ["code", "source_date", *[field for field in field_map if field in df.columns]]
    return df[keep].dropna(subset=["code", "source_date"]).drop_duplicates(subset=["code", "source_date"], keep="last")


def _merge_previous_daily(base: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return base.copy()
    out_daily = _merge_previous_daily_to_daily(base, daily)
    return _expand_daily_sidecar(base, out_daily)


def _previous_dense_daily_calendar_to_daily(base: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return _daily_keys(base).drop(columns=["_exec_dt"], errors="ignore")
    fields = [col for col in daily.columns if col not in {"code", "source_date"}]
    shifted = daily.sort_values(["code", "source_date"]).copy()
    shifted["_next_source_date"] = shifted.groupby("code", sort=False)["source_date"].shift(-1)
    shifted = shifted.dropna(subset=["code", "source_date", "_next_source_date"])
    shifted["exec_date"] = pd.to_datetime(shifted["_next_source_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    shifted = shifted.dropna(subset=["exec_date"])
    left = _daily_keys(base)[["code", "exec_date"]].drop_duplicates()
    out = left.merge(
        shifted[["code", "exec_date", *fields]].drop_duplicates(subset=["code", "exec_date"], keep="last"),
        on=["code", "exec_date"],
        how="left",
    )
    return out


def _load_event_derived_sidecar(
    event_derived_panel: Path,
    fields: set[str],
    canary_keys: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return canary_keys.copy(), {"loaded_fields": [], "source_paths": [], "missing_fields": []}
    if not event_derived_panel.exists():
        return canary_keys.copy(), {
            "loaded_fields": [],
            "source_paths": [],
            "missing_fields": sorted(fields),
            "missing_panel": str(event_derived_panel),
        }
    schema = _schema_names(event_derived_panel)
    wanted = sorted(field for field in fields if field in schema)
    missing = sorted(field for field in fields if field not in schema)
    if not wanted or "date" not in schema or "code" not in schema:
        return canary_keys.copy(), {
            "loaded_fields": [],
            "source_paths": [str(event_derived_panel)],
            "missing_fields": sorted(fields),
        }
    df = pd.read_parquet(event_derived_panel, columns=["date", "code", *wanted])
    df["source_date"] = pd.to_datetime(df["date"], errors="coerce")
    df["code"] = df["code"].map(_normalize_cn_code)
    for field in wanted:
        df[field] = pd.to_numeric(df[field], errors="coerce")
    daily = df[["code", "source_date", *wanted]].dropna(subset=["code", "source_date"]).drop_duplicates(
        subset=["code", "source_date"],
        keep="last",
    )
    out = _previous_dense_daily_calendar_to_daily(canary_keys, daily)
    return out, {
        "loaded_fields": wanted,
        "source_paths": [str(event_derived_panel)],
        "missing_fields": missing,
        "pit_rule": "event-derived fields are joined from previous available daily row only; same exec_date is not used",
        "grain": "code_exec_date_daily_sidecar",
    }


def _attach_minute_open_gap_fields(canary: pd.DataFrame, formula_fields: Counter[str]) -> pd.DataFrame:
    field = "m1_open_gap_vs_preclose"
    if field not in formula_fields or field in canary.columns:
        return canary
    required = {"code", "exec_date", "trade_time", "open", "close"}
    if not required.issubset(canary.columns):
        return canary
    daily = (
        canary.sort_values(["code", "exec_date", "trade_time"])
        .groupby(["code", "exec_date"], as_index=False)
        .agg(m1_open=("open", "first"), m1_day_close=("close", "last"))
        .sort_values(["code", "exec_date"])
    )
    daily["m1_pre_close"] = daily.groupby("code", sort=False)["m1_day_close"].shift(1)
    denominator = pd.to_numeric(daily["m1_pre_close"], errors="coerce").replace(0, np.nan)
    daily[field] = pd.to_numeric(daily["m1_open"], errors="coerce") / denominator - 1.0
    return canary.merge(daily[["code", "exec_date", field]], on=["code", "exec_date"], how="left")


def _merge_previous_daily_to_daily(base: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    if daily.empty:
        return _daily_keys(base).drop(columns=["_exec_dt"], errors="ignore")
    out_chunks: list[pd.DataFrame] = []
    left_base = _daily_keys(base)
    fields = [col for col in daily.columns if col not in {"code", "source_date"}]
    if "code" in daily.columns:
        for code, left in left_base.sort_values("_exec_dt").groupby("code", sort=False):
            right = daily[daily["code"] == code].sort_values("source_date")
            if right.empty:
                chunk = _append_nan_fields(left, fields)
            else:
                chunk = pd.merge_asof(
                    left.sort_values("_exec_dt"),
                    right.drop(columns=["code"]),
                    left_on="_exec_dt",
                    right_on="source_date",
                    direction="backward",
                    allow_exact_matches=False,
                )
            out_chunks.append(chunk)
    else:
        right = daily.sort_values("source_date")
        for _, left in left_base.sort_values("_exec_dt").groupby("code", sort=False):
            out_chunks.append(
                pd.merge_asof(
                    left.sort_values("_exec_dt"),
                    right,
                    left_on="_exec_dt",
                    right_on="source_date",
                    direction="backward",
                    allow_exact_matches=False,
                )
            )
    out_daily = pd.concat(out_chunks, ignore_index=True)
    return out_daily[["code", "exec_date", *fields]].drop_duplicates(subset=["code", "exec_date"], keep="last")


def _load_sentiment_sidecar(
    sentiment_root: Path,
    fields: set[str],
    canary_keys: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not fields:
        return canary_keys.copy(), {"loaded_fields": [], "source_paths": []}
    base = canary_keys.copy()
    panels: list[pd.DataFrame] = []
    source_paths: list[str] = []
    loaded_fields: set[str] = set()

    maps = [
        ("open_sentiment_data.parquet", "date1", {field: raw for field, raw in OPEN_SENTIMENT_FIELDS.items() if field in fields}),
        ("open_sentiment_data.parquet", "date1", {field: raw for field, raw in ZLS_OPEN_SENTIMENT_FIELDS.items() if field in fields}),
        ("sentiment_hot_day.parquet", "Day", {field: raw for field, raw in ZLS_HOT_DAY_FIELDS.items() if field in fields}),
    ]
    for filename, date_col, field_map in maps:
        panel = _load_daily_market_context(sentiment_root / filename, date_col, field_map)
        if not panel.empty:
            panels.append(panel)
            loaded_fields.update([field for field in field_map if field in panel.columns])
            source_paths.append(str(sentiment_root / filename))

    ths_map = {field: raw for field, raw in THS_HOT_FIELDS.items() if field in fields}
    ths = _load_stock_daily_context(sentiment_root / "ths_hot_top.parquet", "collect_date", "symbol_code", ths_map)
    if not ths.empty:
        panels.append(ths)
        loaded_fields.update([field for field in ths_map if field in ths.columns])
        source_paths.append(str(sentiment_root / "ths_hot_top.parquet"))

    zls_evt_map = {field: raw for field, raw in ZLS_EVENT_FIELDS.items() if field in fields}
    zls_evt = _load_stock_daily_context(sentiment_root / "uplimit_stocks.parquet", "date1", "stock_code", zls_evt_map)
    if not zls_evt.empty:
        panels.append(zls_evt)
        loaded_fields.update([field for field in zls_evt_map if field in zls_evt.columns])
        source_paths.append(str(sentiment_root / "uplimit_stocks.parquet"))

    uplimit_lag_map = {field: raw for field, raw in UPLIMIT_EVENT_LAG_COMPAT_FIELDS.items() if field in fields}
    uplimit_lag = _load_stock_daily_context(sentiment_root / "uplimit_stocks.parquet", "date1", "stock_code", uplimit_lag_map)
    if not uplimit_lag.empty:
        panels.append(uplimit_lag)
        loaded_fields.update([field for field in uplimit_lag_map if field in uplimit_lag.columns])
        source_paths.append(str(sentiment_root / "uplimit_stocks.parquet"))

    out = _daily_keys(base).drop(columns=["_exec_dt"], errors="ignore")
    for panel in panels:
        next_out = _merge_previous_daily_to_daily(base, panel)
        add_cols = [col for col in next_out.columns if col not in {"code", "exec_date"} and col not in out.columns]
        if add_cols:
            out = out.merge(next_out[["code", "exec_date", *add_cols]], on=["code", "exec_date"], how="left")
    value_cols = [col for col in out.columns if col not in {"code", "exec_date"}]
    for col in value_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce").astype("float32")
    return out, {
        "loaded_fields": sorted(loaded_fields),
        "source_paths": sorted(set(source_paths)),
        "lag_rule": "previous available daily row; exact same exec_date is not used",
        "grain": "code_exec_date_daily_sidecar",
    }


def _classify_formula(
    fields: list[str],
    *,
    available_fields: set[str],
    safe_context_fields: set[str],
    diagnostic_context_fields: set[str],
    event_fields: set[str],
    cap_fields: set[str],
    fulla_fields: set[str],
    sentiment_fields: set[str],
) -> tuple[str, list[str]]:
    blockers: list[str] = []
    has_event = False
    has_diagnostic = False
    for field in fields:
        if field in KEY_FIELDS:
            blockers.append(f"key_field:{field}")
        elif field in FORBIDDEN_FIELDS:
            blockers.append(f"forbidden_field:{field}")
        elif field in CURRENT_DAY_AGGREGATE_FIELDS:
            blockers.append(f"blocked_current_day_aggregate:{field}")
        elif field in UPLIMIT_EVENT_NO_CUTOFF_FIELDS and field not in sentiment_fields:
            blockers.append(f"blocked_event_without_cutoff_or_lag:{field}")
        elif field not in available_fields:
            blockers.append(f"missing_after_sidecar:{field}")
        elif field in event_fields and field not in sentiment_fields:
            has_event = True
        elif field in diagnostic_context_fields:
            has_diagnostic = True
        elif field in safe_context_fields or field in cap_fields or field in fulla_fields or field in sentiment_fields:
            pass
    if blockers:
        return "still_blocked", blockers
    if has_diagnostic:
        return "diagnostic_context_only", []
    if has_event:
        return "event_state_cutoff_canary", []
    return "sidecar_context_formula", []


def _pack_payload(pack_id: str, lane: str, rows: list[dict[str, Any]], source: dict[str, Any]) -> dict[str, Any]:
    return {
        "factor_pack_id": pack_id,
        "factor_pack_version": "phase3ar-sidecar-field-adapter-v1-2026-06-10",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "lane": lane,
        "candidate_count": len(rows),
        "candidate_rows": rows,
        "policy": {
            "x0_r3_mode": "read_only",
            "promotion_allowed": False,
            "source": source,
            "sidecar_semantics": "lagged context exact-date join; cap fields previous available daily row; event fields hidden before encoded cutoff",
        },
    }


def adapt(
    *,
    canary_panel: Path,
    blocked_rows_path: Path,
    aq_contract_path: Path,
    context_root: Path,
    event_panel: Path,
    event_contract_path: Path,
    event_derived_panel: Path,
    hfq_root: Path,
    hfq_2026: Path,
    fulla_root: Path,
    sentiment_root: Path,
    output_root: Path,
    report_root: Path,
    run_smoke: bool,
    smoke_max_per_pack: int,
    smoke_sample_trade_times: int,
    write_augmented_panel: bool = True,
) -> dict[str, Any]:
    canary_panel = _resolve(canary_panel)
    blocked_rows_path = _resolve(blocked_rows_path)
    aq_contract_path = _resolve(aq_contract_path)
    context_root = _resolve(context_root)
    event_panel = _resolve(event_panel)
    event_contract_path = _resolve(event_contract_path)
    event_derived_panel = _resolve(event_derived_panel)
    fulla_root = _resolve(fulla_root)
    sentiment_root = _resolve(sentiment_root)
    output_root = _resolve(output_root)
    report_root = _resolve(report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    progress_path = output_root / "phase3ar_progress.json"

    def mark_stage(stage: str, **extra: Any) -> None:
        _write_json(
            progress_path,
            {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "stage": stage,
                **extra,
            },
        )

    mark_stage("start")
    blocked_rows = _blocked_candidate_rows(blocked_rows_path)
    aq_contract = _contract_lookup(aq_contract_path)
    context_contract = _contract_lookup(context_root / "field_contract.csv")
    event_contract = _contract_lookup(event_contract_path)
    mark_stage("read_contracts", blocked_candidate_count=len(blocked_rows))

    formula_fields: Counter[str] = Counter()
    for row in blocked_rows:
        for field in _fields(str(row.get("expression") or "")):
            formula_fields[field] += 1

    canary_schema = _schema_names(canary_panel)
    if write_augmented_panel:
        core_canary_cols = [
            "code",
            "date",
            "exec_date",
            "trade_time",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "volume",
            "amount",
            "amount_yuan",
        ]
        direct_formula_cols = sorted(
            field for field in formula_fields if field in aq_contract and field in canary_schema and field not in set(core_canary_cols)
        )
    else:
        core_canary_cols = ["code", "exec_date", "trade_time"]
        direct_formula_cols = []
    canary_cols = [col for col in [*core_canary_cols, *direct_formula_cols] if col in canary_schema]
    mark_stage(
        "read_canary_begin",
        canary_schema_columns=len(canary_schema),
        canary_read_columns=len(canary_cols),
        direct_formula_columns=len(direct_formula_cols),
    )
    canary = pd.read_parquet(canary_panel, columns=canary_cols)
    canary["exec_date"] = canary["exec_date"].astype(str)
    canary["code"] = canary["code"].map(_normalize_cn_code)
    canary["trade_time"] = pd.to_datetime(canary["trade_time"], errors="coerce")
    canary = _attach_minute_open_gap_fields(canary, formula_fields)
    base_keys = canary[["code", "exec_date", "trade_time"]].copy()
    mark_stage(
        "read_canary",
        canary_rows=len(canary),
        canary_columns=len(canary.columns),
        canary_codes=int(canary["code"].nunique()),
        trade_times=int(canary["trade_time"].nunique()),
    )

    context_fields = {field for field in formula_fields if field in context_contract and not field.startswith("ctx_fund_")}
    safe_context_fields = {
        field
        for field in context_fields
        if str(context_contract[field].get("selector_allowed") or "").startswith(("true_after", "true_lagged"))
    }
    diagnostic_context_fields = context_fields - safe_context_fields
    event_fields = {field for field in formula_fields if field in event_contract or field.startswith("evt_") or field.startswith("mkt_")}
    event_derived_fields = {field for field in formula_fields if field in EVENT_DERIVED_LAG_FIELDS}
    cap_fields = {field for field in formula_fields if field in CAP_FIELD_ALIASES}
    fulla_fields = {
        field
        for field in formula_fields
        if field.startswith("ctx_fulla_") or field in FUND_DERIVED_SOURCE_FIELDS or field in CROSS_FUND_DERIVED_SOURCE_FIELDS
    }
    sentiment_fields = {
        field
        for field in formula_fields
        if field in OPEN_SENTIMENT_FIELDS
        or field in ZLS_OPEN_SENTIMENT_FIELDS
        or field in ZLS_HOT_DAY_FIELDS
        or field in THS_HOT_FIELDS
        or field in ZLS_EVENT_FIELDS
        or field in UPLIMIT_EVENT_LAG_COMPAT_FIELDS
    }

    mark_stage("load_context_sidecar_begin", field_count=len(context_fields))
    context_panel, context_meta = _load_context_sidecar(context_root, context_fields, base_keys)
    mark_stage("load_context_sidecar_done", loaded_fields=len(context_meta.get("loaded_fields", [])))
    mark_stage("load_event_sidecar_begin", field_count=len(event_fields))
    event_panel_df, event_meta = _load_event_sidecar(event_panel, event_fields, base_keys)
    mark_stage("load_event_sidecar_done", loaded_fields=len(event_meta.get("loaded_fields", [])))
    mark_stage("load_event_derived_sidecar_begin", field_count=len(event_derived_fields))
    event_derived_panel_df, event_derived_meta = _load_event_derived_sidecar(event_derived_panel, event_derived_fields, base_keys)
    mark_stage("load_event_derived_sidecar_done", loaded_fields=len(event_derived_meta.get("loaded_fields", [])))
    mark_stage("load_cap_sidecar_begin", field_count=len(cap_fields))
    cap_panel, cap_meta = _load_cap_sidecar(hfq_root, hfq_2026, cap_fields, base_keys)
    mark_stage("load_cap_sidecar_done", loaded_fields=len(cap_meta.get("loaded_fields", [])))
    mark_stage("load_fulla_sidecar_begin", field_count=len(fulla_fields))
    fulla_panel, fulla_meta = _load_fulla_sidecar(fulla_root, fulla_fields, base_keys)
    mark_stage("load_fulla_sidecar_done", loaded_fields=len(fulla_meta.get("loaded_fields", [])))
    mark_stage("load_sentiment_sidecar_begin", field_count=len(sentiment_fields))
    sentiment_panel, sentiment_meta = _load_sentiment_sidecar(sentiment_root, sentiment_fields, base_keys)
    mark_stage("load_sentiment_sidecar_done", loaded_fields=len(sentiment_meta.get("loaded_fields", [])))

    sidecar_dir = output_root / "sidecars"
    sidecar_dir.mkdir(parents=True, exist_ok=True)
    sidecar_frames = {
        "context": context_panel,
        "event": event_panel_df,
        "event_derived": event_derived_panel_df,
        "cap": cap_panel,
        "fulla": fulla_panel,
        "sentiment": sentiment_panel,
    }
    sidecar_outputs: dict[str, str] = {}
    for name, frame in sidecar_frames.items():
        path = sidecar_dir / f"phase3ar_{name}_sidecar.parquet"
        frame.to_parquet(path, index=False)
        sidecar_outputs[name] = str(path)
    _write_json(
        output_root / "phase3ar_sidecar_manifest.json",
        {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "canary_panel": str(canary_panel),
            "write_augmented_panel": write_augmented_panel,
            "sidecars": {
                name: {
                    "path": path,
                    "rows": int(len(sidecar_frames[name])),
                    "columns": list(sidecar_frames[name].columns),
                    "grain": "code_exec_date_trade_time" if "trade_time" in sidecar_frames[name].columns else "code_exec_date",
                }
                for name, path in sidecar_outputs.items()
            },
        },
    )
    mark_stage("write_sidecars_done", sidecar_count=len(sidecar_outputs))

    augmented_path: Path | None = None
    augmented: pd.DataFrame | None = None
    if write_augmented_panel:
        augmented = canary.copy()
        for sidecar in sidecar_frames.values():
            keys = ["code", "exec_date", "trade_time"] if "trade_time" in sidecar.columns else ["code", "exec_date"]
            add_cols = [col for col in sidecar.columns if col not in set(keys) and col not in augmented.columns]
            if add_cols:
                compact_sidecar = sidecar[keys + add_cols].drop_duplicates(subset=keys, keep="last")
                augmented = augmented.merge(compact_sidecar, on=keys, how="left")
        augmented_path = output_root / "phase3ar_true_1min_sidecar_canary.parquet"
        augmented.to_parquet(augmented_path, index=False)
        mark_stage("write_augmented_done", augmented_rows=len(augmented), augmented_columns=len(augmented.columns))
    else:
        mark_stage("write_augmented_skipped", reason="no_augmented_panel_mode")

    available_fields = set(canary.columns)
    available_fields.update(field for field in aq_contract if field in canary_schema)
    for sidecar in sidecar_frames.values():
        available_fields.update(col for col in sidecar.columns if col not in {"code", "exec_date", "trade_time"})
    pack_rows: dict[str, list[dict[str, Any]]] = {
        "sidecar_context_formula": [],
        "event_state_cutoff_canary": [],
        "diagnostic_context_only": [],
        "still_blocked": [],
    }
    manifest_rows: list[dict[str, Any]] = []
    for row in blocked_rows:
        expression = str(row.get("expression") or "")
        fields = _fields(expression)
        status, blockers = _classify_formula(
            fields,
            available_fields=available_fields,
            safe_context_fields=safe_context_fields,
            diagnostic_context_fields=diagnostic_context_fields,
            event_fields=event_fields,
            cap_fields=cap_fields,
            fulla_fields=fulla_fields,
            sentiment_fields=sentiment_fields,
        )
        out = dict(row)
        out["phase3ar_status"] = status
        out["phase3ar_fields"] = "|".join(fields)
        out["phase3ar_blockers"] = "|".join(blockers)
        out["dataset_route_id"] = "phase3ar_true_1min_trade_time_sidecar_v1"
        out["official_book_eligible"] = False
        out["x0_r3_mode"] = "read_only"
        pack_rows[status].append(out)
        manifest_rows.append(
            {
                "candidate_id": row.get("candidate_id") or "",
                "status": status,
                "fields": "|".join(fields),
                "blockers": "|".join(blockers),
                "expression": expression,
            }
        )

    source = {
        "canary_panel": str(canary_panel),
        "context_root": str(context_root),
        "event_panel": str(event_panel),
        "event_derived_panel": str(event_derived_panel),
        "hfq_root": str(hfq_root),
        "hfq_2026": str(hfq_2026),
        "fulla_root": str(fulla_root),
        "sentiment_root": str(sentiment_root),
    }
    outputs = {
        "augmented_canary_panel": "" if augmented_path is None else str(augmented_path),
        "sidecar_manifest": str(output_root / "phase3ar_sidecar_manifest.json"),
        **{f"{name}_sidecar": path for name, path in sidecar_outputs.items()},
        "sidecar_context_pack": str(output_root / "phase3ar_sidecar_context_formula_pack.json"),
        "event_state_pack": str(output_root / "phase3ar_event_state_cutoff_canary_pack.json"),
        "diagnostic_context_pack": str(output_root / "phase3ar_diagnostic_context_only_pack.json"),
        "still_blocked_rows": str(output_root / "phase3ar_still_blocked_formula_rows.json"),
        "manifest": str(output_root / "phase3ar_sidecar_adapter_manifest.csv"),
        "field_coverage": str(report_root / "phase3ar_materialized_field_coverage.csv"),
    }
    _write_json(Path(outputs["sidecar_context_pack"]), _pack_payload("phase3ar_sidecar_context_formula_pack", "sidecar_context_formula", pack_rows["sidecar_context_formula"], source))
    _write_json(Path(outputs["event_state_pack"]), _pack_payload("phase3ar_event_state_cutoff_canary_pack", "event_state_cutoff_canary", pack_rows["event_state_cutoff_canary"], source))
    _write_json(Path(outputs["diagnostic_context_pack"]), _pack_payload("phase3ar_diagnostic_context_only_pack", "diagnostic_context_only", pack_rows["diagnostic_context_only"], source))
    _write_json(Path(outputs["still_blocked_rows"]), {"candidate_count": len(pack_rows["still_blocked"]), "candidate_rows": pack_rows["still_blocked"]})
    _write_csv(Path(outputs["manifest"]), manifest_rows)

    smoke: dict[str, Any] = {
        "enabled": bool(run_smoke and augmented is not None),
        "max_per_pack": smoke_max_per_pack,
        "sample_trade_times": smoke_sample_trade_times,
        "packs": {},
    }
    if run_smoke and augmented is None:
        smoke["skip_reason"] = "no_augmented_panel_mode"
    if run_smoke and augmented is not None:
        mark_stage("expression_smoke_begin")
        smoke_frame = augmented
        original_trade_time_count = int(smoke_frame["trade_time"].nunique())
        if smoke_sample_trade_times > 0 and original_trade_time_count > smoke_sample_trade_times:
            unique_times = pd.Series(smoke_frame["trade_time"].dropna().unique()).sort_values(ignore_index=True)
            positions = np.linspace(0, len(unique_times) - 1, smoke_sample_trade_times).round().astype(int)
            sampled_times = set(pd.to_datetime(unique_times.iloc[positions]).tolist())
            smoke_frame = smoke_frame[smoke_frame["trade_time"].isin(sampled_times)].copy().reset_index(drop=True)
        for status in ("sidecar_context_formula", "event_state_cutoff_canary", "diagnostic_context_only"):
            rows = pack_rows[status]
            smoke_rows = rows[:smoke_max_per_pack] if smoke_max_per_pack > 0 else rows
            errors: list[dict[str, Any]] = []
            nonnull = 0
            for row in smoke_rows:
                expr = str(row.get("expression") or "")
                try:
                    signal = pd.to_numeric(evaluate_panel_expression(smoke_frame, expr), errors="coerce")
                    if signal.notna().sum() > 0:
                        nonnull += 1
                except Exception as exc:  # noqa: BLE001 - keep formula smoke errors in report.
                    errors.append({"candidate_id": row.get("candidate_id") or "", "error": f"{type(exc).__name__}: {str(exc)[:240]}"})
            smoke["packs"][status] = {
                "candidate_count": len(rows),
                "smoked_candidate_count": len(smoke_rows),
                "smoke_panel_rows": int(len(smoke_frame)),
                "original_trade_time_count": original_trade_time_count,
                "smoke_trade_time_count": int(smoke_frame["trade_time"].nunique()),
                "nonnull_signal_count": nonnull,
                "error_count": len(errors),
                "errors": errors[:50],
            }
        mark_stage("expression_smoke_done")

    field_contract_rows: list[dict[str, Any]] = []
    for field, count in formula_fields.most_common():
        route = "unknown"
        selector_allowed = ""
        if field in aq_contract:
            route = "true_1min_direct_or_opening"
            selector_allowed = aq_contract[field].get("formula_allowed", "")
        elif field in safe_context_fields:
            route = "lagged_context_sidecar"
            selector_allowed = context_contract[field].get("selector_allowed", "")
        elif field in diagnostic_context_fields:
            route = "diagnostic_context_sidecar"
            selector_allowed = context_contract[field].get("selector_allowed", "")
        elif field in event_fields:
            route = "event_state_cutoff_sidecar"
            selector_allowed = event_contract.get(field, {}).get("allowed_use", "cutoff_required")
        elif field in cap_fields:
            route = "lagged_cap_sidecar_previous_daily"
            selector_allowed = "true_previous_available_daily_only"
        elif field in fulla_fields:
            route = "fulla_pit_context_sidecar"
            selector_allowed = "true_notice_date_previous_available_only"
        elif field in sentiment_fields:
            route = "zzshare_lagged_sentiment_sidecar"
            selector_allowed = "true_tplus1_daily_context_only"
        elif field in FORBIDDEN_FIELDS:
            route = "blocked_legacy_semantic"
            selector_allowed = "false"
        elif field in CURRENT_DAY_AGGREGATE_FIELDS:
            route = "blocked_current_day_aggregate"
            selector_allowed = "false_current_day_future_information"
        elif field in UPLIMIT_EVENT_NO_CUTOFF_FIELDS:
            route = "blocked_event_without_cutoff_or_lag"
            selector_allowed = "false_requires_cutoff_suffix_or_lag1_alias"
        if field in UPLIMIT_EVENT_LAG_COMPAT_FIELDS and field in sentiment_fields:
            route = "uplimit_event_lag_compat_sidecar"
            selector_allowed = "true_previous_available_daily_only_not_same_day_cutoff"
        field_contract_rows.append(
            {
                "field_name": field,
                "formula_reference_count": count,
                "phase3ar_route": route,
                "selector_allowed": selector_allowed,
                "materialized_in_augmented_canary": field in available_fields,
            }
        )
    _write_csv(report_root / "phase3ar_field_integration_contract.csv", field_contract_rows)

    coverage_rows: list[dict[str, Any]] = []
    for field, count in formula_fields.most_common():
        source_frame = None
        for frame in [canary, *sidecar_frames.values()]:
            if field in frame.columns:
                source_frame = frame
                break
        if source_frame is not None:
            nonnull = int(source_frame[field].notna().sum())
            coverage_rows.append(
                {
                    "field_name": field,
                    "formula_reference_count": count,
                    "materialized": True,
                    "nonnull_rows": nonnull,
                    "nonnull_rate": round(nonnull / len(source_frame), 8) if len(source_frame) else 0.0,
                    "unique_nonnull": int(source_frame[field].dropna().nunique()),
                    "coverage_grain": "code_exec_date_trade_time" if "trade_time" in source_frame.columns else "code_exec_date",
                }
            )
        elif not write_augmented_panel and field in aq_contract and field in canary_schema:
            coverage_rows.append(
                {
                    "field_name": field,
                    "formula_reference_count": count,
                    "materialized": True,
                    "nonnull_rows": None,
                    "nonnull_rate": None,
                    "unique_nonnull": None,
                    "coverage_grain": "schema_available_lazy_direct_1min",
                }
            )
        else:
            coverage_rows.append(
                {
                    "field_name": field,
                    "formula_reference_count": count,
                    "materialized": False,
                    "nonnull_rows": 0,
                    "nonnull_rate": 0.0,
                    "unique_nonnull": 0,
                    "coverage_grain": "",
                }
            )
    _write_csv(report_root / "phase3ar_materialized_field_coverage.csv", coverage_rows)

    status_counts = {key: len(value) for key, value in pack_rows.items()}
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3AR_SIDECAR_FIELDS_ATTACHED_FOR_TRUE_1MIN_CANARY",
        "input_blocked_candidate_count": len(blocked_rows),
        "status_counts": status_counts,
        "write_augmented_panel": write_augmented_panel,
        "augmented_panel_rows": None if augmented is None else int(len(augmented)),
        "augmented_panel_columns": None if augmented is None else int(len(augmented.columns)),
        "sidecar_outputs": sidecar_outputs,
        "context_meta": context_meta,
        "event_meta": event_meta,
        "cap_meta": cap_meta,
        "fulla_meta": fulla_meta,
        "sentiment_meta": sentiment_meta,
        "smoke": smoke,
        "outputs": outputs | {"field_contract": str(report_root / "phase3ar_field_integration_contract.csv")},
        "hard_rules": [
            "context fields join on code + exec_date; context panel date is renamed to exec_date",
            "cap fields use previous available daily row by code, never same-day exact match",
            "event fields are null before their encoded cutoff time",
            "daily_ret remains blocked",
            "current-day aggregate fields such as m1_amount_day remain blocked because they require full-day future information",
            "whitelisted raw evt_uplimit_* fields are allowed only as previous-available daily lag context; non-whitelisted event fields still require cutoff suffix or lag1 alias",
            "billboard fields remain diagnostic until disclosure timestamp contract is proven",
            "X0/R3 read-only",
        ],
    }
    _write_json(output_root / "phase3ar_sidecar_field_adapter_report.json", summary)
    _write_json(report_root / "phase3ar_sidecar_field_adapter_summary.json", summary)
    mark_stage("complete", status_counts=status_counts)
    lines = [
        "# Phase3AR Sidecar Field Adapter",
        "",
        f"decision: `{summary['decision']}`",
        "",
        "## Counts",
        "",
        f"- input blocked candidates: `{summary['input_blocked_candidate_count']}`",
    ]
    for key, value in sorted(status_counts.items()):
        lines.append(f"- `{key}`: {value}")
    lines.extend(
        [
            "",
            "## Hard Rules",
            "",
            "- `ctx_*` fields join by `code + exec_date`; partitioned context `date` is explicitly treated as `exec_date`.",
            "- cap fields use previous available daily row, not same-day daily values.",
            "- `evt_*` and `mkt_*` fields are hidden before the cutoff encoded in the field name.",
            "- `daily_ret` remains blocked.",
            "- current-day aggregate fields such as `m1_amount_day` remain blocked because they require full-day future information.",
            "- whitelisted raw `evt_uplimit_*` fields are allowed only as previous-available daily lag context; non-whitelisted event fields still require cutoff suffix or lag1 alias.",
            "- billboard fields remain diagnostic until a disclosure timestamp contract exists.",
            "",
            "## Outputs",
            "",
            f"- augmented canary: `{outputs['augmented_canary_panel'] or 'SKIPPED_NO_AUGMENTED_PANEL_MODE'}`",
            f"- sidecar manifest: `{outputs['sidecar_manifest']}`",
            f"- context pack: `{outputs['sidecar_context_pack']}`",
            f"- event pack: `{outputs['event_state_pack']}`",
            f"- diagnostic pack: `{outputs['diagnostic_context_pack']}`",
            f"- still blocked: `{outputs['still_blocked_rows']}`",
            f"- field coverage: `{outputs['field_coverage']}`",
        ]
    )
    if run_smoke:
        lines.extend(["", "## Expression Smoke", ""])
        for key, payload in smoke["packs"].items():
            lines.append(
                f"- `{key}`: candidates={payload['candidate_count']} smoked={payload['smoked_candidate_count']} nonnull={payload['nonnull_signal_count']} errors={payload['error_count']}"
            )
    (report_root / "PHASE3AR_SIDECAR_FIELD_ADAPTER_20260610.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--canary-panel", type=Path, default=DEFAULT_CANARY_PANEL)
    parser.add_argument("--blocked-rows", type=Path, default=DEFAULT_BLOCKED_ROWS)
    parser.add_argument("--aq-contract", type=Path, default=DEFAULT_AQ_CONTRACT)
    parser.add_argument("--context-root", type=Path, default=DEFAULT_CONTEXT_ROOT)
    parser.add_argument("--event-panel", type=Path, default=DEFAULT_EVENT_PANEL)
    parser.add_argument("--event-contract", type=Path, default=DEFAULT_EVENT_CONTRACT)
    parser.add_argument("--event-derived-panel", type=Path, default=DEFAULT_EVENT_DERIVED_PANEL)
    parser.add_argument("--hfq-root", type=Path, default=DEFAULT_HFQ_ROOT)
    parser.add_argument("--hfq-2026", type=Path, default=DEFAULT_HFQ_2026)
    parser.add_argument("--fulla-root", type=Path, default=DEFAULT_FULLA_ROOT)
    parser.add_argument("--sentiment-root", type=Path, default=DEFAULT_SENTIMENT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    parser.add_argument("--run-smoke", action="store_true")
    parser.add_argument("--smoke-max-per-pack", type=int, default=24)
    parser.add_argument("--smoke-sample-trade-times", type=int, default=2400)
    parser.add_argument(
        "--no-augmented-panel",
        action="store_true",
        help="Write sidecar packs/manifests only and skip the memory-heavy combined 1min augmented parquet.",
    )
    args = parser.parse_args()
    summary = adapt(
        canary_panel=args.canary_panel,
        blocked_rows_path=args.blocked_rows,
        aq_contract_path=args.aq_contract,
        context_root=args.context_root,
        event_panel=args.event_panel,
        event_contract_path=args.event_contract,
        event_derived_panel=args.event_derived_panel,
        hfq_root=args.hfq_root,
        hfq_2026=args.hfq_2026,
        fulla_root=args.fulla_root,
        sentiment_root=args.sentiment_root,
        output_root=args.output_root,
        report_root=args.report_root,
        run_smoke=args.run_smoke,
        smoke_max_per_pack=args.smoke_max_per_pack,
        smoke_sample_trade_times=args.smoke_sample_trade_times,
        write_augmented_panel=not args.no_augmented_panel,
    )
    print(
        json.dumps(
            {
                "decision": summary["decision"],
                "status_counts": summary["status_counts"],
                "augmented_panel_rows": summary["augmented_panel_rows"],
                "augmented_panel_columns": summary["augmented_panel_columns"],
                "outputs": summary["outputs"],
                "smoke": summary["smoke"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
