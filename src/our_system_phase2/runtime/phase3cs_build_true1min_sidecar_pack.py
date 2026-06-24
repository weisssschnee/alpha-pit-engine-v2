"""Build compact sidecar packs for true-1min shard augmentation.

The output is intentionally compact and PIT-aware:

* stock_event_sidecar: same-day limit/upboard event fields with an explicit
  cutoff minute. These fields must be consumed by event/state primitives.
* stock_lagged_context: code-date fields that are only usable from the next
  available trading day onward.
* market_lagged_context: market-date fields, also previous-available only.

This route does not touch X0/R3 and does not run search.
"""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


REPO = Path(__file__).resolve().parents[3]
DATA_ROOT = Path(r"G:\Project_V7_Rotation\data\cn_public_enrichment")
DEFAULT_ZLS_ROOT = DATA_ROOT / "cn_zzshare_limit_sentiment_pack_v1_20260602" / "silver_parquet"
DEFAULT_RZRQ = DATA_ROOT / "cn_public_rzrq_daily_silver_v1_20260530" / "rzrq_margin_xsection_daily.parquet"
DEFAULT_XSECTION = DATA_ROOT / "cn_public_xsection_no_kline_silver_v1_20260530"
DEFAULT_HFQ_ROOT = DATA_ROOT / "cn_local_minute_daily_silver_v1_20260531" / "hfq_daily_2024_2025"
DEFAULT_HFQ_2026 = DATA_ROOT / "cn_local_minute_daily_silver_v1_20260531" / "hfq_daily_2026" / "hfq_daily_2026.parquet"
DEFAULT_OUTPUT_ROOT = Path("runtime/phase3cs_true1min_sidecar_pack_20260624")
DEFAULT_REPORT_ROOT = Path("reports/phase3cs_true1min_sidecar_pack_20260624")


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _normalize_cn_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text or text in {"NAN", "NONE", "NULL"}:
        return ""
    if "." in text:
        left, right = text.split(".", 1)
        digits = re.sub(r"\D", "", left)[-6:]
        suffix = re.sub(r"[^A-Z]", "", right)
        if digits and suffix:
            return f"{digits.zfill(6)}.{suffix[:2]}"
    digits = re.sub(r"\D", "", text)
    if len(digits) < 6:
        return ""
    digits = digits[-6:]
    if digits.startswith(("0", "2", "3")):
        return f"{digits}.SZ"
    if digits.startswith(("4", "8", "9")):
        return f"{digits}.BJ"
    return f"{digits}.SH"


def _date_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("float32")


def _read_parquet_existing(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    schema = set(pq.ParquetFile(path).schema_arrow.names)
    cols = [col for col in columns if col in schema]
    if not cols:
        return pd.DataFrame()
    return pd.read_parquet(path, columns=cols)


def _parse_cutoff_minute(value: Any) -> float:
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null", "--"}:
        return np.nan
    match = re.search(r"([01]?\d|2[0-3])[:：]?([0-5]\d)", text)
    if not match:
        return np.nan
    hour = int(match.group(1))
    minute = int(match.group(2))
    return float(hour * 60 + minute)


def _stock_event_sidecar(zls_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = zls_root / "uplimit_stocks.parquet"
    fields = [
        "stock_code",
        "date1",
        "request_date",
        "up_limit_time",
        "amount",
        "auction_buy",
        "auction_money",
        "auction_offer",
        "auction_pre1max_ratio",
        "auction_turnover",
        "fd_close",
        "fd_max",
        "up_limit_keep_times",
        "up_limit_type",
    ]
    raw = _read_parquet_existing(path, fields)
    if raw.empty:
        return pd.DataFrame(columns=["code", "source_date"]), {"source_path": str(path), "rows": 0}
    out = pd.DataFrame()
    out["code"] = raw["stock_code"].map(_normalize_cn_code)
    date_source = raw["date1"] if "date1" in raw else raw.get("request_date")
    out["source_date"] = _date_series(date_source)
    out["evt_uplimit_cutoff_minute"] = raw.get("up_limit_time", pd.Series(index=raw.index)).map(_parse_cutoff_minute).astype("float32")
    numeric_fields = [
        "amount",
        "auction_buy",
        "auction_money",
        "auction_offer",
        "auction_pre1max_ratio",
        "auction_turnover",
        "fd_close",
        "fd_max",
        "up_limit_keep_times",
    ]
    for field in numeric_fields:
        if field in raw:
            out[f"evt_uplimit_{field}"] = _num(raw[field])
    if "up_limit_type" in raw:
        # Keep as a numeric state when possible; nonnumeric values are not formula inputs.
        out["evt_uplimit_type_code"] = pd.factorize(raw["up_limit_type"].fillna("").astype(str))[0].astype("float32")
    out = out.dropna(subset=["code", "source_date"]).query("code != ''")
    out = out.sort_values(["code", "source_date", "evt_uplimit_cutoff_minute"]).drop_duplicates(["code", "source_date"], keep="first")
    return out.reset_index(drop=True), {"source_path": str(path), "rows": int(len(out)), "columns": list(out.columns)}


def _market_sentiment_sidecar(zls_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    frames: list[pd.DataFrame] = []
    meta: dict[str, Any] = {}
    open_path = zls_root / "open_sentiment_data.parquet"
    open_cols = [
        "date1",
        "uplimit_num",
        "downlimit_num",
        "up_num",
        "down_num",
        "zb_num",
        "lb_2_num",
        "lb_3_num",
        "max_lb_num",
        "mian_num",
        "tiandi_num",
        "ditian_num",
        "damian_num",
        "gt5_num",
        "lt5_num",
        "lb_h_num",
    ]
    open_df = _read_parquet_existing(open_path, open_cols)
    if not open_df.empty:
        out = pd.DataFrame({"source_date": _date_series(open_df["date1"])})
        for field in [col for col in open_cols if col != "date1" and col in open_df]:
            out[f"ctx_sent_{field}"] = _num(open_df[field])
        frames.append(out)
        meta["open_sentiment_rows"] = int(len(out))

    hot_path = zls_root / "sentiment_hot_day.parquet"
    hot_cols = ["Day", "df_num", "lbgd", "strong", "ztjs"]
    hot_df = _read_parquet_existing(hot_path, hot_cols)
    if not hot_df.empty:
        out = pd.DataFrame({"source_date": _date_series(hot_df["Day"])})
        for field in [col for col in hot_cols if col != "Day" and col in hot_df]:
            out[f"ctx_zls_{field}"] = _num(hot_df[field])
        frames.append(out)
        meta["sentiment_hot_rows"] = int(len(out))

    if not frames:
        return pd.DataFrame(columns=["source_date"]), meta
    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on="source_date", how="outer")
    merged = merged.dropna(subset=["source_date"]).sort_values("source_date").drop_duplicates("source_date", keep="last")
    meta["rows"] = int(len(merged))
    meta["columns"] = list(merged.columns)
    return merged.reset_index(drop=True), meta


def _stock_hot_sidecar(zls_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = zls_root / "ths_hot_top.parquet"
    cols = ["collect_date", "symbol_code", "rank", "rank_diff", "last_pct", "last_price", "circulation_value"]
    raw = _read_parquet_existing(path, cols)
    if raw.empty:
        return pd.DataFrame(columns=["code", "source_date"]), {"source_path": str(path), "rows": 0}
    out = pd.DataFrame()
    out["code"] = raw["symbol_code"].map(_normalize_cn_code)
    out["source_date"] = _date_series(raw["collect_date"])
    for field in [col for col in cols if col not in {"collect_date", "symbol_code"} and col in raw]:
        out[f"ctx_ths_hot_{field}"] = _num(raw[field])
    out = out.dropna(subset=["code", "source_date"]).query("code != ''")
    out = out.sort_values(["code", "source_date", "ctx_ths_hot_rank"]).drop_duplicates(["code", "source_date"], keep="first")
    return out.reset_index(drop=True), {"source_path": str(path), "rows": int(len(out)), "columns": list(out.columns)}


def _rzrq_sidecar(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    cols = ["DATE", "SCODE", "RZYE", "RQYE", "RZRQYE", "RZYEZB", "RZMRE", "RZCHE", "RZJME", "RQYL"]
    raw = _read_parquet_existing(path, cols)
    if raw.empty:
        return pd.DataFrame(columns=["code", "source_date"]), {"source_path": str(path), "rows": 0}
    out = pd.DataFrame()
    out["code"] = raw["SCODE"].map(_normalize_cn_code)
    out["source_date"] = _date_series(raw["DATE"])
    for field in [col for col in cols if col not in {"DATE", "SCODE"} and col in raw]:
        out[f"ctx_rzrq_{field.lower()}"] = _num(raw[field])
    out = out.dropna(subset=["code", "source_date"]).query("code != ''")
    out = out.sort_values(["code", "source_date"]).drop_duplicates(["code", "source_date"], keep="last")
    return out.reset_index(drop=True), {"source_path": str(path), "rows": int(len(out)), "columns": list(out.columns)}


def _billboard_sidecar(xsection_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = xsection_root / "billboard_details" / "billboard_details.parquet"
    cols = [
        "TRADE_DATE",
        "SECURITY_CODE",
        "SECUCODE",
        "DEAL_AMOUNT_RATIO",
        "BILLBOARD_DEAL_AMT",
        "BILLBOARD_NET_AMT",
        "BILLBOARD_BUY_AMT",
        "BILLBOARD_SELL_AMT",
        "DEAL_NET_RATIO",
        "ACCUM_AMOUNT",
        "TURNOVERRATE",
        "BUY_RATIO",
        "SELL_RATIO",
        "SUM_BUY_AMT",
        "SUM_SELL_AMT",
        "NET_BS_AMT",
    ]
    raw = _read_parquet_existing(path, cols)
    if raw.empty:
        return pd.DataFrame(columns=["code", "source_date"]), {"source_path": str(path), "rows": 0}
    code_source = raw["SECUCODE"] if "SECUCODE" in raw else raw["SECURITY_CODE"]
    out = pd.DataFrame()
    out["code"] = code_source.map(_normalize_cn_code)
    out["source_date"] = _date_series(raw["TRADE_DATE"])
    for field in [col for col in cols if col not in {"TRADE_DATE", "SECURITY_CODE", "SECUCODE"} and col in raw]:
        out[f"ctx_billboard_{field.lower()}"] = _num(raw[field])
    out = out.dropna(subset=["code", "source_date"]).query("code != ''")
    agg_cols = [col for col in out.columns if col not in {"code", "source_date"}]
    out = out.groupby(["code", "source_date"], as_index=False)[agg_cols].sum(min_count=1)
    return out.reset_index(drop=True), {"source_path": str(path), "rows": int(len(out)), "columns": list(out.columns)}


def _holder_sidecar(xsection_root: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = xsection_root / "holder_num_detail" / "holder_num_detail.parquet"
    cols = ["HOLD_NOTICE_DATE", "SECURITY_CODE", "SECUCODE", "HOLDER_NUM", "HOLDER_NUM_CHANGE", "HOLDER_NUM_RATIO", "AVG_MARKET_CAP", "AVG_HOLD_NUM"]
    raw = _read_parquet_existing(path, cols)
    if raw.empty:
        return pd.DataFrame(columns=["code", "source_date"]), {"source_path": str(path), "rows": 0}
    code_source = raw["SECUCODE"] if "SECUCODE" in raw else raw["SECURITY_CODE"]
    out = pd.DataFrame()
    out["code"] = code_source.map(_normalize_cn_code)
    out["source_date"] = _date_series(raw["HOLD_NOTICE_DATE"])
    for field in [col for col in cols if col not in {"HOLD_NOTICE_DATE", "SECURITY_CODE", "SECUCODE"} and col in raw]:
        out[f"ctx_holder_{field.lower()}"] = _num(raw[field])
    out = out.dropna(subset=["code", "source_date"]).query("code != ''")
    out = out.sort_values(["code", "source_date"]).drop_duplicates(["code", "source_date"], keep="last")
    return out.reset_index(drop=True), {"source_path": str(path), "rows": int(len(out)), "columns": list(out.columns)}


def _hfq_sidecar(hfq_root: Path, hfq_2026: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    cols = ["date", "code", "turnover_ratio", "volume_ratio", "is_st", "is_limit_up", "market_cap_yuan", "float_market_cap_yuan", "pe_ttm", "pb", "ps_ttm"]
    parts: list[pd.DataFrame] = []
    paths = sorted(hfq_root.rglob("*.parquet"))
    if hfq_2026.exists():
        paths.append(hfq_2026)
    for path in paths:
        frame = _read_parquet_existing(path, cols)
        if not frame.empty and {"date", "code"}.issubset(frame.columns):
            parts.append(frame)
    if not parts:
        return pd.DataFrame(columns=["code", "source_date"]), {"source_paths": [str(path) for path in paths], "rows": 0}
    raw = pd.concat(parts, ignore_index=True)
    out = pd.DataFrame()
    out["code"] = raw["code"].map(_normalize_cn_code)
    out["source_date"] = _date_series(raw["date"])
    for field in [col for col in cols if col not in {"date", "code"} and col in raw]:
        prefix = "prev_is_limit_up" if field == "is_limit_up" else field
        out[f"ctx_hfq_{prefix}"] = _num(raw[field])
    out = out.dropna(subset=["code", "source_date"]).query("code != ''")
    out = out.sort_values(["code", "source_date"]).drop_duplicates(["code", "source_date"], keep="last")
    return out.reset_index(drop=True), {"source_paths": [str(path) for path in paths], "rows": int(len(out)), "columns": list(out.columns)}


def _merge_stock_context(frames: list[pd.DataFrame]) -> pd.DataFrame:
    nonempty = [frame for frame in frames if not frame.empty]
    if not nonempty:
        return pd.DataFrame(columns=["code", "source_date"])
    merged = nonempty[0]
    for frame in nonempty[1:]:
        add_cols = [col for col in frame.columns if col not in {"code", "source_date"} and col not in merged.columns]
        merged = merged.merge(frame[["code", "source_date", *add_cols]], on=["code", "source_date"], how="outer")
    return merged.sort_values(["code", "source_date"]).reset_index(drop=True)


def _field_contract_rows(stock_context: pd.DataFrame, market_context: pd.DataFrame, event_context: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for frame_name, frame, route, availability in [
        ("stock_lagged_context", stock_context, "lagged_context", "previous_available_source_date_lt_exec_date"),
        ("market_lagged_context", market_context, "lagged_context", "previous_available_source_date_lt_exec_date"),
        ("stock_event_context", event_context, "event_state", "same_day_after_evt_uplimit_cutoff_minute"),
    ]:
        for field in frame.columns:
            if field in {"code", "source_date"}:
                continue
            rows.append(
                {
                    "field_name": field,
                    "pack": frame_name,
                    "route": route,
                    "availability_contract": availability,
                    "formula_allowed": route == "lagged_context",
                    "event_primitive_required": route == "event_state",
                    "ordinary_primitive_allowed": False if route == "event_state" else "coverage_guard_required",
                }
            )
    return rows


def build(
    *,
    zls_root: Path,
    rzrq_path: Path,
    xsection_root: Path,
    hfq_root: Path,
    hfq_2026: Path,
    skip_hfq: bool,
    skip_rzrq: bool,
    skip_billboard: bool,
    skip_holder: bool,
    output_root: Path,
    report_root: Path,
) -> dict[str, Any]:
    output_root = _resolve(output_root)
    report_root = _resolve(report_root)
    output_root.mkdir(parents=True, exist_ok=True)
    report_root.mkdir(parents=True, exist_ok=True)

    event_context, event_meta = _stock_event_sidecar(zls_root)
    market_context, market_meta = _market_sentiment_sidecar(zls_root)
    stock_hot, stock_hot_meta = _stock_hot_sidecar(zls_root)
    if skip_rzrq:
        rzrq = pd.DataFrame(columns=["code", "source_date"])
        rzrq_meta = {"skipped": True, "reason": "skip_rzrq"}
    else:
        rzrq, rzrq_meta = _rzrq_sidecar(rzrq_path)
    if skip_billboard:
        billboard = pd.DataFrame(columns=["code", "source_date"])
        billboard_meta = {"skipped": True, "reason": "skip_billboard"}
    else:
        billboard, billboard_meta = _billboard_sidecar(xsection_root)
    if skip_holder:
        holder = pd.DataFrame(columns=["code", "source_date"])
        holder_meta = {"skipped": True, "reason": "skip_holder"}
    else:
        holder, holder_meta = _holder_sidecar(xsection_root)
    if skip_hfq:
        hfq = pd.DataFrame(columns=["code", "source_date"])
        hfq_meta = {"skipped": True, "reason": "skip_hfq"}
    else:
        hfq, hfq_meta = _hfq_sidecar(hfq_root, hfq_2026)
    stock_context = _merge_stock_context([stock_hot, rzrq, billboard, holder, hfq])

    stock_context_path = output_root / "phase3cs_stock_lagged_context.parquet"
    market_context_path = output_root / "phase3cs_market_lagged_context.parquet"
    event_context_path = output_root / "phase3cs_stock_event_context.parquet"
    stock_context.to_parquet(stock_context_path, index=False)
    market_context.to_parquet(market_context_path, index=False)
    event_context.to_parquet(event_context_path, index=False)

    contract_rows = _field_contract_rows(stock_context, market_context, event_context)
    contract_path = output_root / "phase3cs_sidecar_field_contract.csv"
    pd.DataFrame(contract_rows).to_csv(contract_path, index=False)

    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3CS_TRUE1MIN_SIDECAR_PACK_BUILT_DIAGNOSTIC_ONLY",
        "outputs": {
            "stock_lagged_context": str(stock_context_path),
            "market_lagged_context": str(market_context_path),
            "stock_event_context": str(event_context_path),
            "field_contract": str(contract_path),
        },
        "row_counts": {
            "stock_lagged_context": int(len(stock_context)),
            "market_lagged_context": int(len(market_context)),
            "stock_event_context": int(len(event_context)),
        },
        "column_counts": {
            "stock_lagged_context": int(len(stock_context.columns)),
            "market_lagged_context": int(len(market_context.columns)),
            "stock_event_context": int(len(event_context.columns)),
        },
        "source_meta": {
            "uplimit_event": event_meta,
            "market_sentiment": market_meta,
            "stock_hot": stock_hot_meta,
            "rzrq": rzrq_meta,
            "billboard": billboard_meta,
            "holder": holder_meta,
            "hfq": hfq_meta,
        },
        "hard_rules": [
            "ctx_* sidecar fields use previous available source_date strictly less than exec_date",
            "evt_uplimit_* fields are same-day event fields and are visible only after evt_uplimit_cutoff_minute",
            "next_open_pct and next_close_pct are not included",
            "text/key fields are not formula inputs",
            "X0/R3 remain read-only",
        ],
    }
    _write_json(output_root / "phase3cs_sidecar_pack_summary.json", summary)
    _write_json(report_root / "phase3cs_sidecar_pack_summary.json", summary)
    report_lines = [
        "# Phase3CS True1min Sidecar Pack 2026-06-24",
        "",
        f"decision: `{summary['decision']}`",
        "",
        "## Row Counts",
        "",
    ]
    for key, value in summary["row_counts"].items():
        report_lines.append(f"- `{key}`: {value}")
    report_lines.extend(["", "## Hard Rules", ""])
    report_lines.extend(f"- {rule}" for rule in summary["hard_rules"])
    (report_root / "PHASE3CS_TRUE1MIN_SIDECAR_PACK_20260624.md").write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--zls-root", type=Path, default=DEFAULT_ZLS_ROOT)
    parser.add_argument("--rzrq-path", type=Path, default=DEFAULT_RZRQ)
    parser.add_argument("--xsection-root", type=Path, default=DEFAULT_XSECTION)
    parser.add_argument("--hfq-root", type=Path, default=DEFAULT_HFQ_ROOT)
    parser.add_argument("--hfq-2026", type=Path, default=DEFAULT_HFQ_2026)
    parser.add_argument("--skip-hfq", action="store_true")
    parser.add_argument("--skip-rzrq", action="store_true")
    parser.add_argument("--skip-billboard", action="store_true")
    parser.add_argument("--skip-holder", action="store_true")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--report-root", type=Path, default=DEFAULT_REPORT_ROOT)
    args = parser.parse_args(argv)
    build(
        zls_root=args.zls_root,
        rzrq_path=args.rzrq_path,
        xsection_root=args.xsection_root,
        hfq_root=args.hfq_root,
        hfq_2026=args.hfq_2026,
        skip_hfq=args.skip_hfq,
        skip_rzrq=args.skip_rzrq,
        skip_billboard=args.skip_billboard,
        skip_holder=args.skip_holder,
        output_root=args.output_root,
        report_root=args.report_root,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
