"""Build a CE2 event-derived daily panel from HFQ daily silver data.

The output is a code-date daily panel. It is intended to be joined into
true-1min canary/eval panels as previous-available daily context only.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from our_system_phase2.services.event_derived_features import (
    attach_event_derived_features,
    event_derived_feature_coverage_report,
)


DATA_ROOT = Path(r"G:\Project_V7_Rotation\data\cn_public_enrichment\cn_local_minute_daily_silver_v1_20260531")
DEFAULT_HFQ_ROOT = DATA_ROOT / "hfq_daily_2024_2025"
DEFAULT_OUTPUT = Path("runtime/phase3ce2_event_derived_daily_panel_202406_202512/phase3ce2_event_derived_daily_panel.parquet")
DEFAULT_REPORT = Path("reports/phase3ce2_event_derived_daily_panel_202406_202512/phase3ce2_event_derived_daily_panel_summary.json")

EVENT_REQUIRED_COLUMNS = [
    "date",
    "code",
    "open_hfq",
    "high_hfq",
    "low_hfq",
    "close_hfq",
    "is_limit_up",
    "pct_chg",
]


def _truthy_limit(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    text = str(value).strip().lower()
    if text in {"1", "1.0", "true", "yes", "y", "涨停", "是"}:
        return 1.0
    return 0.0


def _normalize_cn_code(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if "." in raw:
        left, right = raw.split(".", 1)
        digits = "".join(ch for ch in left if ch.isdigit())
        suffix = "".join(ch for ch in right if ch.isalpha())
        return f"{digits.zfill(6)}.{suffix}" if digits and suffix else raw
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) < 6:
        return raw
    digits = digits[-6:]
    if digits.startswith(("60", "68", "90", "51", "52", "56", "58")):
        return f"{digits}.SH"
    if digits.startswith(("00", "30", "15", "16", "18", "39")):
        return f"{digits}.SZ"
    if digits.startswith(("43", "83", "87", "88", "92")):
        return f"{digits}.BJ"
    return digits


def _read_hfq_root(root: Path) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for path in sorted(root.rglob("*.parquet")):
        schema = pq.ParquetFile(path).schema_arrow.names
        cols = [col for col in EVENT_REQUIRED_COLUMNS if col in schema]
        if not {"date", "code", "open_hfq", "high_hfq", "low_hfq", "close_hfq"}.issubset(cols):
            continue
        frame = pd.read_parquet(path, columns=cols)
        parts.append(frame)
    if not parts:
        return pd.DataFrame(columns=["date", "code", "open", "high", "low", "close", "is_limit_up", "rt_change_pct"])
    raw = pd.concat(parts, ignore_index=True)
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(raw["date"], errors="coerce"),
            "code": raw["code"].map(_normalize_cn_code),
            "open": pd.to_numeric(raw["open_hfq"], errors="coerce"),
            "high": pd.to_numeric(raw["high_hfq"], errors="coerce"),
            "low": pd.to_numeric(raw["low_hfq"], errors="coerce"),
            "close": pd.to_numeric(raw["close_hfq"], errors="coerce"),
            "is_limit_up": raw["is_limit_up"].map(_truthy_limit).astype(float) if "is_limit_up" in raw else 0.0,
            "rt_change_pct": pd.to_numeric(raw["pct_chg"], errors="coerce") if "pct_chg" in raw else pd.NA,
        }
    )
    return out.dropna(subset=["date", "code", "open", "high", "low", "close"]).sort_values(["code", "date"])


def build(*, hfq_root: Path, output_path: Path, report_path: Path, max_window_n: int) -> dict[str, Any]:
    hfq_root = hfq_root.resolve()
    panel = _read_hfq_root(hfq_root)
    if panel.empty:
        raise RuntimeError(f"no_hfq_daily_rows:{hfq_root}")
    derived = attach_event_derived_features(panel, max_streak_n=max_window_n)
    grouped = derived.sort_values(["code", "date"]).groupby("code", sort=False)
    generated: dict[str, pd.Series] = {}
    base_fields = {
        "close": "limit_up_close_event",
        "open": "limit_up_open_event",
        "touch": "limit_up_touch_event",
        "open_not_close": "limit_up_open_not_close",
        "touch_not_close": "limit_up_touch_not_close",
        "close_not_open": "limit_up_close_not_open",
    }
    for window in range(2, int(max_window_n) + 1):
        for suffix, field in base_fields.items():
            generated[f"limit_up_{suffix}_count_t{window}"] = grouped[field].transform(
                lambda item, w=window: pd.to_numeric(item, errors="coerce").fillna(0.0).rolling(w, min_periods=1).sum()
            )
        generated[f"limit_up_any_open_not_close_in_t{window}"] = generated[f"limit_up_open_not_close_count_t{window}"].gt(0.0).astype(float)
        generated[f"limit_up_any_close_not_open_in_t{window}"] = generated[f"limit_up_close_not_open_count_t{window}"].gt(0.0).astype(float)
    if generated:
        derived = pd.concat([derived, pd.DataFrame(generated, index=derived.index)], axis=1)

    output_fields = [
        "date",
        "code",
        "high_board_rank",
        "is_market_high_board",
        *[f"limit_up_any_close_not_open_in_t{window}" for window in range(2, int(max_window_n) + 1)],
        *[f"limit_up_any_open_not_close_in_t{window}" for window in range(2, int(max_window_n) + 1)],
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    derived[output_fields].to_parquet(output_path, index=False)
    coverage = event_derived_feature_coverage_report(derived, max_streak_n=max_window_n)
    summary = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "decision": "PHASE3CE2_EVENT_DERIVED_DAILY_PANEL_BUILT",
        "hfq_root": str(hfq_root),
        "output_path": str(output_path),
        "rows": int(len(derived)),
        "code_count": int(derived["code"].nunique()),
        "date_min": str(derived["date"].min().date()),
        "date_max": str(derived["date"].max().date()),
        "output_fields": output_fields,
        "coverage": {field: coverage["coverage"].get(field) for field in output_fields if field not in {"date", "code"}},
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hfq-root", type=Path, default=DEFAULT_HFQ_ROOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--max-window-n", type=int, default=10)
    args = parser.parse_args()
    summary = build(hfq_root=args.hfq_root, output_path=args.output, report_path=args.report, max_window_n=args.max_window_n)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
