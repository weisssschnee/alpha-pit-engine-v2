"""Build the existing A-share finalist replay's development session sidecar.

The builder has two deliberately separate stages:

``fetch``
    Snapshot public exchange calendars/security masters and raw CNInfo
    corporate-action records.  It is resumable and performs no financial
    evaluation.

``materialize``
    Verify that snapshot, collapse the frozen development minute panels to
    observed sessions, add explicit suspension/delisting rows, and write the
    session fields required by the existing A-share replay.

The result extends ``PHASE3DY_A_SHARE_TRADABILITY_REPLAY``.  It is not a new
evaluation authority and it never reads validation, holdout, or 2026 panels.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


SCHEMA_VERSION = "cn_finalist_session_authority_v1"
SOURCE_SCHEMA_VERSION = "cn_finalist_public_source_snapshot_v1"
MINUTE_PATTERN = (
    "shard_*/phase3aq_wide_true1min/canary/"
    "phase3aq_true_1min_formula_canary.parquet"
)
SESSION_AUTHORITY_COLUMNS = (
    "date",
    "code",
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
)
CNINFO_DIVIDEND_KEYS = {
    "announcement_date": "F006D",
    "bonus_shares_per_10": "F010N",
    "transfer_shares_per_10": "F011N",
    "cash_per_10": "F012N",
    "record_date": "F018D",
    "effective_date": "F020D",
    "cash_payment_date": "F023D",
    "share_arrival_date": "F025D",
}
FEE_SOURCE_REFERENCES = (
    "https://fgk.chinatax.gov.cn/zcfgk/c102416/c5211343/content.html",
    "https://www.szse.cn/disclosure/notice/t20230818_602805.html",
    "https://one.sse.com.cn/onething/gptz/",
    "https://www.chinaclear.cn/zdjs/editor_file/20220701154723234.pdf",
)


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _verify_self_hash(payload: Mapping[str, Any], field: str) -> None:
    expected = str(payload.get(field) or "")
    candidate = dict(payload)
    candidate.pop(field, None)
    observed = _payload_sha256(candidate)
    if observed != expected:
        raise ValueError(
            f"{field} mismatch: declared={expected} observed={observed}"
        )


def normalize_code(value: Any) -> str:
    """Return a six-digit A-share code without inventing an exchange."""

    if isinstance(value, (int, np.integer)):
        numeric_code = int(value)
        if 0 <= numeric_code <= 999999:
            return f"{numeric_code:06d}"
    if isinstance(value, (float, np.floating)):
        numeric_code = float(value)
        if (
            math.isfinite(numeric_code)
            and numeric_code.is_integer()
            and 0 <= numeric_code <= 999999
        ):
            return f"{int(numeric_code):06d}"
    text = str(value).strip().upper()
    if text.endswith(".0") and text[:-2].isdigit():
        return f"{int(text[:-2]):06d}"
    digits = "".join(character for character in text if character.isdigit())
    if len(digits) < 6:
        raise ValueError(f"unsupported A-share code: {value!r}")
    code = digits[-6:]
    if not code.isdigit():
        raise ValueError(f"unsupported A-share code: {value!r}")
    return code


def exchange_from_code(value: Any) -> str:
    text = str(value).strip().upper()
    code = normalize_code(text)
    if any(marker in text for marker in ("XSHG", "SSE", "SHSE", ".SH")):
        return "SSE"
    if any(marker in text for marker in ("XSHE", "SZSE", ".SZ")):
        return "SZSE"
    if code.startswith(("5", "6", "9")):
        return "SSE"
    if code.startswith(("0", "1", "2", "3")):
        return "SZSE"
    raise ValueError(f"cannot resolve A-share exchange: {value!r}")


def _find_column(frame: pd.DataFrame, names: Iterable[str]) -> str:
    for name in names:
        if name in frame.columns:
            return name
    raise ValueError(
        f"none of the required columns are present: {list(names)}; "
        f"observed={list(frame.columns)}"
    )


def _nonnegative_number(value: Any) -> float:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return 0.0
    result = float(numeric)
    if not math.isfinite(result) or result < 0:
        raise ValueError(f"corporate-action value is invalid: {value!r}")
    return result


def _normalize_master_part(
    frame: pd.DataFrame,
    *,
    exchange: str,
    active: bool,
) -> pd.DataFrame:
    code_column = _find_column(
        frame,
        ("证券代码", "A股代码", "公司代码", "code"),
    )
    name_column = _find_column(
        frame,
        ("证券简称", "A股简称", "公司简称", "name"),
    )
    listing_column = _find_column(
        frame,
        ("上市日期", "A股上市日期", "listing_date"),
    )
    result = pd.DataFrame(
        {
            "code": frame[code_column].map(normalize_code),
            "exchange": exchange,
            "security_type": "A_SHARE",
            "name": frame[name_column].astype(str),
            "listing_date": pd.to_datetime(
                frame[listing_column], errors="coerce"
            ).dt.normalize(),
            "delisting_date": pd.NaT,
            "source_status": "ACTIVE" if active else "DELISTED",
        }
    )
    if not active:
        delisting_column = _find_column(
            frame,
            ("终止上市日期", "暂停上市日期", "delisting_date"),
        )
        result["delisting_date"] = pd.to_datetime(
            frame[delisting_column], errors="coerce"
        ).dt.normalize()
    if result["listing_date"].isna().any():
        codes = result.loc[result["listing_date"].isna(), "code"].tolist()
        raise ValueError(f"security master has missing listing dates: {codes[:5]}")
    if not active and result["delisting_date"].isna().any():
        codes = result.loc[result["delisting_date"].isna(), "code"].tolist()
        raise ValueError(
            f"delisted security master has missing delisting dates: {codes[:5]}"
        )
    return result


def normalize_security_master(parts: Iterable[pd.DataFrame]) -> pd.DataFrame:
    combined = pd.concat(list(parts), ignore_index=True)
    rows: list[dict[str, Any]] = []
    for code, group in combined.groupby("code", sort=True):
        exchanges = set(group["exchange"].astype(str))
        if len(exchanges) != 1:
            raise ValueError(f"security master exchange conflict: {code}")
        listing_date = group["listing_date"].min()
        delisting_dates = group["delisting_date"].dropna()
        rows.append(
            {
                "code": str(code),
                "exchange": exchanges.pop(),
                "security_type": "A_SHARE",
                "name": str(group.iloc[-1]["name"]),
                "listing_date": listing_date,
                "delisting_date": (
                    delisting_dates.max()
                    if not delisting_dates.empty
                    else pd.NaT
                ),
                "source_status": (
                    "DELISTED"
                    if not delisting_dates.empty
                    else "ACTIVE"
                ),
            }
        )
    result = pd.DataFrame(rows).sort_values("code").reset_index(drop=True)
    if result["code"].duplicated().any():
        raise RuntimeError("security master normalization produced duplicates")
    return result


def _release_manifest(release_root: Path) -> dict[str, Any]:
    path = release_root / "development_only_release_manifest.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    if str(payload.get("data_role") or "") != "development":
        raise ValueError("release is not development-only")
    if bool(payload.get("forward_2026_present")):
        raise PermissionError("release contains forbidden 2026 data")
    date_min = str(payload.get("allowed_dates", {}).get("min") or "")
    date_max = str(payload.get("allowed_dates", {}).get("max") or "")
    if not date_min or not date_max or date_max >= "2026-01-01":
        raise PermissionError("release date boundary is not development-only")
    panels = sorted(release_root.glob(MINUTE_PATTERN))
    if not panels:
        raise FileNotFoundError(f"no minute panels under {release_root}")
    return {
        "path": path,
        "sha256": _sha256(path),
        "date_min": date_min,
        "date_max": date_max,
        "panels": panels,
    }


def release_codes(release_root: Path) -> list[str]:
    release = _release_manifest(release_root)
    codes: set[str] = set()
    for path in release["panels"]:
        parquet = pq.ParquetFile(path)
        if "code" not in parquet.schema_arrow.names:
            raise ValueError(f"minute panel lacks code: {path}")
        for row_group in range(parquet.num_row_groups):
            values = parquet.read_row_group(
                row_group, columns=["code"]
            ).column("code")
            codes.update(normalize_code(value.as_py()) for value in values.unique())
    return sorted(codes)


def _raw_cninfo_dividend_request(code: str, *, enckey: str) -> dict[str, Any]:
    import requests

    url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1139"
    headers = {
        "Accept": "*/*",
        "Accept-Enckey": enckey,
        "Origin": "http://webapi.cninfo.com.cn",
        "Referer": "http://webapi.cninfo.com.cn/",
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
    }
    response = requests.post(
        url,
        params={"scode": code},
        headers=headers,
        timeout=45,
    )
    response.raise_for_status()
    payload = response.json()
    records = payload.get("records")
    if not isinstance(records, list):
        raise ValueError(f"CNInfo response lacks records for {code}")
    return {
        "schema_version": "cninfo_sysapi1139_raw_v1",
        "code": code,
        "endpoint": url,
        "records": records,
    }


def _cninfo_enckey() -> str:
    import py_mini_racer
    from akshare.datasets import get_ths_js

    js_path = get_ths_js("cninfo.js")
    js = Path(js_path).read_text(encoding="utf-8")
    runtime = py_mini_racer.MiniRacer()
    runtime.eval(js)
    return str(runtime.call("getResCode1"))


def _fetch_dividend_with_retry(
    code: str,
    *,
    enckey: str,
    attempts: int = 4,
) -> dict[str, Any]:
    errors: list[str] = []
    for attempt in range(1, attempts + 1):
        try:
            return _raw_cninfo_dividend_request(code, enckey=enckey)
        except Exception as exc:  # public endpoint errors need bounded retry
            errors.append(f"{type(exc).__name__}:{exc}")
            if attempt < attempts:
                time.sleep(min(8.0, float(2 ** (attempt - 1))))
    raise RuntimeError(f"CNInfo fetch failed for {code}: {errors}")


def fetch_public_sources(
    *,
    release_root: Path,
    source_root: Path,
    dividend_workers: int = 8,
) -> dict[str, Any]:
    """Fetch or resume the zero-financial public source snapshot."""

    release_root = release_root.resolve()
    source_root = source_root.resolve()
    source_root.mkdir(parents=True, exist_ok=True)
    complete_path = source_root / "source_snapshot_manifest.json"
    if complete_path.is_file():
        return verify_source_snapshot(source_root)

    release = _release_manifest(release_root)
    codes = release_codes(release_root)
    import akshare as ak

    master_path = source_root / "security_master.parquet"
    calendar_path = source_root / "trade_calendar.parquet"
    if not master_path.is_file():
        parts = [
            _normalize_master_part(
                ak.stock_info_sh_name_code(symbol="主板A股"),
                exchange="SSE",
                active=True,
            ),
            _normalize_master_part(
                ak.stock_info_sh_name_code(symbol="科创板"),
                exchange="SSE",
                active=True,
            ),
            _normalize_master_part(
                ak.stock_info_sz_name_code(symbol="A股列表"),
                exchange="SZSE",
                active=True,
            ),
            _normalize_master_part(
                ak.stock_info_sh_delist(symbol="全部"),
                exchange="SSE",
                active=False,
            ),
            _normalize_master_part(
                ak.stock_info_sz_delist(symbol="终止上市公司"),
                exchange="SZSE",
                active=False,
            ),
        ]
        normalize_security_master(parts).to_parquet(master_path, index=False)
    if not calendar_path.is_file():
        calendar = ak.tool_trade_date_hist_sina()
        column = _find_column(calendar, ("trade_date", "交易日", "date"))
        dates = pd.to_datetime(calendar[column], errors="raise").dt.normalize()
        pd.DataFrame({"date": sorted(dates.unique())}).to_parquet(
            calendar_path, index=False
        )

    dividend_root = source_root / "cninfo_dividend_raw"
    dividend_root.mkdir(exist_ok=True)
    pending = [
        code
        for code in codes
        if not (dividend_root / f"{code}.json").is_file()
    ]
    if pending:
        enckey = _cninfo_enckey()
        failures: dict[str, str] = {}
        completed = len(codes) - len(pending)
        with ThreadPoolExecutor(max_workers=max(1, dividend_workers)) as pool:
            futures = {
                pool.submit(
                    _fetch_dividend_with_retry, code, enckey=enckey
                ): code
                for code in pending
            }
            for future in as_completed(futures):
                code = futures[future]
                try:
                    payload = future.result()
                    _write_json(dividend_root / f"{code}.json", payload)
                    completed += 1
                except Exception as exc:
                    failures[code] = f"{type(exc).__name__}:{exc}"
                if (completed + len(failures)) % 25 == 0:
                    _write_json(
                        source_root / "source_snapshot_progress.json",
                        {
                            "schema_version": "cn_finalist_source_progress_v1",
                            "total_codes": len(codes),
                            "completed_codes": completed,
                            "failure_count": len(failures),
                            "failures": failures,
                        },
                    )
        if failures:
            raise RuntimeError(
                f"public source snapshot has {len(failures)} failures; "
                "rerun the same command to resume"
            )
    observed_dividend_codes = {
        path.stem for path in dividend_root.glob("*.json")
    }
    expected_dividend_codes = set(codes)
    if observed_dividend_codes != expected_dividend_codes:
        missing = sorted(expected_dividend_codes - observed_dividend_codes)
        extra = sorted(observed_dividend_codes - expected_dividend_codes)
        raise ValueError(
            "dividend snapshot code set differs from release: "
            f"missing={missing[:10]}, extra={extra[:10]}"
        )

    artifacts = []
    for path in (
        master_path,
        calendar_path,
        *sorted(dividend_root.glob("*.json")),
    ):
        artifacts.append(
            {
                "path": path.relative_to(source_root).as_posix(),
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
        )
    manifest = {
        "schema_version": SOURCE_SCHEMA_VERSION,
        "status": "SOURCE_SNAPSHOT_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "data_role": "development",
        "date_min": release["date_min"],
        "date_max": release["date_max"],
        "release_manifest": str(release["path"]),
        "release_manifest_sha256": release["sha256"],
        "release_code_count": len(codes),
        "dividend_code_count": len(
            list(dividend_root.glob("*.json"))
        ),
        "source_references": {
            "sse_active": "https://www.sse.com.cn/assortment/stock/list/share/",
            "sse_delisted": "https://www.sse.com.cn/assortment/stock/list/delisting/",
            "szse_active": "https://www.szse.cn/market/product/stock/list/index.html",
            "szse_delisted": "https://www.szse.cn/market/stock/suspend/index.html",
            "trade_calendar": "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt",
            "corporate_actions": "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1139",
        },
        "artifacts": artifacts,
        "financial_reads": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    _write_json(complete_path, manifest)
    progress_path = source_root / "source_snapshot_progress.json"
    if progress_path.exists():
        progress_path.unlink()
    return verify_source_snapshot(source_root)


def verify_source_snapshot(source_root: Path) -> dict[str, Any]:
    source_root = source_root.resolve()
    path = source_root / "source_snapshot_manifest.json"
    manifest = json.loads(path.read_text(encoding="utf-8"))
    _verify_self_hash(manifest, "manifest_payload_sha256")
    if manifest.get("status") != "SOURCE_SNAPSHOT_CLOSED_IMMUTABLE":
        raise ValueError("public source snapshot is not immutable")
    if manifest.get("data_role") != "development":
        raise ValueError("public source snapshot is not development-only")
    for field in (
        "financial_reads",
        "validation_reads",
        "holdout_reads",
        "forward_2026_reads",
    ):
        if int(manifest.get(field, -1)) != 0:
            raise ValueError(f"source snapshot {field} must remain zero")
    for artifact in manifest.get("artifacts") or []:
        artifact_path = source_root / str(artifact["path"])
        if not artifact_path.is_file():
            raise FileNotFoundError(artifact_path)
        if artifact_path.stat().st_size != int(artifact["bytes"]):
            raise ValueError(f"source artifact size mismatch: {artifact_path}")
        if _sha256(artifact_path) != str(artifact["sha256"]):
            raise ValueError(f"source artifact hash mismatch: {artifact_path}")
    return {
        "status": manifest["status"],
        "source_root": str(source_root),
        "manifest": str(path),
        "manifest_file_sha256": _sha256(path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "date_min": str(manifest["date_min"]),
        "date_max": str(manifest["date_max"]),
        "release_code_count": int(manifest["release_code_count"]),
        "artifact_count": len(manifest.get("artifacts") or []),
    }


def _read_observed_sessions(release_root: Path) -> pd.DataFrame:
    release = _release_manifest(release_root)
    chunks: list[pd.DataFrame] = []
    for path in release["panels"]:
        parquet = pq.ParquetFile(path)
        names = set(parquet.schema_arrow.names)
        st_column = (
            "is_st"
            if "is_st" in names
            else "ctx_hfq_is_st"
            if "ctx_hfq_is_st" in names
            else None
        )
        required = {"code", "trade_time", "open", "high", "low", "close"}
        missing = sorted(required - names)
        if missing or st_column is None:
            raise ValueError(
                f"minute panel lacks session authority inputs: {path}; "
                f"missing={missing}, st_column={st_column}"
            )
        columns = list(required) + [st_column]
        optional_limits = [
            column
            for column in ("up_limit_price", "down_limit_price")
            if column in names
        ]
        columns.extend(optional_limits)
        for row_group in range(parquet.num_row_groups):
            frame = parquet.read_row_group(
                row_group, columns=columns
            ).to_pandas()
            frame["code"] = frame["code"].map(normalize_code)
            frame["trade_time"] = pd.to_datetime(
                frame["trade_time"], errors="raise"
            )
            frame["date"] = frame["trade_time"].dt.normalize()
            frame = frame.sort_values(
                ["code", "trade_time"], kind="mergesort"
            )
            aggregations: dict[str, tuple[str, str]] = {
                "open": ("open", "first"),
                "high": ("high", "max"),
                "low": ("low", "min"),
                "close": ("close", "last"),
                "is_st": (st_column, "last"),
            }
            for column in optional_limits:
                aggregations[column] = (column, "last")
            chunks.append(
                frame.groupby(
                    ["code", "date"], as_index=False, sort=False
                ).agg(**aggregations)
            )
    combined = pd.concat(chunks, ignore_index=True)
    combined = combined.sort_values(
        ["code", "date"], kind="mergesort"
    )
    aggregations = {
        "open": ("open", "first"),
        "high": ("high", "max"),
        "low": ("low", "min"),
        "close": ("close", "last"),
        "is_st": ("is_st", "last"),
    }
    for column in ("up_limit_price", "down_limit_price"):
        if column in combined:
            aggregations[column] = (column, "last")
    result = combined.groupby(
        ["code", "date"], as_index=False, sort=False
    ).agg(**aggregations)
    if result.duplicated(["code", "date"]).any():
        raise RuntimeError("observed daily collapse produced duplicates")
    return result


def parse_dividend_actions(
    raw_payloads: Iterable[Mapping[str, Any]],
    *,
    date_min: str,
    date_max: str,
) -> tuple[pd.DataFrame, list[str]]:
    rows: list[dict[str, Any]] = []
    blockers: list[str] = []
    lower = pd.Timestamp(date_min).normalize()
    upper = pd.Timestamp(date_max).normalize()
    for payload in raw_payloads:
        code = normalize_code(payload["code"])
        for record in payload.get("records") or []:
            announcement = pd.to_datetime(
                record.get(CNINFO_DIVIDEND_KEYS["announcement_date"]),
                errors="coerce",
            )
            effective = pd.to_datetime(
                record.get(CNINFO_DIVIDEND_KEYS["effective_date"]),
                errors="coerce",
            )
            payment = pd.to_datetime(
                record.get(CNINFO_DIVIDEND_KEYS["cash_payment_date"]),
                errors="coerce",
            )
            try:
                bonus = _nonnegative_number(
                    record.get(CNINFO_DIVIDEND_KEYS["bonus_shares_per_10"])
                )
                transfer = _nonnegative_number(
                    record.get(CNINFO_DIVIDEND_KEYS["transfer_shares_per_10"])
                )
                cash = _nonnegative_number(
                    record.get(CNINFO_DIVIDEND_KEYS["cash_per_10"])
                )
            except ValueError:
                blockers.append(f"invalid_corporate_action_value:{code}")
                continue
            relevant_hint = any(
                not pd.isna(value) and lower <= value.normalize() <= upper
                for value in (announcement, effective, payment)
            )
            if cash > 0 and pd.isna(payment) and relevant_hint:
                blockers.append(f"cash_action_missing_payment_date:{code}")
            if bonus + transfer > 0 and pd.isna(effective) and relevant_hint:
                blockers.append(f"share_action_missing_effective_date:{code}")
            if cash > 0 and not pd.isna(payment):
                event_date = payment.normalize()
                if lower <= event_date <= upper:
                    rows.append(
                        {
                            "code": code,
                            "date": event_date,
                            "cash_per_share": cash / 10.0,
                            "share_multiplier": 1.0,
                        }
                    )
            if bonus + transfer > 0 and not pd.isna(effective):
                event_date = effective.normalize()
                if lower <= event_date <= upper:
                    rows.append(
                        {
                            "code": code,
                            "date": event_date,
                            "cash_per_share": 0.0,
                            "share_multiplier": 1.0
                            + (bonus + transfer) / 10.0,
                        }
                    )
    if not rows:
        return pd.DataFrame(
            columns=[
                "code",
                "date",
                "cash_per_share",
                "share_multiplier",
            ]
        ), sorted(set(blockers))
    actions = pd.DataFrame(rows)
    actions = actions.groupby(
        ["code", "date"], as_index=False, sort=True
    ).agg(
        cash_per_share=("cash_per_share", "sum"),
        share_multiplier=("share_multiplier", "prod"),
    )
    return actions, sorted(set(blockers))


def _board_limit_pct(code: pd.Series) -> pd.Series:
    pct = pd.Series(np.nan, index=code.index, dtype=float)
    pct.loc[code.str.startswith(("300", "301", "688", "689"))] = 0.20
    pct.loc[code.str.startswith(("0", "1", "2", "5", "6")) & pct.isna()] = 0.10
    return pct


def _round_tick(values: pd.Series, tick: float = 0.01) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    rounded = np.floor(numeric / tick + 0.5 + 1e-10) * tick
    return pd.Series(rounded, index=values.index, dtype=float)


def materialize_session_authority(
    *,
    observed: pd.DataFrame,
    security_master: pd.DataFrame,
    trade_calendar: pd.Series,
    actions: pd.DataFrame,
    date_min: str,
    date_max: str,
) -> pd.DataFrame:
    """Build explicit listed-session rows from development observations."""

    lower = pd.Timestamp(date_min).normalize()
    upper = pd.Timestamp(date_max).normalize()
    full_calendar = pd.DatetimeIndex(
        pd.to_datetime(trade_calendar, errors="raise")
    ).normalize()
    full_calendar = full_calendar.unique().sort_values()
    calendar = full_calendar[
        (full_calendar >= lower) & (full_calendar <= upper)
    ]
    calendar = calendar.sort_values()
    if calendar.empty:
        raise ValueError("trade calendar does not cover the release")
    master = security_master.copy()
    master["code"] = master["code"].map(normalize_code)
    master["listing_date"] = pd.to_datetime(
        master["listing_date"], errors="raise"
    ).dt.normalize()
    master["delisting_date"] = pd.to_datetime(
        master["delisting_date"], errors="coerce"
    ).dt.normalize()
    master = master.set_index("code", drop=False)
    observed = observed.copy()
    observed["code"] = observed["code"].map(normalize_code)
    observed["date"] = pd.to_datetime(
        observed["date"], errors="raise"
    ).dt.normalize()
    missing_codes = sorted(set(observed["code"]) - set(master.index))
    if missing_codes:
        raise ValueError(
            f"release codes absent from exchange security master: "
            f"{missing_codes[:20]}"
        )

    grid_parts: list[pd.DataFrame] = []
    terminal_dates: dict[str, pd.Timestamp] = {}
    for code in sorted(observed["code"].unique()):
        row = master.loc[code]
        listing_date = pd.Timestamp(row["listing_date"]).normalize()
        delisting_date = (
            pd.Timestamp(row["delisting_date"]).normalize()
            if not pd.isna(row["delisting_date"])
            else None
        )
        start = max(lower, listing_date)
        end = min(upper, delisting_date) if delisting_date is not None else upper
        dates = calendar[(calendar >= start) & (calendar <= end)]
        if dates.empty:
            raise ValueError(f"no listed sessions for release code {code}")
        if delisting_date is not None and lower <= delisting_date <= upper:
            terminal_dates[code] = pd.Timestamp(dates[-1])
        listing_ordinal = int(
            full_calendar.searchsorted(listing_date, side="left")
        )
        ordinals = np.arange(
            int(full_calendar.searchsorted(dates[0], side="left")),
            int(full_calendar.searchsorted(dates[-1], side="right")),
        )
        grid_parts.append(
            pd.DataFrame(
                {
                    "code": code,
                    "date": dates,
                    "exchange": str(row["exchange"]),
                    "security_type": "A_SHARE",
                    "listing_age_sessions": ordinals - listing_ordinal + 1,
                }
            )
        )
    grid = pd.concat(grid_parts, ignore_index=True)
    grid = grid.merge(
        observed,
        on=["code", "date"],
        how="left",
        validate="one_to_one",
    )
    grid["suspended"] = grid["open"].isna()
    grid["universe_eligible"] = True
    grid["is_st"] = pd.to_numeric(
        grid["is_st"], errors="coerce"
    ).groupby(grid["code"], sort=False).ffill()
    if grid["is_st"].isna().any():
        missing = grid.loc[grid["is_st"].isna(), "code"].unique().tolist()
        raise ValueError(
            "PIT ST state is unavailable before first explicit observation: "
            f"{missing[:20]}"
        )
    grid["is_st"] = grid["is_st"].ne(0)
    grid["close"] = pd.to_numeric(
        grid["close"], errors="coerce"
    ).groupby(grid["code"], sort=False).ffill()
    previous_close = grid.groupby("code", sort=False)["close"].shift(1)
    pct = _board_limit_pct(grid["code"]).where(~grid["is_st"], 0.05)
    eligible = (
        grid["listing_age_sessions"].gt(5)
        & previous_close.gt(0)
        & pct.notna()
    )
    derived_up = _round_tick(previous_close * (1.0 + pct))
    derived_down = _round_tick(previous_close * (1.0 - pct))
    existing_up = pd.to_numeric(
        grid.get("up_limit_price"), errors="coerce"
    )
    existing_down = pd.to_numeric(
        grid.get("down_limit_price"), errors="coerce"
    )
    if not isinstance(existing_up, pd.Series):
        existing_up = pd.Series(np.nan, index=grid.index)
    if not isinstance(existing_down, pd.Series):
        existing_down = pd.Series(np.nan, index=grid.index)
    grid["up_limit_price"] = existing_up.where(
        existing_up.gt(0), derived_up.where(eligible)
    )
    grid["down_limit_price"] = existing_down.where(
        existing_down.gt(0), derived_down.where(eligible)
    )

    if actions.empty:
        action_frame = pd.DataFrame(
            columns=[
                "code",
                "date",
                "cash_per_share",
                "share_multiplier",
            ]
        )
    else:
        action_frame = actions.copy()
        action_frame["code"] = action_frame["code"].map(normalize_code)
        action_frame["date"] = pd.to_datetime(
            action_frame["date"], errors="raise"
        ).dt.normalize()
        relevant_actions = action_frame.loc[
            action_frame["code"].isin(observed["code"].unique())
            & action_frame["date"].between(lower, upper)
        ]
        grid_coordinates = pd.MultiIndex.from_frame(grid[["code", "date"]])
        action_coordinates = pd.MultiIndex.from_frame(
            relevant_actions[["code", "date"]]
        )
        unmatched = action_coordinates.difference(grid_coordinates)
        if len(unmatched):
            raise ValueError(
                "corporate action does not map to a listed exchange session: "
                f"{list(unmatched[:10])}"
            )
    grid = grid.merge(
        action_frame,
        on=["code", "date"],
        how="left",
        validate="one_to_one",
    )
    grid["corporate_action_cash_per_share"] = pd.to_numeric(
        grid["cash_per_share"], errors="coerce"
    ).fillna(0.0)
    grid["corporate_action_share_multiplier"] = pd.to_numeric(
        grid["share_multiplier"], errors="coerce"
    ).fillna(1.0)
    grid["is_terminal_session"] = False
    for code, date in terminal_dates.items():
        mask = grid["code"].eq(code) & grid["date"].eq(date)
        if int(mask.sum()) != 1:
            raise RuntimeError(f"terminal session coordinate missing: {code}")
        grid.loc[mask, "is_terminal_session"] = True
    grid["is_delisting"] = grid["is_terminal_session"]
    grid["terminal_liquidation_price"] = np.nan
    grid.loc[
        grid["is_terminal_session"], "terminal_liquidation_price"
    ] = 0.0
    result = grid.loc[:, SESSION_AUTHORITY_COLUMNS].sort_values(
        ["date", "code"], kind="mergesort"
    ).reset_index(drop=True)
    if result.duplicated(["date", "code"]).any():
        raise RuntimeError("session authority produced duplicate coordinates")
    terminal = result["is_terminal_session"]
    if (
        terminal
        & (
            result["terminal_liquidation_price"].isna()
            | result["terminal_liquidation_price"].lt(0)
        )
    ).any():
        raise RuntimeError("terminal recovery is not explicit and nonnegative")
    return result


def _artifact(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def build_authority(
    *,
    release_root: Path,
    source_root: Path,
    output_root: Path,
) -> dict[str, Any]:
    release_root = release_root.resolve()
    source_root = source_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    release = _release_manifest(release_root)
    source = verify_source_snapshot(source_root)
    if source["date_min"] != release["date_min"]:
        raise ValueError("source/release date_min mismatch")
    if source["date_max"] != release["date_max"]:
        raise ValueError("source/release date_max mismatch")

    observed = _read_observed_sessions(release_root)
    master = pd.read_parquet(source_root / "security_master.parquet")
    calendar = pd.read_parquet(source_root / "trade_calendar.parquet")["date"]
    raw_payloads = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((source_root / "cninfo_dividend_raw").glob("*.json"))
    ]
    actions, action_blockers = parse_dividend_actions(
        raw_payloads,
        date_min=release["date_min"],
        date_max=release["date_max"],
    )
    if action_blockers:
        raise ValueError(
            "corporate-action source is incomplete: "
            + ", ".join(action_blockers[:20])
        )
    authority = materialize_session_authority(
        observed=observed,
        security_master=master,
        trade_calendar=calendar,
        actions=actions,
        date_min=release["date_min"],
        date_max=release["date_max"],
    )
    sidecar_path = output_root / "session_authority.parquet"
    authority.to_parquet(sidecar_path, index=False)

    universe = {
        "schema_version": "cn_a_share_pit_universe_manifest_v1",
        "pit_membership": True,
        "survivorship_free": True,
        "delisting_history_included": True,
        "date_min": release["date_min"],
        "date_max": release["date_max"],
        "security_count": int(authority["code"].nunique()),
        "session_row_count": len(authority),
        "source_reference": (
            "SSE/SZSE active and delisted security masters plus full "
            "exchange trade calendar; PIT ST state from frozen development "
            "minute release"
        ),
        "source_snapshot_manifest": source["manifest"],
        "source_snapshot_manifest_sha256": source["manifest_file_sha256"],
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    universe_path = _write_json(
        output_root / "universe_manifest.json", universe
    )
    fee = {
        "schema_version": "a_share_fee_contract_v2",
        "contract_mode": "CONSERVATIVE_RESEARCH_UPPER_BOUND_NON_PROMOTION",
        "account_contract_confirmed": False,
        "research_upper_bound_confirmed": True,
        "promotion_authorized": False,
        "fee_schedule": {
            "commission_bps": 3.0,
            "minimum_commission_cny": 5.0,
            "exchange_handling_bps": 0.541,
            "transfer_fee_bps": 0.1,
            "sell_stamp_duty_bps": 5.0,
            "effective_start": "2023-08-28",
            "effective_end": "2025-12-31",
            "source_reference": "; ".join(FEE_SOURCE_REFERENCES),
        },
        "interpretation": (
            "Research-only conservative upper bound. It is not an actual "
            "broker account contract and cannot authorize promotion."
        ),
    }
    fee_path = _write_json(output_root / "fee_contract.json", fee)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": "SESSION_AUTHORITY_CLOSED_IMMUTABLE",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "existing_authority": "PHASE3DY_A_SHARE_TRADABILITY_REPLAY",
        "new_authority_node_created": False,
        "data_role": "development",
        "date_min": release["date_min"],
        "date_max": release["date_max"],
        "security_count": int(authority["code"].nunique()),
        "session_row_count": len(authority),
        "observed_session_row_count": len(observed),
        "suspended_session_row_count": int(authority["suspended"].sum()),
        "terminal_session_row_count": int(
            authority["is_terminal_session"].sum()
        ),
        "corporate_cash_event_count": int(
            authority["corporate_action_cash_per_share"].gt(0).sum()
        ),
        "corporate_share_event_count": int(
            authority["corporate_action_share_multiplier"].ne(1.0).sum()
        ),
        "columns": list(SESSION_AUTHORITY_COLUMNS),
        "policies": {
            "suspension": "LISTED_CALENDAR_MINUS_MINUTE_OBSERVATION",
            "st": "PIT_MINUTE_STATE_FORWARD_FILLED_ONLY_ACROSS_SUSPENSION",
            "limits": "CN_CONSERVATIVE_LIMIT_LIFECYCLE_V2_EQUIVALENT_SESSION_RULES",
            "cash_actions": "CNINFO_F012N_PER_10_ON_F023D_PAYMENT_SESSION",
            "share_actions": "CNINFO_F010N_PLUS_F011N_ON_F020D_EFFECTIVE_SESSION",
            "delisting": "EXPLICIT_TERMINAL_SESSION_ZERO_RECOVERY_FAIL_CLOSED",
        },
        "source_snapshot_manifest": source["manifest"],
        "source_snapshot_manifest_sha256": source["manifest_file_sha256"],
        "release_manifest": str(release["path"]),
        "release_manifest_sha256": release["sha256"],
        "session_authority_path": str(sidecar_path),
        "universe_manifest_path": str(universe_path),
        "fee_contract_path": str(fee_path),
        "artifacts": [
            _artifact(sidecar_path, output_root),
            _artifact(universe_path, output_root),
            _artifact(fee_path, output_root),
        ],
        "financial_reads": 0,
        "financial_result_recomputed": False,
        "optimizer_feedback_writes": 0,
        "scheduler_writes": 0,
        "archive_writes": 0,
        "validation_reads": 0,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    manifest["manifest_payload_sha256"] = _payload_sha256(manifest)
    manifest_path = _write_json(
        output_root / "session_authority_manifest.json", manifest
    )
    result = {
        "status": manifest["status"],
        "output_root": str(output_root),
        "manifest": str(manifest_path),
        "manifest_file_sha256": _sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "security_count": manifest["security_count"],
        "session_row_count": manifest["session_row_count"],
        "suspended_session_row_count": manifest[
            "suspended_session_row_count"
        ],
        "terminal_session_row_count": manifest[
            "terminal_session_row_count"
        ],
        "financial_result_recomputed": False,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    fetch = subparsers.add_parser("fetch")
    fetch.add_argument("--release-root", type=Path, required=True)
    fetch.add_argument("--source-root", type=Path, required=True)
    fetch.add_argument("--dividend-workers", type=int, default=8)
    materialize = subparsers.add_parser("materialize")
    materialize.add_argument("--release-root", type=Path, required=True)
    materialize.add_argument("--source-root", type=Path, required=True)
    materialize.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "fetch":
        result = fetch_public_sources(
            release_root=args.release_root,
            source_root=args.source_root,
            dividend_workers=args.dividend_workers,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        build_authority(
            release_root=args.release_root,
            source_root=args.source_root,
            output_root=args.output_root,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
