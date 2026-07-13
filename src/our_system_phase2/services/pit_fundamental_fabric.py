"""PIT-safe access to versioned CN fundamental sidecars.

The source archive is a current snapshot, not a historical revision tape.  A
row is therefore never exposed at its report period and is not exposed at the
initial notice date when the snapshot says it was updated later.  The current
value becomes eligible only after both NOTICE_DATE and UPDATE_DATE are known,
using the later date, and after the next declared trading session starts.

This module contains no reward, ranking, selection, or candidate-promotion
logic.  It is a typed data-access layer only.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd
import pyarrow.parquet as pq


FABRIC_VERSION = "CN_PIT_FUNDAMENTAL_FABRIC_V1"
FINANCIAL_TABLES = {
    "balance_sheet_report_em",
    "profit_sheet_report_em",
    "cash_flow_sheet_report_em",
}
PIT_UNRESOLVED_TABLES = {"zygc_em"}


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(payload: Any) -> str:
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def normalize_cn_code(value: Any) -> str:
    digits = "".join(re.findall(r"\d", str(value or "")))
    return digits[-6:].zfill(6) if digits else ""


def exchange_prefix(code: str) -> str:
    compact = normalize_cn_code(code)
    if compact.startswith(("4", "8", "92")):
        return "BJ"
    if compact.startswith(("5", "6", "9")):
        return "SH"
    return "SZ"


def source_partition_path(source_root: Path, table: str, code: Any) -> Path:
    compact = normalize_cn_code(code)
    root = Path(source_root) / table
    candidates = [
        root / f"{exchange_prefix(compact)}{compact}.parquet",
        root / f"{compact}.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path
    return candidates[0]


def load_development_sessions(split_manifest: Path) -> pd.DatetimeIndex:
    frame = pd.read_csv(split_manifest, usecols=["trade_date", "split"])
    if set(frame["split"].astype(str).str.lower()) - {"train", "validation", "holdout"}:
        raise ValueError("unexpected split role in fixed calendar manifest")
    allowed = frame.loc[frame["split"].astype(str).str.lower().eq("train"), "trade_date"]
    sessions = pd.DatetimeIndex(pd.to_datetime(allowed, errors="raise").sort_values().unique())
    if sessions.empty:
        raise ValueError("fixed split manifest contains no development/train sessions")
    return sessions


def next_session_open(
    source_dates: pd.Series,
    sessions: Sequence[pd.Timestamp] | pd.DatetimeIndex,
    *,
    open_time: str = "09:30:00",
) -> pd.Series:
    """Map date-only source timestamps to the next declared session open.

    The mapping is strictly later than the source calendar date.  A disclosure
    with no time-of-day is never eligible during that same session.
    """

    calendar = pd.DatetimeIndex(pd.to_datetime(list(sessions), errors="raise")).normalize()
    if calendar.empty or not calendar.is_monotonic_increasing:
        raise ValueError("sessions must be a non-empty increasing calendar")
    source = pd.to_datetime(source_dates, errors="coerce").dt.normalize()
    positions = calendar.searchsorted(source.to_numpy(dtype="datetime64[ns]"), side="right")
    out = pd.Series(pd.NaT, index=source.index, dtype="datetime64[ns]")
    valid = source.notna().to_numpy() & (positions < len(calendar))
    if valid.any():
        offset = pd.to_timedelta(open_time)
        out.iloc[np.flatnonzero(valid)] = calendar.take(positions[valid]) + offset
    return out


def conservative_financial_versions(
    frame: pd.DataFrame,
    *,
    table: str,
    sessions: Sequence[pd.Timestamp] | pd.DatetimeIndex,
    maximum_observable_time: pd.Timestamp | str | None = None,
) -> pd.DataFrame:
    """Attach safe version clocks to a current-snapshot financial table.

    Missing NOTICE_DATE, UPDATE_DATE, REPORT_DATE, or code fails closed.  The
    function intentionally does not fall back to REPORT_DATE plus a guessed
    lag and does not pretend to reconstruct superseded values.
    """

    if table not in FINANCIAL_TABLES:
        raise ValueError(f"not a financial statement table: {table}")
    required = {"NOTICE_DATE", "UPDATE_DATE", "REPORT_DATE"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"missing financial PIT columns: {sorted(missing)}")
    out = frame.copy()
    code_source = out["source_code6"] if "source_code6" in out else out.get("SECURITY_CODE")
    if code_source is None:
        raise ValueError("financial frame lacks source_code6/SECURITY_CODE")
    out["code"] = pd.Series(code_source, index=out.index).map(normalize_cn_code)
    out["report_period"] = pd.to_datetime(out["REPORT_DATE"], errors="coerce").dt.normalize()
    out["notice_date"] = pd.to_datetime(out["NOTICE_DATE"], errors="coerce").dt.normalize()
    out["revision_date"] = pd.to_datetime(out["UPDATE_DATE"], errors="coerce").dt.normalize()
    out["safe_source_date"] = out[["notice_date", "revision_date"]].max(axis=1, skipna=False)
    out["observable_time"] = next_session_open(out["safe_source_date"], sessions)
    out["pit_status"] = "ELIGIBLE_CURRENT_SNAPSHOT_VERSION"
    unresolved = (
        out["code"].eq("")
        | out["report_period"].isna()
        | out["notice_date"].isna()
        | out["revision_date"].isna()
        | out["observable_time"].isna()
    )
    out.loc[unresolved, "pit_status"] = "PIT_CONTRACT_UNRESOLVED"
    if maximum_observable_time is not None:
        maximum = pd.Timestamp(maximum_observable_time)
        after = out["observable_time"].gt(maximum)
        out.loc[after, "pit_status"] = "OUTSIDE_DEVELOPMENT_ACCESS_BOUNDARY"
    out["version_kind"] = np.where(
        out["revision_date"].eq(out["notice_date"]),
        "INITIAL_UNCHANGED_CURRENT_VERSION",
        "LATEST_REVISED_SNAPSHOT_ONLY",
    )
    calendar_start = pd.DatetimeIndex(pd.to_datetime(list(sessions), errors="raise")).min().normalize()
    out["disclosure_event_eligible"] = (
        out["pit_status"].eq("ELIGIBLE_CURRENT_SNAPSHOT_VERSION")
        & out["version_kind"].eq("INITIAL_UNCHANGED_CURRENT_VERSION")
        & out["safe_source_date"].ge(calendar_start)
    )
    out["version_id"] = [
        stable_hash(
            {
                "fabric": FABRIC_VERSION,
                "table": table,
                "code": code,
                "report_period": str(report_period),
                "safe_source_date": str(safe_source_date),
                "version_kind": version_kind,
            }
        )[:24]
        for code, report_period, safe_source_date, version_kind in zip(
            out["code"], out["report_period"], out["safe_source_date"], out["version_kind"]
        )
    ]
    return out


def conservative_holder_episodes(
    frame: pd.DataFrame,
    *,
    sessions: Sequence[pd.Timestamp] | pd.DatetimeIndex,
    maximum_observable_time: pd.Timestamp | str | None = None,
) -> pd.DataFrame:
    required = {"公告日期", "截至日期"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"missing holder PIT columns: {sorted(missing)}")
    out = frame.copy()
    code_source = out["source_code6"] if "source_code6" in out else out.get("股票代码")
    if code_source is None:
        raise ValueError("holder frame lacks source_code6/股票代码")
    out["code"] = pd.Series(code_source, index=out.index).map(normalize_cn_code)
    out["report_period"] = pd.to_datetime(out["截至日期"], errors="coerce").dt.normalize()
    out["notice_date"] = pd.to_datetime(out["公告日期"], errors="coerce").dt.normalize()
    out["revision_date"] = pd.NaT
    out["safe_source_date"] = out["notice_date"]
    out["observable_time"] = next_session_open(out["notice_date"], sessions)
    out["pit_status"] = "ELIGIBLE_DISCLOSURE_EPISODE"
    unresolved = out["code"].eq("") | out["report_period"].isna() | out["observable_time"].isna()
    out.loc[unresolved, "pit_status"] = "PIT_CONTRACT_UNRESOLVED"
    if maximum_observable_time is not None:
        maximum = pd.Timestamp(maximum_observable_time)
        out.loc[out["observable_time"].gt(maximum), "pit_status"] = "OUTSIDE_DEVELOPMENT_ACCESS_BOUNDARY"
    calendar_start = pd.DatetimeIndex(pd.to_datetime(list(sessions), errors="raise")).min().normalize()
    out["disclosure_event_eligible"] = (
        out["pit_status"].eq("ELIGIBLE_DISCLOSURE_EPISODE")
        & out["notice_date"].ge(calendar_start)
    )
    out["episode_id"] = [
        stable_hash(
            {
                "fabric": FABRIC_VERSION,
                "table": "main_stock_holder_sina",
                "code": code,
                "report_period": str(period),
                "notice_date": str(notice),
            }
        )[:24]
        for code, period, notice in zip(out["code"], out["report_period"], out["notice_date"])
    ]
    return out


@dataclass(frozen=True, slots=True)
class FundamentalFieldRequest:
    source_table: str
    source_field: str
    route: str = "FUNDAMENTAL_LEVEL"
    transform: str = "latest"

    @property
    def output_name(self) -> str:
        table_tag = {
            "balance_sheet_report_em": "bs",
            "profit_sheet_report_em": "ps",
            "cash_flow_sheet_report_em": "cf",
            "main_stock_holder_sina": "holder",
            "zygc_em": "zygc",
        }.get(self.source_table, self.source_table)
        field = re.sub(r"[^0-9A-Za-z_]+", "_", self.source_field).strip("_").lower()
        if not field:
            field = "field_" + hashlib.sha256(self.source_field.encode("utf-8")).hexdigest()[:12]
        return f"fund_{table_tag}_{field}_{self.transform}"


class StockSessionAsOfResolver:
    """Resolve latest eligible versions onto stock-session coordinates."""

    def __init__(self, *, maximum_observable_time: pd.Timestamp | str) -> None:
        self.maximum_observable_time = pd.Timestamp(maximum_observable_time)

    def resolve(
        self,
        versions: pd.DataFrame,
        coordinates: pd.DataFrame,
        *,
        value_fields: Sequence[str],
        coordinate_time_field: str = "session_time",
    ) -> pd.DataFrame:
        required_versions = {"code", "observable_time", "report_period", "pit_status", *value_fields}
        required_coords = {"code", coordinate_time_field}
        if missing := required_versions - set(versions.columns):
            raise ValueError(f"version frame missing columns: {sorted(missing)}")
        if missing := required_coords - set(coordinates.columns):
            raise ValueError(f"coordinate frame missing columns: {sorted(missing)}")
        left = coordinates.copy()
        left["code"] = left["code"].map(normalize_cn_code)
        left[coordinate_time_field] = pd.to_datetime(left[coordinate_time_field], errors="raise")
        if left[coordinate_time_field].max() > self.maximum_observable_time:
            raise PermissionError("coordinate exceeds development-only maximum observable time")
        right = versions.loc[
            versions["pit_status"].isin({"ELIGIBLE_CURRENT_SNAPSHOT_VERSION", "ELIGIBLE_DISCLOSURE_EPISODE"})
        ].copy()
        right["code"] = right["code"].map(normalize_cn_code)
        right["observable_time"] = pd.to_datetime(right["observable_time"], errors="coerce")
        right = right.loc[
            right["observable_time"].notna()
            & right["observable_time"].le(self.maximum_observable_time)
        ]
        evidence = [column for column in ("version_id", "episode_id", "report_period") if column in right]
        right = right.sort_values(["code", "observable_time", "report_period"], kind="mergesort")
        parts: list[pd.DataFrame] = []
        for code, left_part in left.groupby("code", sort=False):
            right_part = right.loc[
                right["code"].eq(code), ["observable_time", *evidence, *value_fields]
            ].sort_values(["observable_time", "report_period"], kind="mergesort")
            if right_part.empty:
                resolved = left_part.copy()
                for field in ["observable_time", *evidence, *value_fields]:
                    resolved[field] = pd.NaT if field.endswith("time") or field == "report_period" else np.nan
            else:
                source_rows = right_part.to_dict("records")
                cursor = 0
                visible_by_period: dict[pd.Timestamp, dict[str, Any]] = {}
                resolved_rows: list[dict[str, Any]] = []
                for coordinate in left_part.sort_values(coordinate_time_field).to_dict("records"):
                    query_time = pd.Timestamp(coordinate[coordinate_time_field])
                    while cursor < len(source_rows) and pd.Timestamp(source_rows[cursor]["observable_time"]) <= query_time:
                        source_row = source_rows[cursor]
                        period = pd.Timestamp(source_row["report_period"])
                        prior = visible_by_period.get(period)
                        if prior is None or pd.Timestamp(source_row["observable_time"]) >= pd.Timestamp(prior["observable_time"]):
                            visible_by_period[period] = source_row
                        cursor += 1
                    combined = dict(coordinate)
                    if visible_by_period:
                        best_period = max(visible_by_period)
                        best = visible_by_period[best_period]
                        for field in ["observable_time", *evidence, *value_fields]:
                            combined[field] = best.get(field)
                    else:
                        for field in ["observable_time", *evidence, *value_fields]:
                            combined[field] = pd.NaT if field.endswith("time") or field == "report_period" else np.nan
                    resolved_rows.append(combined)
                resolved = pd.DataFrame(resolved_rows)
            parts.append(resolved)
        if not parts:
            return left.assign(**{field: np.nan for field in value_fields})
        output = pd.concat(parts, ignore_index=True)
        return output.sort_values(["code", coordinate_time_field], kind="mergesort").reset_index(drop=True)


class DeterministicSessionCache:
    """Atomic, content-addressed cache for session-level materializations."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def key(self, *, registry_hash: str, source_manifest_hash: str, request: Any, coordinates: pd.DataFrame) -> str:
        keys = [column for column in ("code", "session_time", "trade_date") if column in coordinates]
        canonical = coordinates[keys].copy().sort_values(keys, kind="mergesort").reset_index(drop=True)
        coordinate_hash = hashlib.sha256(
            pd.util.hash_pandas_object(canonical, index=False).to_numpy(dtype=np.uint64).tobytes()
        ).hexdigest()
        return stable_hash(
            {
                "fabric_version": FABRIC_VERSION,
                "registry_hash": registry_hash,
                "source_manifest_hash": source_manifest_hash,
                "request": request,
                "coordinate_hash": coordinate_hash,
            }
        )

    def read(self, key: str) -> pd.DataFrame | None:
        path = self.root / f"{key}.parquet"
        return pd.read_parquet(path) if path.exists() else None

    def write(self, key: str, frame: pd.DataFrame, manifest: Mapping[str, Any]) -> tuple[Path, Path]:
        output = self.root / f"{key}.parquet"
        evidence = self.root / f"{key}.manifest.json"
        with tempfile.NamedTemporaryFile(dir=self.root, suffix=".parquet", delete=False) as handle:
            temp_output = Path(handle.name)
        with tempfile.NamedTemporaryFile(dir=self.root, suffix=".json", mode="w", encoding="utf-8", delete=False) as handle:
            temp_evidence = Path(handle.name)
            json.dump(dict(manifest), handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        try:
            frame.to_parquet(temp_output, index=False)
            os.replace(temp_output, output)
            os.replace(temp_evidence, evidence)
        finally:
            temp_output.unlink(missing_ok=True)
            temp_evidence.unlink(missing_ok=True)
        return output, evidence


class PITFundamentalFabricAdapter:
    """Lazy field/coordinate adapter over symbol-partitioned sidecars."""

    def __init__(
        self,
        *,
        source_root: Path,
        sessions: Sequence[pd.Timestamp] | pd.DatetimeIndex,
        maximum_observable_time: pd.Timestamp | str,
    ) -> None:
        self.source_root = Path(source_root)
        self.sessions = pd.DatetimeIndex(pd.to_datetime(list(sessions), errors="raise"))
        self.maximum_observable_time = pd.Timestamp(maximum_observable_time)
        self.resolver = StockSessionAsOfResolver(maximum_observable_time=self.maximum_observable_time)

    def _load_financial(self, request: FundamentalFieldRequest, codes: Iterable[str]) -> pd.DataFrame:
        columns = [
            "source_code6", "SECURITY_CODE", "NOTICE_DATE", "UPDATE_DATE", "REPORT_DATE",
            request.source_field,
        ]
        parts: list[pd.DataFrame] = []
        for code in sorted({normalize_cn_code(item) for item in codes}):
            path = source_partition_path(self.source_root, request.source_table, code)
            if not path.exists():
                continue
            schema = set(pq.ParquetFile(path).schema_arrow.names)
            selected = [column for column in columns if column in schema]
            if request.source_field not in selected:
                continue
            parts.append(pd.read_parquet(path, columns=selected))
        if not parts:
            return pd.DataFrame()
        return conservative_financial_versions(
            pd.concat(parts, ignore_index=True),
            table=request.source_table,
            sessions=self.sessions,
            maximum_observable_time=self.maximum_observable_time,
        )

    def _load_holder(self, request: FundamentalFieldRequest, codes: Iterable[str]) -> pd.DataFrame:
        columns = ["source_code6", "股票代码", "公告日期", "截至日期", request.source_field]
        parts: list[pd.DataFrame] = []
        for code in sorted({normalize_cn_code(item) for item in codes}):
            path = source_partition_path(self.source_root, request.source_table, code)
            if not path.exists():
                continue
            schema = set(pq.ParquetFile(path).schema_arrow.names)
            selected = [column for column in columns if column in schema]
            if request.source_field not in selected:
                continue
            parts.append(pd.read_parquet(path, columns=selected))
        if not parts:
            return pd.DataFrame()
        return conservative_holder_episodes(
            pd.concat(parts, ignore_index=True),
            sessions=self.sessions,
            maximum_observable_time=self.maximum_observable_time,
        )

    @staticmethod
    def _aggregate_holder_versions(versions: pd.DataFrame, request: FundamentalFieldRequest) -> pd.DataFrame:
        scalar_fields = {"股东总数", "平均持股数"}
        transform = request.transform
        if transform == "latest" and request.source_field not in scalar_fields:
            raise ValueError("holder-level field requires explicit sum/max/mean/count/top10_sum aggregation")
        rows: list[dict[str, Any]] = []
        group_columns = ["episode_id", "code", "report_period", "notice_date", "observable_time", "pit_status"]
        for keys, group in versions.groupby(group_columns, dropna=False, sort=False):
            episode_id, code, report_period, notice_date, observable_time, pit_status = keys
            values = pd.to_numeric(group[request.source_field], errors="coerce")
            if transform == "latest":
                unique = values.dropna().unique()
                value = unique[0] if len(unique) == 1 else np.nan
            elif transform == "sum":
                value = values.sum(min_count=1)
            elif transform == "max":
                value = values.max()
            elif transform == "mean":
                value = values.mean()
            elif transform == "count":
                value = float(values.notna().sum())
            elif transform == "top10_sum":
                value = values.sort_values(ascending=False).head(10).sum(min_count=1)
            else:
                raise ValueError(f"unsupported holder aggregation: {transform}")
            rows.append(
                {
                    "episode_id": episode_id,
                    "code": code,
                    "report_period": report_period,
                    "notice_date": notice_date,
                    "observable_time": observable_time,
                    "pit_status": pit_status,
                    request.output_name: value,
                }
            )
        return pd.DataFrame(rows)

    def materialize_level(self, request: FundamentalFieldRequest, coordinates: pd.DataFrame) -> pd.DataFrame:
        if request.route != "FUNDAMENTAL_LEVEL":
            raise ValueError("materialize_level requires FUNDAMENTAL_LEVEL route")
        if request.source_table in PIT_UNRESOLVED_TABLES:
            raise PermissionError(f"{request.source_table} is PIT_CONTRACT_UNRESOLVED")
        if request.source_table in FINANCIAL_TABLES:
            versions = self._load_financial(request, coordinates["code"])
            if not versions.empty:
                versions = versions.rename(columns={request.source_field: request.output_name})
        elif request.source_table == "main_stock_holder_sina":
            versions = self._load_holder(request, coordinates["code"])
            if not versions.empty:
                versions = self._aggregate_holder_versions(versions, request)
        else:
            raise ValueError(f"unsupported fundamental source table: {request.source_table}")
        if versions.empty:
            output = coordinates.copy()
            output[request.output_name] = np.nan
            return output
        return self.resolver.resolve(versions, coordinates, value_fields=[request.output_name])

    def materialize_change(self, request: FundamentalFieldRequest, coordinates: pd.DataFrame) -> pd.DataFrame:
        if request.route != "FUNDAMENTAL_CHANGE":
            raise ValueError("materialize_change requires FUNDAMENTAL_CHANGE route")
        if request.source_table not in FINANCIAL_TABLES:
            raise NotImplementedError("holder temporal changes require an aggregated level request first")
        if request.transform == "ttm" and request.source_table == "balance_sheet_report_em":
            raise ValueError("TTM is defined for cumulative flow statements, not balance-sheet stock levels")
        versions = self._load_financial(request, coordinates["code"])
        if versions.empty:
            output = coordinates.copy()
            output[request.output_name] = np.nan
            return output
        derived = compute_disclosed_change(
            versions,
            value_field=request.source_field,
            transform=request.transform,
        ).rename(columns={f"{request.source_field}__{request.transform}": request.output_name})
        return self.resolver.resolve(derived, coordinates, value_fields=[request.output_name])

    def disclosure_episodes(
        self,
        request: FundamentalFieldRequest,
        *,
        codes: Iterable[str],
    ) -> pd.DataFrame:
        if request.route != "DISCLOSURE_EVENT":
            raise ValueError("disclosure_episodes requires DISCLOSURE_EVENT route")
        if request.source_table in PIT_UNRESOLVED_TABLES:
            raise PermissionError(f"{request.source_table} is PIT_CONTRACT_UNRESOLVED")
        if request.source_table in FINANCIAL_TABLES:
            versions = self._load_financial(request, codes)
            if versions.empty:
                return pd.DataFrame()
            eligible = versions.loc[
                versions["pit_status"].eq("ELIGIBLE_CURRENT_SNAPSHOT_VERSION")
                & versions["disclosure_event_eligible"]
            ].copy()
            eligible["episode_id"] = [
                stable_hash(
                    {
                        "fabric": FABRIC_VERSION,
                        "table": request.source_table,
                        "code": code,
                        "report_period": str(period),
                        "observable_time": str(observable),
                    }
                )[:24]
                for code, period, observable in zip(
                    eligible["code"], eligible["report_period"], eligible["observable_time"]
                )
            ]
            eligible["event_kind"] = "INITIAL_FINANCIAL_DISCLOSURE_CURRENT_VERSION_UNCHANGED"
            eligible["maturity_time"] = eligible["observable_time"]
            return eligible[
                ["episode_id", "code", "report_period", "notice_date", "observable_time", "maturity_time", "event_kind"]
            ].drop_duplicates("episode_id")
        if request.source_table == "main_stock_holder_sina":
            versions = self._load_holder(request, codes)
            if versions.empty:
                return pd.DataFrame()
            eligible = versions.loc[
                versions["pit_status"].eq("ELIGIBLE_DISCLOSURE_EPISODE")
                & versions["disclosure_event_eligible"]
            ].copy()
            eligible["event_kind"] = "MAJOR_HOLDER_DISCLOSURE"
            eligible["maturity_time"] = eligible["observable_time"]
            return eligible[
                ["episode_id", "code", "report_period", "notice_date", "observable_time", "maturity_time", "event_kind"]
            ].drop_duplicates("episode_id")
        raise ValueError(f"unsupported disclosure source table: {request.source_table}")


def compute_disclosed_change(
    versions: pd.DataFrame,
    *,
    value_field: str,
    transform: str,
) -> pd.DataFrame:
    """Derive a temporal route while propagating every dependency clock."""

    allowed = {"delta", "yoy", "slope", "acceleration", "persistence", "ttm"}
    if transform not in allowed:
        raise ValueError(f"unsupported disclosed change transform: {transform}")
    required = {"code", "report_period", "observable_time", "pit_status", value_field}
    if missing := required - set(versions.columns):
        raise ValueError(f"change input missing columns: {sorted(missing)}")
    frame = versions.loc[
        versions["pit_status"].eq("ELIGIBLE_CURRENT_SNAPSHOT_VERSION")
    ].copy()
    frame["report_period"] = pd.to_datetime(frame["report_period"], errors="coerce").dt.normalize()
    frame["observable_time"] = pd.to_datetime(frame["observable_time"], errors="coerce")
    frame[value_field] = pd.to_numeric(frame[value_field], errors="coerce")
    frame = frame.sort_values(["code", "report_period", "observable_time"], kind="mergesort").drop_duplicates(
        ["code", "report_period"], keep="last"
    )
    output_rows: list[dict[str, Any]] = []
    for code, group in frame.groupby("code", sort=False):
        records = list(group.to_dict("records"))
        by_period = {pd.Timestamp(row["report_period"]): row for row in records}
        previous_deltas: list[tuple[float, pd.Timestamp]] = []
        persistence_sign: float | None = None
        persistence_count = 0
        persistence_clock = pd.NaT
        for index, row in enumerate(records):
            period = pd.Timestamp(row["report_period"])
            current = row[value_field]
            current_clock = pd.Timestamp(row["observable_time"])
            value = np.nan
            dependency_clocks = [current_clock]
            if transform in {"delta", "slope", "acceleration", "persistence"}:
                previous = records[index - 1] if index > 0 else None
                delta = np.nan
                delta_clock = current_clock
                if previous is not None and pd.notna(current) and pd.notna(previous[value_field]):
                    delta = float(current - previous[value_field])
                    delta_clock = max(current_clock, pd.Timestamp(previous["observable_time"]))
                if transform == "delta":
                    value = delta
                    dependency_clocks = [delta_clock]
                elif transform == "slope":
                    days = (period - pd.Timestamp(previous["report_period"])).days if previous is not None else 0
                    value = delta / days if pd.notna(delta) and days > 0 else np.nan
                    dependency_clocks = [delta_clock]
                elif transform == "acceleration":
                    if previous_deltas and pd.notna(delta) and pd.notna(previous_deltas[-1][0]):
                        value = float(delta - previous_deltas[-1][0])
                        dependency_clocks = [delta_clock, previous_deltas[-1][1]]
                    previous_deltas.append((delta, delta_clock))
                else:
                    sign = float(np.sign(delta)) if pd.notna(delta) and delta != 0 else None
                    if sign is None:
                        persistence_sign = None
                        persistence_count = 0
                        persistence_clock = delta_clock
                        value = np.nan
                    elif sign == persistence_sign:
                        persistence_count += 1
                        persistence_clock = max(pd.Timestamp(persistence_clock), delta_clock)
                        value = float(persistence_count)
                    else:
                        persistence_sign = sign
                        persistence_count = 1
                        persistence_clock = delta_clock
                        value = 1.0
                    dependency_clocks = [pd.Timestamp(persistence_clock)]
                if transform != "acceleration":
                    previous_deltas.append((delta, delta_clock))
            elif transform == "yoy":
                comparable_period = pd.Timestamp(year=period.year - 1, month=period.month, day=period.day)
                comparable = by_period.get(comparable_period)
                if comparable is not None and pd.notna(current) and pd.notna(comparable[value_field]):
                    value = float(current - comparable[value_field])
                    dependency_clocks.append(pd.Timestamp(comparable["observable_time"]))
            elif transform == "ttm":
                if period.month == 12 and period.day == 31:
                    value = float(current) if pd.notna(current) else np.nan
                elif (period.month, period.day) in {(3, 31), (6, 30), (9, 30)}:
                    annual_period = pd.Timestamp(year=period.year - 1, month=12, day=31)
                    comparable_period = pd.Timestamp(year=period.year - 1, month=period.month, day=period.day)
                    annual = by_period.get(annual_period)
                    comparable = by_period.get(comparable_period)
                    if (
                        annual is not None
                        and comparable is not None
                        and pd.notna(current)
                        and pd.notna(annual[value_field])
                        and pd.notna(comparable[value_field])
                    ):
                        value = float(annual[value_field] + current - comparable[value_field])
                        dependency_clocks.extend(
                            [pd.Timestamp(annual["observable_time"]), pd.Timestamp(comparable["observable_time"])]
                        )
            derived_clock = max(dependency_clocks)
            derived = dict(row)
            derived[f"{value_field}__{transform}"] = value
            derived["observable_time"] = derived_clock
            derived["version_id"] = stable_hash(
                {
                    "base_version": row.get("version_id", ""),
                    "transform": transform,
                    "derived_observable_time": derived_clock.isoformat(),
                }
            )[:24]
            output_rows.append(derived)
    return pd.DataFrame(output_rows)


def compute_ttm_from_disclosed_ytd(
    versions: pd.DataFrame,
    *,
    value_field: str,
    as_of_time: pd.Timestamp | str,
) -> pd.DataFrame:
    """Compute TTM without consulting versions not observable at ``as_of``.

    Regular fiscal quarter ends only.  Annual rows equal TTM.  For Q1/H1/Q3,
    TTM is prior annual plus current YTD minus prior-year comparable YTD.
    Missing or irregular periods stay unknown.
    """

    cutoff = pd.Timestamp(as_of_time)
    derived = compute_disclosed_change(versions, value_field=value_field, transform="ttm")
    return derived.loc[pd.to_datetime(derived["observable_time"], errors="coerce").le(cutoff)].copy()
