"""Build an immutable report-only validation session authority.

This extends the existing A-share replay kernel to the already-frozen
validation sidecar.  It does not evaluate candidates, labels, holdout, or
2026 data.  The builder deliberately keeps SSE/SZSE membership, PIT ST state,
the exchange calendar, and corporate actions explicit and independently
auditable.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

from scripts import build_cn_finalist_session_authority as base
from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1


SCHEMA_VERSION = "cn_validation_session_authority_v1"
STATUS = "VALIDATION_SESSION_AUTHORITY_CLOSED_IMMUTABLE"
ALLOWED_EXCHANGES = ("SSE", "SZSE")
DATE_MIN = "2025-07-08"
DATE_MAX = "2025-10-24"
ST_FIELD = "ctx_hfq_is_st"


def _artifact(path: Path, root: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(root).as_posix(),
        "bytes": path.stat().st_size,
        "sha256": v1._sha256(path),
    }


def _load_field_sessions(
    *, field_manifest_path: Path, expected_sha256: str
) -> tuple[pd.DataFrame, dict[str, Any]]:
    field_manifest_path = field_manifest_path.resolve()
    if v1._sha256(field_manifest_path) != expected_sha256:
        raise RuntimeError("validation field manifest hash drift")
    manifest = v1._read_json(field_manifest_path)
    required = {
        "status": "TIME_MAJOR_LAYOUT_PARITY_PASS",
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "holdout_reads": 0,
        "forward_2026_reads": 0,
    }
    drift = [key for key, value in required.items() if manifest.get(key) != value]
    if drift:
        raise RuntimeError("validation field manifest drift: " + ",".join(drift))
    if int(manifest.get("validation_reads") or 0) <= 0:
        raise RuntimeError("validation field manifest has no validation reads")
    shards = list(manifest.get("shards") or ())
    if len(shards) != int(manifest.get("source_shard_count") or -1):
        raise RuntimeError("validation field shard cardinality drift")
    frames: list[pd.DataFrame] = []
    for shard in shards:
        path = Path(str(shard["output_path"])).resolve()
        if not path.is_file() or v1._sha256(path) != str(shard["output_sha256"]):
            raise RuntimeError(f"validation field shard drift: {path}")
        frame = pd.read_parquet(
            path,
            columns=["trade_time", "code", "open", "close", "source_shard"],
        )
        if len(frame) != int(shard["rows"]):
            raise RuntimeError(f"validation field shard row drift: {path}")
        frames.append(frame)
    observed = pd.concat(frames, ignore_index=True)
    observed["date"] = pd.to_datetime(
        observed.pop("trade_time"), errors="raise"
    ).dt.normalize()
    observed["code"] = observed["code"].map(base.normalize_code)
    observed["open"] = pd.to_numeric(observed["open"], errors="coerce")
    observed["close"] = pd.to_numeric(observed["close"], errors="coerce")
    observed = observed.sort_values(["date", "code"], kind="mergesort")
    observed = observed.reset_index(drop=True)
    if observed.duplicated(["date", "code"]).any():
        raise RuntimeError("validation field sessions contain duplicate coordinates")
    if len(observed) != int(manifest.get("sidecar_rows") or -1):
        raise RuntimeError("validation field aggregate row drift")
    dates = pd.DatetimeIndex(observed["date"].unique()).sort_values()
    if (
        dates[0] != pd.Timestamp(DATE_MIN)
        or dates[-1] != pd.Timestamp(DATE_MAX)
        or len(dates) != int(manifest.get("eligible_validation_date_count") or -1)
    ):
        raise RuntimeError("validation field date boundary drift")
    return observed, manifest


def _extract_exact_st(
    *, field_manifest: dict[str, Any], validation_dates: tuple[Any, ...]
) -> tuple[pd.DataFrame, list[dict[str, Any]], int]:
    parts: list[pd.DataFrame] = []
    receipts: list[dict[str, Any]] = []
    total_source_rows = 0
    for shard in field_manifest.get("shards") or ():
        source_path = Path(str(shard["source_path"])).resolve()
        observed_source_sha = v1._sha256(source_path)
        if observed_source_sha != str(shard["source_sha256"]):
            raise RuntimeError(f"validation ST source hash drift: {source_path}")
        schema = set(pl.scan_parquet(source_path).collect_schema().names())
        required = {"trade_time", "code", ST_FIELD}
        if not required.issubset(schema):
            raise RuntimeError(
                f"validation ST source lacks {sorted(required - schema)}: {source_path}"
            )
        grouped = (
            pl.scan_parquet(source_path, low_memory=True)
            .filter(pl.col("trade_time").dt.date().is_in(validation_dates))
            .select("trade_time", "code", ST_FIELD)
            .with_columns(pl.col("trade_time").dt.date().alias("date"))
            .sort("code", "date", "trade_time")
            .group_by("code", "date", maintain_order=True)
            .agg(
                pl.col(ST_FIELD)
                .drop_nulls()
                .last()
                .alias("is_st"),
                pl.col(ST_FIELD)
                .drop_nulls()
                .n_unique()
                .alias("st_intraday_nunique"),
                pl.len().alias("source_rows"),
            )
            .sort("date", "code")
            .collect(engine="streaming")
            .to_pandas()
        )
        if grouped.empty:
            raise RuntimeError(f"validation ST source selected no rows: {source_path}")
        if int(grouped["st_intraday_nunique"].max()) > 1:
            raise RuntimeError(f"validation ST state varies intraday: {source_path}")
        grouped["code"] = grouped["code"].map(base.normalize_code)
        grouped["date"] = pd.to_datetime(grouped["date"], errors="raise").dt.normalize()
        grouped["is_st"] = base._normalize_pit_st(grouped["is_st"])
        source_rows = int(grouped["source_rows"].sum())
        total_source_rows += source_rows
        receipts.append(
            {
                "source_shard": int(shard["source_shard"]),
                "source_path": str(source_path),
                "source_sha256": observed_source_sha,
                "selected_source_rows": source_rows,
                "selected_session_rows": len(grouped),
            }
        )
        parts.append(grouped[["date", "code", "is_st"]])
    exact = pd.concat(parts, ignore_index=True)
    exact = exact.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)
    if exact.duplicated(["date", "code"]).any():
        raise RuntimeError("validation ST sources contain duplicate coordinates")
    return exact, receipts, total_source_rows


def build_validation_session_authority(
    *,
    field_manifest_path: Path,
    public_source_root: Path,
    output_root: Path,
    expected_field_manifest_sha256: str,
    expected_source_manifest_sha256: str,
    builder_commit_sha: str,
) -> dict[str, Any]:
    public_source_root = public_source_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)

    observed, field_manifest = _load_field_sessions(
        field_manifest_path=field_manifest_path,
        expected_sha256=expected_field_manifest_sha256,
    )
    source_receipt = base.verify_source_snapshot(public_source_root)
    if source_receipt["manifest_file_sha256"] != expected_source_manifest_sha256:
        raise RuntimeError("public source snapshot manifest hash drift")
    source_manifest_path = public_source_root / "source_snapshot_manifest.json"

    master = pd.read_parquet(public_source_root / "security_master.parquet")
    master["code"] = master["code"].map(base.normalize_code)
    allowed_master = master.loc[
        master["exchange"].astype(str).isin(ALLOWED_EXCHANGES)
    ].copy()
    allowed_codes = set(allowed_master["code"])
    observed_codes = set(observed["code"])
    excluded_codes = sorted(observed_codes - allowed_codes)
    if any(not code.startswith(("4", "8", "9")) for code in excluded_codes):
        raise RuntimeError(
            "validation code absent from SSE/SZSE authority is not an explicit BSE-family exclusion"
        )
    excluded = observed.loc[observed["code"].isin(excluded_codes)].copy()
    observed = observed.loc[observed["code"].isin(allowed_codes)].copy()
    if observed.empty:
        raise RuntimeError("validation authority has no SSE/SZSE observations")

    dates = tuple(pd.DatetimeIndex(observed["date"].unique()).date)
    exact_st, st_receipts, st_source_rows = _extract_exact_st(
        field_manifest=field_manifest,
        validation_dates=dates,
    )
    observed = observed.merge(
        exact_st, on=["date", "code"], how="left", validate="one_to_one"
    )
    missing_exact_st = int(observed["is_st"].isna().sum())
    observed["is_st"] = observed["is_st"].fillna(True).astype(bool)

    calendar = pd.read_parquet(
        public_source_root / "trade_calendar.parquet",
        filters=[("date", "<=", pd.Timestamp(DATE_MAX))],
    )["date"]
    raw_payloads = []
    dividend_root = public_source_root / "cninfo_dividend_raw"
    for code in sorted(set(observed["code"])):
        path = dividend_root / f"{code}.json"
        if not path.is_file():
            raise FileNotFoundError(path)
        raw_payloads.append(json.loads(path.read_text(encoding="utf-8")))
    actions, action_blockers = base.parse_dividend_actions(
        raw_payloads, date_min=DATE_MIN, date_max=DATE_MAX
    )
    if action_blockers:
        raise RuntimeError(
            "validation corporate-action source is incomplete: "
            + ",".join(action_blockers[:20])
        )
    authority = base.materialize_session_authority(
        observed=observed[["date", "code", "open", "close", "is_st"]],
        security_master=allowed_master,
        trade_calendar=calendar,
        actions=actions,
        date_min=DATE_MIN,
        date_max=DATE_MAX,
    )

    observed_path = output_root / "validation_observed_sessions.parquet"
    authority_path = output_root / "validation_session_authority.parquet"
    excluded_path = output_root / "excluded_non_sse_szse_codes.json"
    observed.to_parquet(observed_path, index=False)
    authority.to_parquet(authority_path, index=False)
    excluded_payload = {
        "schema_version": "cn_validation_session_exclusions_v1",
        "policy": "EXCLUDE_IF_ABSENT_FROM_IMMUTABLE_SSE_SZSE_SECURITY_MASTER",
        "allowed_exchanges": list(ALLOWED_EXCHANGES),
        "excluded_code_count": len(excluded_codes),
        "excluded_row_count": len(excluded),
        "excluded_codes": excluded_codes,
    }
    excluded_path = v1._write_json(excluded_path, excluded_payload)

    artifacts = [
        _artifact(observed_path, output_root),
        _artifact(authority_path, output_root),
        _artifact(excluded_path, output_root),
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "builder_commit_sha": builder_commit_sha,
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "evidence_scope": "ADAPTIVE_REPORT_ONLY_VALIDATION_OOS_INPUT",
        "date_min": DATE_MIN,
        "date_max": DATE_MAX,
        "validation_date_count": int(authority["date"].nunique()),
        "validation_field_rows_read": int(field_manifest["validation_reads"]),
        "validation_st_source_rows_read": st_source_rows,
        "validation_reads": int(field_manifest["validation_reads"]) + st_source_rows,
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
        "allowed_exchanges": list(ALLOWED_EXCHANGES),
        "observed_security_count": int(observed["code"].nunique()),
        "observed_session_row_count": len(observed),
        "authority_security_count": int(authority["code"].nunique()),
        "authority_session_row_count": len(authority),
        "exact_st_session_count": len(observed) - missing_exact_st,
        "missing_exact_st_fail_closed_session_count": missing_exact_st,
        "excluded_non_sse_szse_code_count": len(excluded_codes),
        "excluded_non_sse_szse_row_count": len(excluded),
        "suspended_session_row_count": int(authority["suspended"].sum()),
        "terminal_session_row_count": int(authority["is_terminal_session"].sum()),
        "corporate_cash_event_count": int(
            authority["corporate_action_cash_per_share"].gt(0).sum()
        ),
        "corporate_share_event_count": int(
            authority["corporate_action_share_multiplier"].ne(1.0).sum()
        ),
        "columns": list(base.SESSION_AUTHORITY_COLUMNS),
        "field_manifest": str(field_manifest_path.resolve()),
        "field_manifest_sha256": expected_field_manifest_sha256,
        "field_split_manifest_hash": str(field_manifest["split_manifest_hash"]),
        "public_source_snapshot_manifest": str(source_manifest_path),
        "public_source_snapshot_manifest_sha256": expected_source_manifest_sha256,
        "public_source_snapshot_payload_sha256": v1._read_json(
            source_manifest_path
        )["manifest_payload_sha256"],
        "public_source_snapshot_verified_artifact_count": int(
            source_receipt["artifact_count"]
        ),
        "st_source_receipts": st_receipts,
        "policies": {
            "signal_clock": "PRIOR_CLOSE",
            "execution_clock": "NEXT_OPEN",
            "suspension": "LISTED_CALENDAR_MINUS_OBSERVED_SESSION",
            "st": "EXACT_CODE_DATE_PIT_SOURCE_FAIL_CLOSED_ON_GAP",
            "universe": "SSE_SZSE_ONLY_EXPLICIT_BSE_EXCLUSION",
            "corporate_actions": "IMMUTABLE_CNINFO_RAW_EFFECTIVE_DATE",
        },
        "artifacts": artifacts,
    }
    manifest["manifest_payload_sha256"] = v1._stable_hash(manifest)
    manifest_path = v1._write_json(
        output_root / "validation_session_authority_manifest.json", manifest
    )
    return {
        "status": STATUS,
        "manifest": str(manifest_path),
        "manifest_file_sha256": v1._sha256(manifest_path),
        "manifest_payload_sha256": manifest["manifest_payload_sha256"],
        "authority_session_row_count": len(authority),
        "excluded_non_sse_szse_code_count": len(excluded_codes),
        "validation_reads": manifest["validation_reads"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field-manifest", type=Path, required=True)
    parser.add_argument("--public-source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-field-manifest-sha256", required=True)
    parser.add_argument("--expected-source-manifest-sha256", required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    args = parser.parse_args()
    result = build_validation_session_authority(
        field_manifest_path=args.field_manifest,
        public_source_root=args.public_source_root,
        output_root=args.output_root,
        expected_field_manifest_sha256=args.expected_field_manifest_sha256,
        expected_source_manifest_sha256=args.expected_source_manifest_sha256,
        builder_commit_sha=args.builder_commit_sha,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
