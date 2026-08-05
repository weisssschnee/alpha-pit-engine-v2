"""Deterministically sessionize a yearly CN 1-minute ZIP without changing daily execution semantics.

The converter never selects securities or sessions by returns.  Every selected
archive member is reduced to first open, last close, high, low, summed volume
and summed amount for each observed session.  Outputs are symbol-sharded so the
existing report-only sidecar builder can retain candidate-axis parallelism.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import re
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq


SCHEMA_VERSION = "cn_yearly_1min_zip_session_shards_v1"
STATUS = "SESSIONIZED_ARCHIVE_COMPLETE"
MEMBER_RE = re.compile(r"^(?P<exchange>sh|sz|bj)(?P<code>\d{6})_(?P<year>\d{4})\.csv$", re.I)
OUTPUT_SCHEMA = pa.schema(
    [
        ("code", pa.string()),
        ("trade_time", pa.timestamp("ns")),
        ("date", pa.timestamp("ns")),
        ("open", pa.float32()),
        ("high", pa.float32()),
        ("low", pa.float32()),
        ("close", pa.float32()),
        ("vol", pa.int64()),
        ("amount", pa.float64()),
    ]
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_payload_sha256(payload: dict) -> str:
    body = dict(payload)
    body.pop("manifest_payload_sha256", None)
    return hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _normalize_member(member: str, expected_year: int) -> tuple[str, str]:
    name = Path(member).name
    match = MEMBER_RE.fullmatch(name)
    if match is None or int(match.group("year")) != int(expected_year):
        raise ValueError(f"unexpected yearly archive member: {member}")
    suffix = {"sh": "SH", "sz": "SZ", "bj": "BJ"}[match.group("exchange").lower()]
    return f"{match.group('code')}.{suffix}", name


def _session_rows(
    archive: zipfile.ZipFile,
    member: str,
    *,
    expected_year: int,
    date_min: str,
    date_max: str,
) -> list[dict]:
    normalized_code, _ = _normalize_member(member, expected_year)
    output: list[dict] = []
    current: dict | None = None
    last_timestamp: datetime | None = None
    with archive.open(member, "r") as raw:
        text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        required = {"时间", "代码", "开盘价", "收盘价", "最高价", "最低价", "成交量", "成交额"}
        if reader.fieldnames is None or not required.issubset(reader.fieldnames):
            raise RuntimeError(f"archive member schema drift: {member}")
        for row in reader:
            timestamp = datetime.fromisoformat(str(row["时间"]))
            date_text = timestamp.date().isoformat()
            if date_text < date_min or date_text > date_max:
                continue
            if timestamp.year != int(expected_year):
                raise RuntimeError(f"member crossed expected year: {member}")
            if last_timestamp is not None and timestamp <= last_timestamp:
                raise RuntimeError(f"member timestamps are not strictly increasing: {member}")
            last_timestamp = timestamp
            open_value = float(row["开盘价"])
            close_value = float(row["收盘价"])
            high_value = float(row["最高价"])
            low_value = float(row["最低价"])
            volume_value = int(round(float(row["成交量"])))
            amount_value = float(row["成交额"])
            if current is None or current["date_text"] != date_text:
                if current is not None:
                    output.append(current)
                current = {
                    "date_text": date_text,
                    "code": normalized_code,
                    "trade_time": timestamp,
                    "date": datetime(timestamp.year, timestamp.month, timestamp.day),
                    "open": open_value,
                    "high": high_value,
                    "low": low_value,
                    "close": close_value,
                    "vol": volume_value,
                    "amount": amount_value,
                }
            else:
                current["trade_time"] = timestamp
                current["high"] = max(float(current["high"]), high_value)
                current["low"] = min(float(current["low"]), low_value)
                current["close"] = close_value
                current["vol"] = int(current["vol"]) + volume_value
                current["amount"] = float(current["amount"]) + amount_value
    if current is not None:
        output.append(current)
    for row in output:
        row.pop("date_text", None)
    return output


def _convert_shard(args: tuple) -> dict:
    archive_path, members, output_path, expected_year, date_min, date_max = args
    archive_path = Path(archive_path)
    output_path = Path(output_path)
    rows: list[dict] = []
    with zipfile.ZipFile(archive_path) as archive:
        for member in members:
            rows.extend(
                _session_rows(
                    archive,
                    member,
                    expected_year=expected_year,
                    date_min=date_min,
                    date_max=date_max,
                )
            )
    rows.sort(key=lambda row: (row["trade_time"], row["code"]))
    table = pa.Table.from_pylist(rows, schema=OUTPUT_SCHEMA)
    if table.num_rows == 0:
        raise RuntimeError(f"sessionized shard is empty: {output_path}")
    temporary = output_path.with_suffix(".tmp.parquet")
    pq.write_table(table, temporary, compression="zstd", row_group_size=262_144)
    temporary.replace(output_path)
    codes = {str(row["code"]) for row in rows}
    dates = {row["date"].date().isoformat() for row in rows}
    return {
        "path": output_path.name,
        "sha256": _sha256(output_path),
        "bytes": output_path.stat().st_size,
        "rows": len(rows),
        "security_count": len(codes),
        "date_count": len(dates),
        "date_min": min(dates),
        "date_max": max(dates),
    }


def convert_archive(
    *,
    archive_path: Path,
    expected_archive_sha256: str,
    expected_year: int,
    output_root: Path,
    shard_count: int,
    worker_count: int,
    date_min: str,
    date_max: str,
    member_limit: int | None,
) -> dict:
    archive_path = archive_path.resolve()
    output_root = output_root.resolve()
    if output_root.exists():
        raise FileExistsError(f"output root must be fresh: {output_root}")
    if _sha256(archive_path) != expected_archive_sha256:
        raise RuntimeError("yearly archive hash drift")
    with zipfile.ZipFile(archive_path) as archive:
        members = sorted(
            name for name in archive.namelist() if not name.endswith("/")
        )
    for member in members:
        _normalize_member(member, expected_year)
    if member_limit is not None:
        members = members[: int(member_limit)]
    if not members:
        raise RuntimeError("yearly archive has no selected members")
    shard_count = min(int(shard_count), len(members))
    worker_count = min(int(worker_count), shard_count)
    assignments = [members[index::shard_count] for index in range(shard_count)]
    output_root.mkdir(parents=True, exist_ok=False)
    work = [
        (
            str(archive_path),
            assignment,
            str(output_root / f"shard_{index:02d}.parquet"),
            int(expected_year),
            date_min,
            date_max,
        )
        for index, assignment in enumerate(assignments)
    ]
    if worker_count == 1:
        artifacts = [_convert_shard(item) for item in work]
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            artifacts = list(executor.map(_convert_shard, work))
    artifacts.sort(key=lambda row: row["path"])
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "archive_path": str(archive_path),
        "archive_sha256": expected_archive_sha256,
        "expected_year": int(expected_year),
        "date_min": date_min,
        "date_max": date_max,
        "selected_archive_member_count": len(members),
        "shard_count": shard_count,
        "worker_count": worker_count,
        "sessionization_contract": {
            "open": "FIRST_OBSERVED_OPEN_PER_SYMBOL_SESSION",
            "close": "LAST_OBSERVED_CLOSE_PER_SYMBOL_SESSION",
            "high": "MAX_OBSERVED_HIGH_PER_SYMBOL_SESSION",
            "low": "MIN_OBSERVED_LOW_PER_SYMBOL_SESSION",
            "vol": "SUM_OBSERVED_VOLUME_PER_SYMBOL_SESSION",
            "amount": "SUM_OBSERVED_AMOUNT_PER_SYMBOL_SESSION",
            "trade_time": "LAST_OBSERVED_TIMESTAMP_PER_SYMBOL_SESSION",
            "outcome_based_filtering": "FORBIDDEN",
        },
        "session_row_count": sum(int(row["rows"]) for row in artifacts),
        "security_count": sum(int(row["security_count"]) for row in artifacts),
        "observed_date_min": min(str(row["date_min"]) for row in artifacts),
        "observed_date_max": max(str(row["date_max"]) for row in artifacts),
        "artifacts": artifacts,
    }
    manifest["manifest_payload_sha256"] = _canonical_payload_sha256(manifest)
    manifest_path = output_root / "SESSIONIZED_ARCHIVE_COMPLETE.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--expected-archive-sha256", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--shard-count", type=int, default=12)
    parser.add_argument("--worker-count", type=int, default=12)
    parser.add_argument("--date-min")
    parser.add_argument("--date-max")
    parser.add_argument("--member-limit", type=int)
    args = parser.parse_args()
    manifest = convert_archive(
        archive_path=args.archive,
        expected_archive_sha256=args.expected_archive_sha256,
        expected_year=args.year,
        output_root=args.output_root,
        shard_count=args.shard_count,
        worker_count=args.worker_count,
        date_min=args.date_min or f"{args.year:04d}-01-01",
        date_max=args.date_max or f"{args.year:04d}-12-31",
        member_limit=args.member_limit,
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "session_row_count": manifest["session_row_count"],
                "security_count": manifest["security_count"],
                "manifest_payload_sha256": manifest["manifest_payload_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
