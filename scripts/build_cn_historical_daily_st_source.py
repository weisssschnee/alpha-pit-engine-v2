"""Build an immutable exact code/date ST source from daily public CSV snapshots."""

from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import pandas as pd


STATUS = "HISTORICAL_DAILY_ST_SOURCE_COMPLETE"
SCHEMA_VERSION = "cn_historical_daily_st_source_v1"
DATE_RE = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(payload: dict) -> str:
    body = dict(payload)
    body.pop("manifest_payload_sha256", None)
    return hashlib.sha256(
        json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()


def _normalize_is_st(value: object) -> bool:
    text = str(value).strip().lower()
    if text in {"是", "true", "1", "yes", "y"}:
        return True
    if text in {"否", "false", "0", "no", "n"}:
        return False
    raise ValueError(f"unsupported ST value: {value!r}")


def _load_daily(args: tuple[str, int]) -> tuple[pd.DataFrame, dict]:
    raw_path, expected_year = args
    path = Path(raw_path)
    match = DATE_RE.search(path.name)
    if match is None:
        raise ValueError(f"daily ST source filename has no date: {path.name}")
    filename_date = match.group("date")
    if int(filename_date[:4]) != int(expected_year):
        raise ValueError(f"daily ST source crossed expected year: {path.name}")
    frame = pd.read_csv(
        path,
        encoding="utf-8-sig",
        usecols=["日期", "代码", "名称", "是否ST"],
        dtype={"日期": "string", "代码": "string", "名称": "string", "是否ST": "string"},
    )
    if frame.empty:
        raise RuntimeError(f"daily ST source is empty: {path}")
    observed_dates = set(frame["日期"].astype(str))
    if observed_dates != {filename_date}:
        raise RuntimeError(f"daily ST filename/content date drift: {path.name}")
    frame = frame.rename(
        columns={"日期": "date", "代码": "code", "名称": "name", "是否ST": "is_st"}
    )
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
    frame["code"] = frame["code"].astype(str).str.extract(r"(\d{6})", expand=False)
    if frame["code"].isna().any():
        raise ValueError(f"invalid security code in {path.name}")
    frame["is_st"] = frame["is_st"].map(_normalize_is_st).astype(bool)
    if frame.duplicated(["date", "code"]).any():
        raise RuntimeError(f"duplicate daily ST coordinate: {path.name}")
    frame = frame.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)
    receipt = {
        "path": str(path.resolve()),
        "sha256": _sha256(path),
        "bytes": path.stat().st_size,
        "rows": len(frame),
        "date": filename_date,
        "st_rows": int(frame["is_st"].sum()),
    }
    return frame, receipt


def build_daily_st_source(
    *,
    input_root: Path,
    expected_year: int,
    output_root: Path,
    worker_count: int,
    file_limit: int | None = None,
) -> dict:
    input_root = input_root.resolve()
    output_root = output_root.resolve()
    if output_root.exists():
        raise FileExistsError(f"output root must be fresh: {output_root}")
    sources = sorted(input_root.glob(f"{expected_year:04d}-*.csv"))
    if file_limit is not None:
        sources = sources[: int(file_limit)]
    if not sources:
        raise FileNotFoundError("no daily ST source files selected")
    output_root.mkdir(parents=True, exist_ok=False)
    work = [(str(path), int(expected_year)) for path in sources]
    if int(worker_count) == 1:
        results = [_load_daily(item) for item in work]
    else:
        with ProcessPoolExecutor(max_workers=min(int(worker_count), len(work))) as executor:
            results = list(executor.map(_load_daily, work))
    frames = [item[0] for item in results]
    receipts = [item[1] for item in results]
    combined = pd.concat(frames, ignore_index=True).sort_values(
        ["date", "code"], kind="mergesort"
    )
    combined = combined.reset_index(drop=True)
    if combined.duplicated(["date", "code"]).any():
        raise RuntimeError("duplicate coordinate across daily ST sources")
    target = output_root / f"historical_daily_st_{expected_year}.parquet"
    temporary = target.with_suffix(".tmp.parquet")
    combined.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(target)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "status": STATUS,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "expected_year": int(expected_year),
        "input_root": str(input_root),
        "source_file_count": len(sources),
        "worker_count": min(int(worker_count), len(work)),
        "row_count": len(combined),
        "date_count": int(combined["date"].nunique()),
        "security_count": int(combined["code"].nunique()),
        "date_min": combined["date"].min().date().isoformat(),
        "date_max": combined["date"].max().date().isoformat(),
        "st_row_count": int(combined["is_st"].sum()),
        "source_columns_read": ["日期", "代码", "名称", "是否ST"],
        "performance_columns_read": [],
        "sources": receipts,
        "artifact": {
            "path": target.name,
            "sha256": _sha256(target),
            "bytes": target.stat().st_size,
            "rows": len(combined),
        },
    }
    manifest["manifest_payload_sha256"] = _canonical_sha256(manifest)
    manifest_path = output_root / "HISTORICAL_DAILY_ST_SOURCE_COMPLETE.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--worker-count", type=int, default=12)
    parser.add_argument("--file-limit", type=int)
    args = parser.parse_args()
    manifest = build_daily_st_source(
        input_root=args.input_root,
        expected_year=args.year,
        output_root=args.output_root,
        worker_count=args.worker_count,
        file_limit=args.file_limit,
    )
    print(
        json.dumps(
            {
                "status": manifest["status"],
                "row_count": manifest["row_count"],
                "manifest_payload_sha256": manifest["manifest_payload_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
