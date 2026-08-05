"""Verify sessionized archive values against the official minute release."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re

import numpy as np
import pandas as pd
import polars as pl


STATUS = "SESSIONIZED_ARCHIVE_PARITY_PASS"


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
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _normalize_code(value: object) -> str:
    text = str(value)
    match = re.search(r"(\d{6})", text)
    if match is None:
        raise ValueError(f"invalid reference code: {value!r}")
    suffix = "SH" if match.group(1).startswith(("5", "6", "9")) else "SZ"
    return f"{match.group(1)}.{suffix}"


def verify_parity(
    *,
    sessionized_root: Path,
    expected_manifest_sha256: str,
    reference_root: Path,
    reference_pattern: str,
    output_path: Path,
    price_atol: float = 1e-4,
    amount_atol: float = 256.0,
    amount_rtol: float = 1e-6,
) -> dict:
    sessionized_root = sessionized_root.resolve()
    manifest_path = sessionized_root / "SESSIONIZED_ARCHIVE_COMPLETE.json"
    if _sha256(manifest_path) != expected_manifest_sha256:
        raise RuntimeError("sessionized manifest hash drift")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifacts = list(manifest.get("artifacts") or ())
    paths = [sessionized_root / str(item["path"]) for item in artifacts]
    for path, item in zip(paths, artifacts):
        if _sha256(path) != str(item["sha256"]):
            raise RuntimeError(f"sessionized artifact hash drift: {path}")
    observed = pd.concat([pd.read_parquet(path) for path in paths], ignore_index=True)
    observed["code"] = observed["code"].map(_normalize_code)
    observed["date"] = pd.to_datetime(observed["date"], errors="raise").dt.normalize()
    observed = observed.sort_values(["date", "code"], kind="mergesort").reset_index(drop=True)
    if observed.duplicated(["date", "code"]).any():
        raise RuntimeError("sessionized parity input has duplicate coordinates")
    codes = tuple(sorted(observed["code"].unique()))
    dates = tuple(pd.DatetimeIndex(observed["date"].unique()).date)
    reference_paths = sorted(reference_root.resolve().glob(reference_pattern))
    if not reference_paths:
        raise FileNotFoundError("reference release has no selected shards")
    lazy = pl.concat(
        [
            pl.scan_parquet(path, low_memory=True).select(
                "code", "trade_time", "open", "high", "low", "close", "vol", "amount"
            )
            for path in reference_paths
        ],
        how="vertical_relaxed",
    )
    reference = (
        lazy.with_columns(
            pl.col("code")
            .cast(pl.String)
            .str.extract(r"(\d{6})", 1)
            .map_elements(_normalize_code, return_dtype=pl.String)
            .alias("code"),
            pl.col("trade_time").dt.date().alias("date"),
        )
        .filter(pl.col("code").is_in(codes) & pl.col("date").is_in(dates))
        .group_by("code", "date")
        .agg(
            pl.col("open").sort_by("trade_time").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").sort_by("trade_time").last().alias("close"),
            pl.col("vol").sum().alias("vol"),
            pl.col("amount").sum().alias("amount"),
        )
        .sort("date", "code")
        .collect(engine="streaming")
        .to_pandas()
    )
    reference["date"] = pd.to_datetime(reference["date"], errors="raise").dt.normalize()
    if len(reference) != len(observed):
        raise RuntimeError(
            f"parity coordinate count mismatch: observed={len(observed)} reference={len(reference)}"
        )
    keys = ["date", "code"]
    observed_keys = list(observed[keys].itertuples(index=False, name=None))
    reference_keys = list(reference[keys].itertuples(index=False, name=None))
    if observed_keys != reference_keys:
        raise RuntimeError("sessionized/reference coordinate identity or order mismatch")
    max_abs: dict[str, float] = {}
    for field in ("open", "high", "low", "close"):
        left = observed[field].to_numpy(dtype=float)
        right = reference[field].to_numpy(dtype=float)
        max_abs[field] = float(np.max(np.abs(left - right))) if len(left) else 0.0
        if not np.allclose(left, right, rtol=1e-6, atol=float(price_atol), equal_nan=False):
            raise RuntimeError(f"sessionized/reference {field} parity failure")
    left_volume = observed["vol"].to_numpy(dtype=np.int64)
    right_volume = np.rint(reference["vol"].to_numpy(dtype=float)).astype(np.int64)
    max_abs["vol"] = float(np.max(np.abs(left_volume - right_volume))) if len(left_volume) else 0.0
    if not np.array_equal(left_volume, right_volume):
        raise RuntimeError("sessionized/reference volume parity failure")
    left_amount = observed["amount"].to_numpy(dtype=float)
    right_amount = reference["amount"].to_numpy(dtype=float)
    max_abs["amount"] = (
        float(np.max(np.abs(left_amount - right_amount))) if len(left_amount) else 0.0
    )
    if not np.allclose(
        left_amount,
        right_amount,
        rtol=float(amount_rtol),
        atol=float(amount_atol),
        equal_nan=False,
    ):
        raise RuntimeError("sessionized/reference amount parity failure")
    receipt = {
        "schema_version": "cn_sessionized_archive_parity_v1",
        "status": STATUS,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sessionized_manifest": str(manifest_path),
        "sessionized_manifest_sha256": expected_manifest_sha256,
        "reference_root": str(reference_root.resolve()),
        "reference_pattern": reference_pattern,
        "reference_shard_count": len(reference_paths),
        "coordinate_count": len(observed),
        "security_count": len(codes),
        "date_count": len(dates),
        "max_absolute_differences": max_abs,
        "tolerances": {
            "price_atol": float(price_atol),
            "price_rtol": 1e-6,
            "amount_atol": float(amount_atol),
            "amount_rtol": float(amount_rtol),
            "volume": "EXACT_INTEGER",
        },
    }
    receipt["manifest_payload_sha256"] = _canonical_sha256(receipt)
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sessionized-root", type=Path, required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--reference-root", type=Path, required=True)
    parser.add_argument("--reference-pattern", default="*.parquet")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--price-atol", type=float, default=1e-4)
    parser.add_argument("--amount-atol", type=float, default=256.0)
    parser.add_argument("--amount-rtol", type=float, default=1e-6)
    args = parser.parse_args()
    receipt = verify_parity(
        sessionized_root=args.sessionized_root,
        expected_manifest_sha256=args.expected_manifest_sha256,
        reference_root=args.reference_root,
        reference_pattern=args.reference_pattern,
        output_path=args.output,
        price_atol=args.price_atol,
        amount_atol=args.amount_atol,
        amount_rtol=args.amount_rtol,
    )
    print(json.dumps({"status": receipt["status"], "coordinate_count": receipt["coordinate_count"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
