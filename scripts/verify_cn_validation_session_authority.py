"""Independently verify report-only validation session authority evidence."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any

import pandas as pd

from scripts import build_cn_finalist_session_authority as base
from scripts import build_cn_portfolio_decoder_autopsy_v1 as v1
from scripts import build_cn_validation_session_authority as build


def verify_validation_session_authority(
    *, authority_root: Path, output_root: Path
) -> dict[str, Any]:
    authority_root = authority_root.resolve()
    output_root = output_root.resolve()
    output_root.mkdir(parents=True, exist_ok=False)
    manifest_path = authority_root / "validation_session_authority_manifest.json"
    manifest = v1._read_json(manifest_path)
    payload = dict(manifest)
    expected_payload_sha = str(payload.pop("manifest_payload_sha256", ""))
    if not expected_payload_sha or v1._stable_hash(payload) != expected_payload_sha:
        raise RuntimeError("validation session authority self-hash mismatch")
    required = {
        "status": build.STATUS,
        "evaluation_role": "validation",
        "data_role": "validation_report_only",
        "date_min": build.DATE_MIN,
        "date_max": build.DATE_MAX,
        "allowed_exchanges": list(build.ALLOWED_EXCHANGES),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "optimizer_feedback_write": "FORBIDDEN",
        "scheduler_write": "FORBIDDEN",
        "archive_write": "FORBIDDEN",
        "promotion": "FORBIDDEN",
    }
    drift = [key for key, value in required.items() if manifest.get(key) != value]
    if drift:
        raise RuntimeError("validation session authority drift: " + ",".join(drift))
    if int(manifest.get("validation_reads") or 0) <= 0:
        raise RuntimeError("validation session authority has no validation reads")
    if list(manifest.get("columns") or ()) != list(base.SESSION_AUTHORITY_COLUMNS):
        raise RuntimeError("validation session authority column drift")
    for artifact in manifest.get("artifacts") or ():
        path = authority_root / str(artifact["path"])
        if not path.is_file() or path.stat().st_size != int(artifact["bytes"]):
            raise RuntimeError(f"validation authority artifact size drift: {path}")
        if v1._sha256(path) != str(artifact["sha256"]):
            raise RuntimeError(f"validation authority artifact hash drift: {path}")

    field_path = Path(str(manifest["field_manifest"])).resolve()
    if v1._sha256(field_path) != str(manifest["field_manifest_sha256"]):
        raise RuntimeError("validation field manifest binding drift")
    field = v1._read_json(field_path)
    if (
        field.get("status") != "TIME_MAJOR_LAYOUT_PARITY_PASS"
        or field.get("evaluation_role") != "validation"
        or field.get("data_role") != "validation_report_only"
        or int(field.get("holdout_reads", -1)) != 0
        or int(field.get("forward_2026_reads", -1)) != 0
    ):
        raise RuntimeError("validation field evidence drift")
    source_root = Path(str(manifest["public_source_snapshot_manifest"])).parent
    source = base.verify_source_snapshot(source_root)
    if source["manifest_file_sha256"] != str(
        manifest["public_source_snapshot_manifest_sha256"]
    ):
        raise RuntimeError("public source snapshot binding drift")

    authority = pd.read_parquet(
        authority_root / "validation_session_authority.parquet"
    )
    observed = pd.read_parquet(
        authority_root / "validation_observed_sessions.parquet"
    )
    excluded = v1._read_json(
        authority_root / "excluded_validation_codes.json"
    )
    for frame in (authority, observed):
        frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.normalize()
        frame["code"] = frame["code"].map(base.normalize_code)
    if authority.duplicated(["date", "code"]).any():
        raise RuntimeError("validation authority duplicate coordinates")
    if observed.duplicated(["date", "code"]).any():
        raise RuntimeError("validation observations duplicate coordinates")
    if set(authority["exchange"].astype(str)) - set(build.ALLOWED_EXCHANGES):
        raise RuntimeError("validation authority contains a prohibited exchange")
    dates = pd.DatetimeIndex(authority["date"].unique()).sort_values()
    if dates[0] != pd.Timestamp(build.DATE_MIN) or dates[-1] != pd.Timestamp(
        build.DATE_MAX
    ):
        raise RuntimeError("validation authority calendar boundary drift")
    if len(dates) != int(manifest["validation_date_count"]):
        raise RuntimeError("validation authority date cardinality drift")
    if len(authority) != int(manifest["authority_session_row_count"]):
        raise RuntimeError("validation authority row cardinality drift")
    if len(observed) != int(manifest["observed_session_row_count"]):
        raise RuntimeError("validation observation row cardinality drift")
    if observed["is_st"].isna().any():
        raise RuntimeError("validation observation ST state is not fail-closed")
    joined = observed.merge(
        authority,
        on=["date", "code"],
        how="left",
        suffixes=("_observed", "_authority"),
        validate="one_to_one",
    )
    if joined["exchange"].isna().any():
        raise RuntimeError("validation observations are absent from authority")
    if not joined["is_st_observed"].eq(joined["is_st_authority"]).all():
        raise RuntimeError("validation observed ST state parity failure")
    if joined["suspended"].any():
        raise RuntimeError("observed validation session marked suspended")
    if int(excluded["excluded_non_sse_szse_code_count"]) != int(
        manifest["excluded_non_sse_szse_code_count"]
    ):
        raise RuntimeError("validation exchange exclusion count drift")
    if any(
        not str(code).startswith(("4", "8", "9"))
        for code in excluded.get("excluded_non_sse_szse_codes") or ()
    ):
        raise RuntimeError("validation exclusion is not BSE-family explicit")
    if int(excluded["excluded_missing_corporate_action_source_code_count"]) != int(
        manifest["excluded_missing_corporate_action_source_code_count"]
    ):
        raise RuntimeError("validation corporate-action exclusion count drift")
    if int(
        excluded["excluded_incomplete_corporate_action_source_code_count"]
    ) != int(manifest["excluded_incomplete_corporate_action_source_code_count"]):
        raise RuntimeError("validation incomplete-action exclusion count drift")
    expected_total = (
        int(excluded["excluded_non_sse_szse_code_count"])
        + int(excluded["excluded_missing_corporate_action_source_code_count"])
        + int(excluded["excluded_incomplete_corporate_action_source_code_count"])
    )
    if int(excluded["excluded_code_count"]) != expected_total:
        raise RuntimeError("validation total exclusion count drift")

    receipt = {
        "schema_version": "cn_validation_session_authority_audit_v1",
        "status": "PASS_INDEPENDENT_VALIDATION_SESSION_AUTHORITY_VERIFICATION",
        "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "authority_manifest_sha256": v1._sha256(manifest_path),
        "authority_manifest_payload_sha256": expected_payload_sha,
        "authority_session_row_count": len(authority),
        "observed_session_row_count": len(observed),
        "validation_date_count": len(dates),
        "excluded_non_sse_szse_code_count": int(
            excluded["excluded_non_sse_szse_code_count"]
        ),
        "excluded_missing_corporate_action_source_code_count": int(
            excluded["excluded_missing_corporate_action_source_code_count"]
        ),
        "excluded_incomplete_corporate_action_source_code_count": int(
            excluded["excluded_incomplete_corporate_action_source_code_count"]
        ),
        "identity_calendar_status": "PASS",
        "observed_st_parity_status": "PASS",
        "source_artifact_verification_status": "PASS",
        "validation_reads": int(manifest["validation_reads"]),
        "holdout_reads": 0,
        "forward_2026_reads": 0,
        "promotion_authorized": False,
    }
    receipt["receipt_payload_sha256"] = v1._stable_hash(receipt)
    receipt_path = v1._write_json(output_root / "audit.json", receipt)
    return {
        **receipt,
        "audit_path": str(receipt_path),
        "audit_file_sha256": v1._sha256(receipt_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args()
    result = verify_validation_session_authority(
        authority_root=args.authority_root, output_root=args.output_root
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
