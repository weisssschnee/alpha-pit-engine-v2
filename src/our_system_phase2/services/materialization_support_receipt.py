"""Content-bound receipts for candidate-visible materialized roots.

The receipt does not materialize a field and does not qualify its economic
value.  It binds output from the existing Feature/State or PIT Fundamental
Fabric to development-only coordinates and proves that non-zero real support
exists before a virtual registry leaf can enter signal sketch or strict
evaluation.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


RECEIPT_SCHEMA = "cn_materialization_and_support_receipt_v1"
ALLOWED_RECEIPT_TYPES = {
    "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT",
    "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT",
}
SUPPLEMENTAL_RECEIPT_GATED_FIELDS = frozenset(
    {
        "fund_disclosure_balance_age_sessions",
        "fund_disclosure_profit_age_sessions",
        "fund_disclosure_cashflow_age_sessions",
        "fund_disclosure_holder_age_sessions",
        "state_close_range_location_sign",
    }
)
FULL_DEVELOPMENT_ASSEMBLY_BY_FIELD = {
    "fund_disclosure_balance_age_sessions": "FULL_DEVELOPMENT_SESSION_PANEL",
    "fund_disclosure_profit_age_sessions": "FULL_DEVELOPMENT_SESSION_PANEL",
    "fund_disclosure_cashflow_age_sessions": "FULL_DEVELOPMENT_SESSION_PANEL",
    "fund_disclosure_holder_age_sessions": "FULL_DEVELOPMENT_SESSION_PANEL",
    "state_close_range_location_sign": "FULL_DEVELOPMENT_ACTIVE_PANEL",
}
FULL_DEVELOPMENT_ROOT_AUTHORITY = "FULL_DEVELOPMENT_ROOT_AUTHORITY"
STAGE_MATERIALIZATION = "STAGE_MATERIALIZATION"


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(_stable_json(payload).encode("utf-8")).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_frame(path: Path, columns: Sequence[str]) -> pd.DataFrame:
    suffix = path.suffix.casefold()
    if suffix in {".parquet", ".pq"} or path.is_dir():
        return pd.read_parquet(path, columns=list(columns))
    if suffix in {".csv", ".txt"}:
        frame = pd.read_csv(path, usecols=list(columns))
        return frame
    raise ValueError(f"unsupported materialization evidence artifact: {path}")


def _frame_fingerprint(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str],
    row_keys: Sequence[str],
) -> str:
    selected = [*row_keys, *[column for column in columns if column not in row_keys]]
    if missing := set(selected) - set(frame.columns):
        raise ValueError(f"receipt frame missing columns: {sorted(missing)}")
    canonical = frame[selected].sort_values(list(row_keys), kind="mergesort").reset_index(
        drop=True
    )
    hashed = pd.util.hash_pandas_object(canonical, index=False).to_numpy(
        dtype=np.uint64, copy=False
    )
    digest = hashlib.sha256()
    digest.update("|".join(selected).encode("utf-8"))
    digest.update("|".join(str(canonical[name].dtype) for name in selected).encode("utf-8"))
    digest.update(hashed.tobytes())
    return digest.hexdigest()


def _time_field(frame: pd.DataFrame, row_keys: Sequence[str]) -> str:
    for field in ("trade_time", "session_time"):
        if field in row_keys and field in frame:
            return field
    raise ValueError("materialization receipt row keys need trade_time or session_time")


def materialization_frame_fingerprints(
    frame: pd.DataFrame,
    *,
    field_id: str,
    row_keys: Sequence[str],
) -> dict[str, str]:
    """Return the two digests a downstream reader must re-check."""

    keys = tuple(str(value) for value in row_keys)
    return {
        "coordinate_fingerprint": _frame_fingerprint(
            frame, columns=(), row_keys=keys
        ),
        "value_fingerprint": _frame_fingerprint(
            frame, columns=(field_id,), row_keys=keys
        ),
    }


def _episode_count(
    frame: pd.DataFrame,
    *,
    field_id: str,
    row_keys: Sequence[str],
) -> int:
    if "code" not in row_keys:
        return 0
    time_field = _time_field(frame, row_keys)
    work = frame[["code", time_field, field_id]].copy()
    work[time_field] = pd.to_datetime(work[time_field], errors="coerce")
    work["_session"] = work[time_field].dt.normalize()
    work["_value"] = pd.to_numeric(work[field_id], errors="coerce")
    work["_finite"] = np.isfinite(work["_value"].to_numpy(dtype=float))
    work = work.sort_values(
        ["code", "_session", time_field], kind="mergesort"
    )
    if not work["_finite"].any():
        return 0
    group = work.groupby(["code", "_session"], sort=False, dropna=False)
    previous = group["_value"].shift(1)
    previous_finite = group["_finite"].shift(1, fill_value=False)
    starts = work["_finite"] & (
        ~previous_finite | work["_value"].ne(previous)
    )
    return int(starts.sum())


def build_materialization_support_receipt(
    frame: pd.DataFrame,
    *,
    field_id: str,
    representation_id: str,
    registry_hash: str,
    receipt_type: str,
    materializer_authority: str,
    materializer_manifest: Mapping[str, Any],
    support_unit: str,
    observable_time_contract: str,
    maturity_contract: str,
    row_keys: Sequence[str],
    partition_identity: Mapping[str, Any] | None = None,
    source_binding: Mapping[str, Any] | None = None,
    access_ledger: Mapping[str, Any] | None = None,
    evidence_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a development-only receipt over real materialized values."""

    if receipt_type not in ALLOWED_RECEIPT_TYPES:
        raise ValueError(f"unsupported materialization receipt type: {receipt_type}")
    if re.fullmatch(r"[0-9a-f]{64}", str(registry_hash)) is None:
        raise ValueError("registry_hash must be a lower-case SHA-256 digest")
    if not field_id or not representation_id.startswith("cn.rep."):
        raise ValueError("receipt needs a field and registered representation identity")
    if not materializer_authority or not observable_time_contract or not maturity_contract:
        raise ValueError("receipt materializer and clock contracts must be explicit")
    keys = tuple(str(value) for value in row_keys)
    if not keys or frame.duplicated(list(keys)).any():
        raise ValueError("materialization receipt coordinates must be unique")
    time_field = _time_field(frame, keys)
    clocks = pd.to_datetime(frame[time_field], errors="coerce")
    if clocks.isna().any():
        raise ValueError("materialization receipt contains invalid coordinate clocks")
    if clocks.ge(pd.Timestamp("2026-01-01")).any():
        raise PermissionError("materialization receipt cannot bind sealed 2026 coordinates")

    values = pd.to_numeric(frame[field_id], errors="coerce")
    finite = np.isfinite(values.to_numpy(dtype=float))
    finite_count = int(finite.sum())
    row_count = int(len(frame))
    unique_count = int(values.loc[finite].nunique(dropna=True))
    episode_count = _episode_count(frame, field_id=field_id, row_keys=keys)
    session_count = int(clocks.loc[finite].dt.normalize().nunique())
    code_count = int(frame.loc[finite, "code"].astype(str).nunique()) if "code" in frame else 0
    support_count = episode_count if "episode" in support_unit.lower() else finite_count
    ledger = {
        "data_role": "development",
        "labels_or_returns_read": False,
        "reward_or_performance_used": False,
        "validation_rows_read": 0,
        "holdout_rows_read": 0,
        "forward_2026_rows_read": 0,
    }
    ledger.update(dict(access_ledger or {}))
    fingerprints = materialization_frame_fingerprints(
        frame, field_id=field_id, row_keys=keys
    )
    body: dict[str, Any] = {
        "receipt_schema": RECEIPT_SCHEMA,
        "receipt_type": receipt_type,
        "materialization_status": "MATERIALIZED_DEVELOPMENT_ONLY",
        "support_status": "NONZERO_SUPPORT_OBSERVED" if support_count > 0 else "ZERO_SUPPORT",
        "field_id": str(field_id),
        "representation_id": str(representation_id),
        "registry_hash": str(registry_hash),
        "materializer_authority": str(materializer_authority),
        "materializer_manifest_hash": _stable_hash(dict(materializer_manifest)),
        "observable_time_contract": str(observable_time_contract),
        "maturity_contract": str(maturity_contract),
        "coordinate_contract": {
            "row_keys": list(keys),
            "row_count": row_count,
            "coordinate_fingerprint": fingerprints["coordinate_fingerprint"],
            "partition_identity": dict(partition_identity or {}),
        },
        "materialization": {
            "value_fingerprint": fingerprints["value_fingerprint"],
            "finite_count": finite_count,
            "null_or_nonfinite_count": row_count - finite_count,
            "coverage": finite_count / max(1, row_count),
            "unique_count": unique_count,
        },
        "support": {
            "support_unit": str(support_unit),
            "support_count": support_count,
            "episode_count": episode_count,
            "session_count": session_count,
            "code_count": code_count,
        },
        "source_binding": dict(source_binding or {}),
        "access_ledger": ledger,
        "evidence_contract": dict(evidence_contract or {}),
    }
    body["receipt_hash"] = _stable_hash(body)
    return body


def verify_materialization_support_receipt(
    receipt: Mapping[str, Any],
    *,
    expected_field_id: str | None = None,
    expected_registry_hash: str | None = None,
    expected_coordinate_fingerprint: str | None = None,
    expected_partition_identity: Mapping[str, Any] | None = None,
    required_receipt_type: str | None = None,
) -> dict[str, Any]:
    """Fail closed unless a receipt is self-consistent and usable downstream."""

    row = dict(receipt)
    claimed_hash = str(row.pop("receipt_hash", ""))
    if re.fullmatch(r"[0-9a-f]{64}", claimed_hash) is None or _stable_hash(row) != claimed_hash:
        raise ValueError("materialization/support receipt self-hash mismatch")
    row["receipt_hash"] = claimed_hash
    if row.get("receipt_schema") != RECEIPT_SCHEMA:
        raise ValueError("unsupported materialization/support receipt schema")
    if row.get("receipt_type") not in ALLOWED_RECEIPT_TYPES:
        raise ValueError("unsupported materialization/support receipt type")
    if required_receipt_type and row.get("receipt_type") != required_receipt_type:
        raise ValueError("required materialization/support receipt type mismatch")
    if expected_field_id and row.get("field_id") != expected_field_id:
        raise ValueError("materialization/support receipt field mismatch")
    if expected_registry_hash and row.get("registry_hash") != expected_registry_hash:
        raise ValueError("materialization/support receipt registry mismatch")
    if row.get("materialization_status") != "MATERIALIZED_DEVELOPMENT_ONLY":
        raise ValueError("field does not have development-only materialization status")
    if row.get("support_status") != "NONZERO_SUPPORT_OBSERVED":
        raise ValueError("field has no real materialized support")

    coordinate = dict(row.get("coordinate_contract") or {})
    materialization = dict(row.get("materialization") or {})
    support = dict(row.get("support") or {})
    if expected_coordinate_fingerprint and coordinate.get("coordinate_fingerprint") != expected_coordinate_fingerprint:
        raise ValueError("materialization/support coordinate fingerprint mismatch")
    if expected_partition_identity is not None and coordinate.get("partition_identity") != dict(
        expected_partition_identity
    ):
        raise ValueError("materialization/support partition identity mismatch")
    row_count = int(coordinate.get("row_count") or 0)
    finite_count = int(materialization.get("finite_count") or 0)
    nonfinite_count = int(materialization.get("null_or_nonfinite_count") or 0)
    support_count = int(support.get("support_count") or 0)
    coverage = float(materialization.get("coverage") or 0.0)
    if row_count <= 0 or finite_count <= 0 or support_count <= 0:
        raise ValueError("materialization/support receipt has empty materialized support")
    if finite_count + nonfinite_count != row_count or not math.isclose(
        coverage, finite_count / row_count, rel_tol=0.0, abs_tol=1e-12
    ):
        raise ValueError("materialization/support receipt counts are inconsistent")
    if not str(row.get("observable_time_contract") or "") or not str(
        row.get("maturity_contract") or ""
    ):
        raise ValueError("materialization/support receipt lacks clock contracts")
    ledger = dict(row.get("access_ledger") or {})
    if ledger.get("data_role") != "development":
        raise PermissionError("materialization/support receipt is not development-only")
    forbidden_flags = ("labels_or_returns_read", "reward_or_performance_used")
    if any(ledger.get(name) is not False for name in forbidden_flags):
        raise PermissionError("materialization/support receipt used performance data")
    forbidden_counts = ("validation_rows_read", "holdout_rows_read", "forward_2026_rows_read")
    if any(int(ledger.get(name, -1)) != 0 for name in forbidden_counts):
        raise PermissionError("materialization/support receipt accessed restricted data")
    evidence = dict(row.get("evidence_contract") or {})
    role = str(evidence.get("evidence_role") or "")
    if role and role not in {FULL_DEVELOPMENT_ROOT_AUTHORITY, STAGE_MATERIALIZATION}:
        raise ValueError("materialization/support receipt has unsupported evidence role")
    if role == STAGE_MATERIALIZATION and re.fullmatch(
        r"[0-9a-f]{64}", str(evidence.get("parent_authority_receipt_hash") or "")
    ) is None:
        raise ValueError("stage materialization receipt lacks a full-authority parent")
    return row


def _binding_path(binding: Mapping[str, Any], *, label: str) -> Path:
    path = Path(str(binding.get("path") or "")).resolve()
    expected = str(binding.get("sha256") or "")
    if not path.is_file() or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
        raise ValueError(f"{label} binding is incomplete")
    if _sha256_path(path) != expected:
        raise ValueError(f"{label} artifact hash mismatch")
    return path


def _development_train_sessions(binding: Mapping[str, Any]) -> tuple[Path, set[pd.Timestamp]]:
    path = _binding_path(binding, label="frozen split manifest")
    frame = pd.read_csv(path)
    required = {"trade_date", "split"}
    if not required.issubset(frame.columns):
        raise ValueError("frozen split manifest lacks trade_date/split")
    train = pd.to_datetime(
        frame.loc[frame["split"].astype(str).eq("train"), "trade_date"], errors="coerce"
    ).dt.normalize()
    if train.empty or train.isna().any() or train.duplicated().any():
        raise ValueError("frozen split manifest has invalid development sessions")
    if train.min() < pd.Timestamp("2024-01-01") or train.max() >= pd.Timestamp("2026-01-01"):
        raise PermissionError("frozen split is not development 2024-2025 only")
    return path, set(pd.Timestamp(value) for value in train)


def verify_full_development_receipt_artifacts(
    receipt: Mapping[str, Any],
    *,
    expected_field_id: str,
    expected_registry_hash: str,
    expected_receipt_type: str,
    expected_assembly: str,
) -> dict[str, Any]:
    """Re-open every artifact behind a full-development root authority.

    A receipt self-hash proves only that one JSON document is internally
    consistent.  This verifier also proves that the frozen split, coordinate
    authority and actual materialized panel still exist and contain exactly
    the coordinates and values claimed by the receipt.
    """

    verified = verify_materialization_support_receipt(
        receipt,
        expected_field_id=expected_field_id,
        expected_registry_hash=expected_registry_hash,
        required_receipt_type=expected_receipt_type,
    )
    evidence = dict(verified.get("evidence_contract") or {})
    if evidence.get("evidence_role") != FULL_DEVELOPMENT_ROOT_AUTHORITY:
        raise ValueError("root is not backed by full-development authority evidence")
    partition = dict(verified["coordinate_contract"].get("partition_identity") or {})
    if partition.get("assembly") != expected_assembly:
        raise ValueError("root receipt assembly is not the frozen full-development assembly")

    _split_path, train_sessions = _development_train_sessions(
        dict(evidence.get("split_manifest") or {})
    )
    if expected_assembly == "FULL_DEVELOPMENT_ACTIVE_PANEL":
        release_binding = dict(evidence.get("development_release") or {})
        release_path = _binding_path(
            release_binding, label="development-only release manifest"
        )
        release = json.loads(release_path.read_text(encoding="utf-8"))
        release_hash = str(release.get("release_hash") or "")
        if (
            re.fullmatch(r"[0-9a-f]{64}", release_hash) is None
            or release_binding.get("release_hash") != release_hash
            or dict(verified.get("source_binding") or {}).get(
                "development_release_hash"
            )
            != release_hash
        ):
            raise ValueError("development release authority binding mismatch")
        if release.get("forbidden_roles_present") or bool(
            release.get("forward_2026_present")
        ):
            raise PermissionError("root authority release is not development-only")
    coordinate_binding = dict(evidence.get("coordinate_authority") or {})
    materialized_binding = dict(evidence.get("materialized_artifact") or {})
    coordinate_path = _binding_path(coordinate_binding, label="full coordinate authority")
    materialized_path = _binding_path(materialized_binding, label="materialized panel")
    row_keys = tuple(str(value) for value in verified["coordinate_contract"].get("row_keys", ()))
    if not row_keys:
        raise ValueError("full-development receipt lacks row keys")
    coordinate_frame = _read_frame(coordinate_path, row_keys)
    materialized_frame = _read_frame(materialized_path, (*row_keys, expected_field_id))
    coordinate_fingerprint = materialization_frame_fingerprints(
        coordinate_frame.assign(**{expected_field_id: 0.0}),
        field_id=expected_field_id,
        row_keys=row_keys,
    )["coordinate_fingerprint"]
    fingerprints = materialization_frame_fingerprints(
        materialized_frame,
        field_id=expected_field_id,
        row_keys=row_keys,
    )
    claimed_coordinate = str(
        verified["coordinate_contract"].get("coordinate_fingerprint") or ""
    )
    if (
        len(coordinate_frame) != int(verified["coordinate_contract"].get("row_count") or 0)
        or len(materialized_frame) != len(coordinate_frame)
        or coordinate_fingerprint != claimed_coordinate
        or fingerprints["coordinate_fingerprint"] != claimed_coordinate
        or fingerprints["value_fingerprint"]
        != str(verified["materialization"].get("value_fingerprint") or "")
    ):
        raise ValueError("full-development coordinate or materialized content mismatch")
    time_field = _time_field(materialized_frame, row_keys)
    observed_sessions = set(
        pd.to_datetime(materialized_frame[time_field], errors="coerce").dt.normalize()
    )
    if pd.NaT in observed_sessions or observed_sessions != train_sessions:
        raise PermissionError(
            "materialized panel does not exactly cover frozen development/train sessions"
        )
    source = dict(verified.get("source_binding") or {})
    if source.get("split_manifest_sha256") != evidence["split_manifest"].get("sha256"):
        raise ValueError("root receipt does not bind the frozen split hash")
    maximum = pd.to_datetime(source.get("maximum_observable_time"), errors="coerce")
    if pd.isna(maximum) or pd.Timestamp(maximum) >= pd.Timestamp("2026-01-01"):
        raise PermissionError("root receipt observable boundary is not 2024-2025 sealed")
    return verified


def load_verified_full_development_receipts(
    receipt_paths: Sequence[Path],
    *,
    registry: Any,
) -> dict[str, dict[str, str]]:
    """Build the canonical per-root catalog used by generation and strict gates."""

    bindings: dict[str, dict[str, str]] = {}
    for raw_path in receipt_paths:
        path = Path(raw_path).resolve()
        raw = json.loads(path.read_text(encoding="utf-8"))
        field_id = str(raw.get("field_id") or "")
        if field_id not in FULL_DEVELOPMENT_ASSEMBLY_BY_FIELD:
            raise ValueError(f"receipt does not cover a gated supplemental root: {field_id}")
        if field_id in bindings:
            raise ValueError(f"duplicate full-development root receipt: {field_id}")
        capability = registry.resolve(field_id)
        receipt_type = (
            "FEATURE_STATE_FABRIC_MATERIALIZATION_AND_SUPPORT_RECEIPT"
            if field_id == "state_close_range_location_sign"
            else "PIT_SESSION_ASOF_MATERIALIZATION_AND_SUPPORT_RECEIPT"
        )
        verified = verify_full_development_receipt_artifacts(
            raw,
            expected_field_id=field_id,
            expected_registry_hash=registry.registry_hash,
            expected_receipt_type=receipt_type,
            expected_assembly=FULL_DEVELOPMENT_ASSEMBLY_BY_FIELD[field_id],
        )
        if (
            verified.get("representation_id") != capability.representation_id
            or verified.get("observable_time_contract") != capability.observable_clock
            or verified.get("maturity_contract") != capability.maturity_rule
            or verified["support"].get("support_unit") != capability.support_unit
            or verified.get("source_binding", {}).get("pit_status") != capability.pit_status
        ):
            raise ValueError(f"full-development PIT/representation contract drift: {field_id}")
        bindings[field_id] = {
            "receipt_hash": str(verified["receipt_hash"]),
            "receipt_sha256": _sha256_path(path),
            "path": str(path),
        }
    return bindings


def assert_candidate_materialization_receipts(
    candidate: Mapping[str, Any],
    *,
    eligibility_field: str,
    stage_name: str,
    verified_receipt_hashes: Mapping[str, str] | None = None,
    require_verified_catalog: bool = False,
) -> None:
    """Require one verified receipt hash for every gated root a row consumes.

    Receipt content is verified at the runtime boundary.  This second, cheap
    guard makes the resulting per-root hash binding mandatory on every
    candidate row that can enter sketch or strict evaluation.
    """

    declared = {str(value) for value in candidate.get("declared_field_ids", ())}
    gated = declared & set(SUPPLEMENTAL_RECEIPT_GATED_FIELDS)
    if not gated:
        return
    bindings = {
        str(field_id): str(receipt_hash)
        for field_id, receipt_hash in dict(
            candidate.get("materialization_support_receipt_hashes") or {}
        ).items()
    }
    bound = set(bindings)
    hashes_valid = all(
        re.fullmatch(r"[0-9a-f]{64}", bindings[field_id]) is not None
        for field_id in gated & bound
    )
    catalog = {
        str(field_id): str(receipt_hash)
        for field_id, receipt_hash in dict(verified_receipt_hashes or {}).items()
    }
    catalog_matches = all(catalog.get(field_id) == bindings.get(field_id) for field_id in gated)
    if (
        candidate.get("runtime_ready") is not True
        or candidate.get("materialization_status") != "MATERIALIZED_DEVELOPMENT_ONLY"
        or candidate.get(eligibility_field) is not True
        or bound != gated
        or not hashes_valid
        or (require_verified_catalog and (not catalog or not catalog_matches))
    ):
        raise RuntimeError(
            "supplemental candidate lacks exact per-root materialization/support "
            f"receipt binding and cannot enter {stage_name}: "
            f"{candidate.get('candidate_id')}"
        )
