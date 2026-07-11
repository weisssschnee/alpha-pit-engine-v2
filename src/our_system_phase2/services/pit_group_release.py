"""Build auditable PIT plate/industry releases without performance feedback."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from our_system_phase2.services.pit_group_sidecar import (
    PITGroupContract,
    canonical_membership,
    membership_content_sha256,
    membership_manifest,
)


PIT_GROUP_RELEASE_VERSION = "nextgen_dark_pit_group_release_v1"
FORWARD_SEALED_FROM = pd.Timestamp("2026-01-01")
_FORBIDDEN_COLUMN_TOKENS = {
    "reward", "return", "label", "target", "performance", "validation",
    "holdout", "oos", "forward", "winner", "rank", "sortino", "sharpe",
}


@dataclass(frozen=True, slots=True)
class PITGroupReleaseSpec:
    source_name: str
    source_version: str
    source_uri: str
    source_artifact_sha256: str
    retrieved_at: str
    group_type: str
    membership_policy: str
    required_start: str
    required_end: str
    maximum_observable_time: str

    def contract(self) -> PITGroupContract:
        contract = PITGroupContract(
            source_name=self.source_name,
            source_version=self.source_version,
            group_type=self.group_type,
            membership_policy=self.membership_policy,
        )
        contract.validate()
        return contract

    def timestamps(self) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
        if not re.fullmatch(r"[0-9a-f]{64}", self.source_artifact_sha256.lower()):
            raise ValueError("source_artifact_sha256 must be a SHA-256 hex digest")
        start = pd.Timestamp(self.required_start)
        end = pd.Timestamp(self.required_end)
        maximum = pd.Timestamp(self.maximum_observable_time)
        if end <= start:
            raise ValueError("required_end must be after required_start")
        if maximum < end:
            raise ValueError("maximum_observable_time cannot precede required_end")
        if end >= FORWARD_SEALED_FROM or maximum >= FORWARD_SEALED_FROM:
            raise ValueError("2026 forward boundary is sealed in NEXTGEN-DARK")
        return start, end, maximum


def _reject_feedback_columns(frame: pd.DataFrame) -> None:
    bad = sorted(
        column
        for column in frame.columns
        if any(token in str(column).strip().lower() for token in _FORBIDDEN_COLUMN_TOKENS)
    )
    if bad:
        raise ValueError(f"performance/evaluation columns are forbidden in PIT membership: {bad}")


def _normalize_cn_code(value: Any) -> str:
    text = str(value).strip().upper()
    digits = re.findall(r"\d{6}", text)
    if len(digits) != 1:
        raise ValueError(f"cannot normalize CN security code: {value!r}")
    return digits[0]


def _prepare_source(frame: pd.DataFrame) -> pd.DataFrame:
    _reject_feedback_columns(frame)
    if frame.empty:
        raise ValueError("membership source is empty")
    out = frame.copy()
    if "code" not in out.columns:
        raise ValueError("membership source requires code")
    out["code"] = out["code"].map(_normalize_cn_code)
    return out


def _release_manifest(
    canonical: pd.DataFrame,
    spec: PITGroupReleaseSpec,
    *,
    input_mode: str,
    snapshot_count: int | None = None,
) -> dict[str, Any]:
    start, end, maximum = spec.timestamps()
    observable_from = canonical[["effective_from", "source_observed_at"]].max(axis=1)
    observable_to = pd.Series(pd.NaT, index=canonical.index, dtype="datetime64[ns]")
    observed_exit = canonical["source_observed_to"].notna()
    observable_to.loc[observed_exit] = canonical.loc[
        observed_exit, ["effective_to", "source_observed_to"]
    ].max(axis=1)
    if observable_from.max() > maximum:
        raise ValueError("membership observation crosses sealed boundary")
    if canonical["source_observed_to"].dropna().gt(maximum).any():
        raise ValueError("membership exit observation crosses sealed boundary")
    covers_start = bool(observable_from.min() <= start)
    covers_end = bool(observable_to.isna().any() or observable_to.max() >= end)
    if not (covers_start and covers_end):
        raise ValueError(
            f"release does not cover required PIT interval {start.isoformat()}..{end.isoformat()}"
        )

    exit_event_count = int(canonical["effective_to"].notna().sum())
    if input_mode == "effective_intervals" and exit_event_count == 0:
        raise ValueError(
            "effective-interval release needs historical churn/exit evidence; "
            "an open-only current snapshot is not PIT history"
        )

    manifest = membership_manifest(canonical, spec.contract())
    manifest.update(
        {
            "release_version": PIT_GROUP_RELEASE_VERSION,
            "input_mode": input_mode,
            "source_uri": spec.source_uri,
            "source_artifact_sha256": spec.source_artifact_sha256.lower(),
            "retrieved_at": spec.retrieved_at,
            "required_start": start.isoformat(),
            "required_end": end.isoformat(),
            "maximum_observable_time": maximum.isoformat(),
            "coverage_gate": "PASS",
            "observed_membership_start": observable_from.min().isoformat(),
            "open_interval_count": int(canonical["effective_to"].isna().sum()),
            "entry_event_count": int(len(canonical)),
            "exit_event_count": exit_event_count,
            "distinct_security_count": int(canonical["code"].nunique()),
            "distinct_group_count": int(canonical["group_id"].nunique()),
            "reward_or_performance_used": False,
            "survivorship_guard": exit_event_count > 0,
            "spec": asdict(spec),
        }
    )
    if snapshot_count is not None:
        manifest["snapshot_count"] = snapshot_count
    return manifest


def build_interval_release(
    membership: pd.DataFrame,
    spec: PITGroupReleaseSpec,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = _prepare_source(membership)
    canonical = canonical_membership(source, spec.contract())
    manifest = _release_manifest(canonical, spec, input_mode="effective_intervals")
    return canonical, manifest


def build_snapshot_release(
    snapshots: pd.DataFrame,
    spec: PITGroupReleaseSpec,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    source = _prepare_source(snapshots)
    required = {"group_id", "snapshot_at", "source_observed_at"}
    missing = sorted(required - set(source.columns))
    if missing:
        raise ValueError(f"historical snapshots missing columns: {missing}")
    source["group_id"] = source["group_id"].astype(str).str.strip()
    source["snapshot_at"] = pd.to_datetime(source["snapshot_at"], errors="coerce", format="mixed")
    source["source_observed_at"] = pd.to_datetime(
        source["source_observed_at"], errors="coerce", format="mixed"
    )
    if source[["snapshot_at", "source_observed_at"]].isna().any().any():
        raise ValueError("snapshot_at/source_observed_at must be valid timestamps")
    if source.duplicated(["snapshot_at", "code", "group_id"]).any():
        raise ValueError("duplicate membership within a historical snapshot")

    clock = (
        source.groupby("snapshot_at", as_index=False, sort=True)["source_observed_at"]
        .max()
        .sort_values("snapshot_at", kind="mergesort")
        .reset_index(drop=True)
    )
    if len(clock) < 2:
        raise ValueError("at least two historical snapshots are required; a current snapshot is not PIT history")
    start, end, maximum = spec.timestamps()
    if clock.iloc[0]["snapshot_at"] > start or clock.iloc[-1]["snapshot_at"] < end:
        raise ValueError("historical snapshots do not span the required PIT interval")
    if clock["source_observed_at"].max() > maximum:
        raise ValueError("historical snapshot observation crosses sealed boundary")
    if spec.membership_policy == "single":
        counts = source.groupby(["snapshot_at", "code"], sort=False)["group_id"].nunique()
        if counts.gt(1).any():
            raise ValueError("single-valued historical snapshot has multiple groups for one security")

    present = set(zip(source["snapshot_at"], source["code"], source["group_id"]))
    records: list[dict[str, Any]] = []
    snapshot_rows = list(clock.itertuples(index=False))
    for code, group_id in sorted(set(zip(source["code"], source["group_id"]))):
        entry_snapshot: pd.Timestamp | None = None
        entry_observed: pd.Timestamp | None = None
        for snapshot in snapshot_rows:
            is_present = (snapshot.snapshot_at, code, group_id) in present
            if is_present and entry_snapshot is None:
                entry_snapshot = snapshot.snapshot_at
                entry_observed = snapshot.source_observed_at
            elif not is_present and entry_snapshot is not None:
                records.append(
                    {
                        "code": code,
                        "group_id": group_id,
                        "effective_from": entry_snapshot,
                        "effective_to": snapshot.snapshot_at,
                        "source_observed_at": entry_observed,
                        "source_observed_to": snapshot.source_observed_at,
                    }
                )
                entry_snapshot = None
                entry_observed = None
        if entry_snapshot is not None:
            records.append(
                {
                    "code": code,
                    "group_id": group_id,
                    "effective_from": entry_snapshot,
                    "effective_to": pd.NaT,
                    "source_observed_at": entry_observed,
                    "source_observed_to": pd.NaT,
                }
            )

    canonical = canonical_membership(pd.DataFrame.from_records(records), spec.contract())
    manifest = _release_manifest(
        canonical,
        spec,
        input_mode="historical_snapshots",
        snapshot_count=len(clock),
    )
    return canonical, manifest


def _atomic_replace(write: Any, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    os.close(handle)
    temporary = Path(temporary_name)
    try:
        write(temporary)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def _file_sha256(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _manifest_sha256(manifest: dict[str, Any]) -> str:
    payload = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def write_release(
    membership: pd.DataFrame,
    manifest: dict[str, Any],
    output_directory: str | Path,
    *,
    source_path: str | Path,
) -> dict[str, Path]:
    actual_source_hash = _file_sha256(source_path)
    if actual_source_hash != manifest.get("source_artifact_sha256"):
        raise ValueError("PIT source artifact hash mismatch at release write")
    committed_manifest = dict(manifest)
    committed_manifest["source_artifact_verified_at_write"] = True
    committed_manifest["manifest_sha256"] = _manifest_sha256(committed_manifest)
    output = Path(output_directory)
    membership_path = output / "pit_group_membership.parquet"
    manifest_path = output / "pit_group_membership.manifest.json"
    _atomic_replace(lambda path: membership.to_parquet(path, index=False), membership_path)
    _atomic_replace(
        lambda path: path.write_text(
            json.dumps(committed_manifest, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        ),
        manifest_path,
    )
    return {"membership": membership_path, "manifest": manifest_path}


def load_verified_release(
    membership_path: str | Path,
    manifest_path: str | Path,
    *,
    source_path: str | Path | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    membership = pd.read_parquet(membership_path)
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if manifest.get("manifest_sha256") != _manifest_sha256(manifest):
        raise ValueError("PIT membership manifest hash mismatch")
    if manifest.get("source_artifact_verified_at_write") is not True:
        raise ValueError("PIT source artifact was not verified at release write")
    if source_path is not None and _file_sha256(source_path) != manifest.get("source_artifact_sha256"):
        raise ValueError("PIT source artifact hash mismatch at release read")
    spec = PITGroupReleaseSpec(**manifest["spec"])
    canonical = canonical_membership(membership, spec.contract())
    actual = membership_content_sha256(canonical, spec.contract())
    if actual != manifest.get("membership_sha256"):
        raise ValueError("PIT membership canonical hash mismatch")
    if manifest.get("coverage_gate") != "PASS" or manifest.get("reward_or_performance_used") is not False:
        raise ValueError("PIT membership manifest is not admissible")
    return canonical, manifest
