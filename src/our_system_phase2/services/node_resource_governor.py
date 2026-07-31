"""Cross-process resource admission for the shared 77o alpha compute node.

The governor is deliberately small: it owns admission and evidence, not job
orchestration.  Every heavy launcher acquires one lease from the same state
root before financial work.  A dead owner is reclaimed on the next admission.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import socket
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping


class NodeResourceAdmissionError(RuntimeError):
    """A node cannot safely admit the requested heavy workload."""


class NodeResourceLeaseDriftError(RuntimeError):
    """A runtime lease no longer matches its content-addressed receipt."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8-sig"))


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    ) + "\n"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=target.parent,
        prefix=target.name + ".",
        suffix=".tmp",
        delete=False,
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    os.replace(temporary, target)


@contextlib.contextmanager
def _exclusive_lock(path: Path) -> Iterator[None]:
    lock_path = Path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _process_create_time(pid: int) -> float:
    try:
        import psutil  # type: ignore[import-not-found]

        return float(psutil.Process(int(pid)).create_time())
    except Exception as exc:
        raise NodeResourceAdmissionError(
            f"cannot resolve owner process {int(pid)}"
        ) from exc


def _owner_is_alive(lease: Mapping[str, Any]) -> bool:
    try:
        observed = _process_create_time(int(lease["owner_pid"]))
    except NodeResourceAdmissionError:
        return False
    expected = float(lease.get("owner_create_time") or 0.0)
    return abs(observed - expected) < 1.0


def _workload_process_is_alive(lease: Mapping[str, Any]) -> bool:
    workload_id = str(lease.get("workload_id") or "").strip()
    if not workload_id:
        return False
    try:
        import psutil  # type: ignore[import-not-found]

        for process in psutil.process_iter(attrs=("pid", "cmdline")):
            try:
                command_line = " ".join(
                    str(value) for value in (process.info.get("cmdline") or ())
                )
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                continue
            if workload_id in command_line:
                return True
    except Exception:
        return False
    return False


def _lease_is_active(lease: Mapping[str, Any]) -> bool:
    # The launcher is the normal owner.  The workload binding protects against
    # oversubscription when that guardian dies but its heavy child survives.
    return _owner_is_alive(lease) or _workload_process_is_alive(lease)


def _available_memory_bytes() -> int:
    try:
        import psutil  # type: ignore[import-not-found]

        return int(psutil.virtual_memory().available)
    except Exception:
        return 0


def _validate_lease_state(
    state: Mapping[str, Any],
    *,
    capacity_manifest_sha256: str,
) -> None:
    if str(state.get("schema_version") or "") != "cn_node_resource_lease_state_v1":
        raise NodeResourceLeaseDriftError("node resource lease state schema drift")
    claimed = str(state.get("state_sha256") or "")
    body = dict(state)
    body.pop("state_sha256", None)
    if claimed != _stable_hash(body):
        raise NodeResourceLeaseDriftError("node resource lease state self-hash drift")
    if str(state.get("authorized_host") or "").upper() != socket.gethostname().upper():
        raise NodeResourceLeaseDriftError("node resource lease state host drift")
    if (
        str(state.get("capacity_manifest_sha256") or "")
        != str(capacity_manifest_sha256)
    ):
        raise NodeResourceLeaseDriftError(
            "node resource lease state capacity authority drift"
        )


@dataclass(frozen=True, slots=True)
class NodeResourceProfile:
    profile_id: str
    role: str
    cpu_threads: int
    memory_claim_bytes: int
    minimum_free_memory_bytes: int

    @classmethod
    def from_manifest(
        cls,
        manifest: Mapping[str, Any],
        profile_id: str,
    ) -> "NodeResourceProfile":
        profiles = dict(manifest.get("profiles") or {})
        if str(profile_id) not in profiles:
            raise NodeResourceAdmissionError(
                f"unknown node resource profile: {profile_id}"
            )
        row = dict(profiles[str(profile_id)] or {})
        profile = cls(
            profile_id=str(profile_id),
            role=str(row.get("role") or "").upper(),
            cpu_threads=int(row.get("cpu_threads") or 0),
            memory_claim_bytes=int(row.get("memory_claim_bytes") or 0),
            minimum_free_memory_bytes=int(
                row.get("minimum_free_memory_bytes")
                or manifest.get("minimum_free_memory_bytes")
                or 0
            ),
        )
        if (
            not profile.role
            or profile.cpu_threads < 1
            or profile.memory_claim_bytes < 1
            or profile.minimum_free_memory_bytes < 1
        ):
            raise NodeResourceAdmissionError(
                f"invalid node resource profile: {profile.profile_id}"
            )
        return profile


def load_capacity_manifest(path: Path) -> dict[str, Any]:
    manifest = _read_json(Path(path))
    claimed_hash = str(manifest.get("capacity_manifest_sha256") or "")
    body = dict(manifest)
    body.pop("capacity_manifest_sha256", None)
    if claimed_hash != _stable_hash(body):
        raise NodeResourceAdmissionError("node capacity manifest self-hash drift")
    if str(manifest.get("authorized_host") or "").upper() != socket.gethostname().upper():
        raise NodeResourceAdmissionError("node capacity manifest host mismatch")
    if int(manifest.get("logical_cpu_threads") or 0) < 1:
        raise NodeResourceAdmissionError("node logical CPU capacity is invalid")
    return manifest


def acquire_node_resource_lease(
    *,
    state_root: Path,
    capacity_manifest_path: Path,
    profile_id: str,
    lease_id: str,
    owner_pid: int,
    workload_id: str,
) -> dict[str, Any]:
    root = Path(state_root)
    manifest_path = Path(capacity_manifest_path).resolve()
    manifest = load_capacity_manifest(manifest_path)
    profile = NodeResourceProfile.from_manifest(manifest, profile_id)
    pid = int(owner_pid)
    owner_create_time = _process_create_time(pid)
    state_path = root / "active_node_resource_leases.json"
    lock_path = root / "active_node_resource_leases.lock"
    with _exclusive_lock(lock_path):
        if state_path.is_file():
            state = _read_json(state_path)
            _validate_lease_state(
                state,
                capacity_manifest_sha256=str(
                    manifest["capacity_manifest_sha256"]
                ),
            )
        else:
            state = {
                "schema_version": "cn_node_resource_lease_state_v1",
                "authorized_host": socket.gethostname().upper(),
                "capacity_manifest_path": str(manifest_path),
                "capacity_manifest_sha256": str(
                    manifest["capacity_manifest_sha256"]
                ),
                "leases": [],
                "updated_at_utc": _utc_now(),
            }
            state["state_sha256"] = _stable_hash(state)
        active = [
            dict(row)
            for row in list(state.get("leases") or [])
            if _lease_is_active(row)
        ]
        existing = next(
            (row for row in active if str(row.get("lease_id")) == str(lease_id)),
            None,
        )
        if existing is not None:
            if (
                int(existing.get("owner_pid") or 0) != pid
                or abs(
                    float(existing.get("owner_create_time") or 0.0)
                    - owner_create_time
                )
                >= 1.0
                or str(existing.get("profile_id") or "")
                != profile.profile_id
                or str(existing.get("role") or "").upper() != profile.role
                or int(existing.get("cpu_threads") or 0)
                != profile.cpu_threads
                or int(existing.get("memory_claim_bytes") or 0)
                != profile.memory_claim_bytes
                or str(existing.get("workload_id") or "")
                != str(workload_id)
            ):
                raise NodeResourceAdmissionError(
                    f"node resource lease id is already active: {lease_id}"
                )
            lease = existing
        else:
            requested_cpu = sum(int(row.get("cpu_threads") or 0) for row in active)
            requested_memory = sum(
                int(row.get("memory_claim_bytes") or 0) for row in active
            )
            admitted_cpu = requested_cpu + profile.cpu_threads
            admitted_memory = requested_memory + profile.memory_claim_bytes
            logical_capacity = int(manifest["logical_cpu_threads"])
            memory_capacity = int(manifest["memory_capacity_bytes"])
            reserve = max(
                profile.minimum_free_memory_bytes,
                int(manifest.get("minimum_free_memory_bytes") or 0),
            )
            if admitted_cpu > logical_capacity:
                raise NodeResourceAdmissionError(
                    "node CPU entitlement exceeded: "
                    f"requested={admitted_cpu} capacity={logical_capacity}"
                )
            if admitted_memory > memory_capacity - reserve:
                raise NodeResourceAdmissionError(
                    "node memory claim exceeded: "
                    f"requested={admitted_memory} usable={memory_capacity - reserve}"
                )
            available = _available_memory_bytes()
            admission_floor = reserve + profile.memory_claim_bytes
            if available and available < admission_floor:
                raise NodeResourceAdmissionError(
                    "node free-memory admission headroom is insufficient: "
                    f"available={available} required={admission_floor} "
                    f"reserve={reserve} claim={profile.memory_claim_bytes}"
                )
            lease = {
                "lease_id": str(lease_id),
                "profile_id": profile.profile_id,
                "role": profile.role,
                "cpu_threads": profile.cpu_threads,
                "memory_claim_bytes": profile.memory_claim_bytes,
                "minimum_free_memory_bytes": reserve,
                "owner_pid": pid,
                "owner_create_time": owner_create_time,
                "workload_id": str(workload_id),
                "acquired_at_utc": _utc_now(),
            }
            active.append(lease)
        state = {
            "schema_version": "cn_node_resource_lease_state_v1",
            "authorized_host": socket.gethostname().upper(),
            "capacity_manifest_path": str(manifest_path),
            "capacity_manifest_sha256": str(
                manifest["capacity_manifest_sha256"]
            ),
            "leases": sorted(active, key=lambda row: str(row["lease_id"])),
            "updated_at_utc": _utc_now(),
        }
        state["state_sha256"] = _stable_hash(
            {key: value for key, value in state.items() if key != "state_sha256"}
        )
        _write_json_atomic(state_path, state)
    receipt = {
        "schema_version": "cn_node_resource_lease_receipt_v1",
        "status": "NODE_RESOURCE_LEASE_ACTIVE",
        "authorized_host": socket.gethostname().upper(),
        "capacity_manifest_path": str(manifest_path),
        "capacity_manifest_sha256": str(manifest["capacity_manifest_sha256"]),
        "state_path": str(state_path.resolve()),
        "lease": lease,
        "active_cpu_threads_after_admission": sum(
            int(row["cpu_threads"]) for row in state["leases"]
        ),
        "active_memory_claim_bytes_after_admission": sum(
            int(row["memory_claim_bytes"]) for row in state["leases"]
        ),
        "state_sha256": state["state_sha256"],
    }
    receipt["receipt_sha256"] = _stable_hash(receipt)
    return receipt


def release_node_resource_lease(
    *,
    state_root: Path,
    lease_id: str,
    owner_pid: int,
) -> dict[str, Any]:
    root = Path(state_root)
    state_path = root / "active_node_resource_leases.json"
    lock_path = root / "active_node_resource_leases.lock"
    with _exclusive_lock(lock_path):
        if not state_path.is_file():
            return {
                "schema_version": "cn_node_resource_lease_release_v1",
                "status": "LEASE_ALREADY_ABSENT",
                "lease_id": str(lease_id),
                "released_at_utc": _utc_now(),
                "state_sha256": None,
            }
        state = _read_json(state_path)
        _validate_lease_state(
            state,
            capacity_manifest_sha256=str(
                state.get("capacity_manifest_sha256") or ""
            ),
        )
        retained = []
        released = None
        for row in list(state.get("leases") or []):
            if str(row.get("lease_id")) == str(lease_id):
                if int(row.get("owner_pid") or 0) != int(owner_pid):
                    raise NodeResourceLeaseDriftError(
                        f"node resource lease owner mismatch: {lease_id}"
                    )
                released = dict(row)
            elif _lease_is_active(row):
                retained.append(dict(row))
        state = {
            **{key: value for key, value in state.items() if key not in {"leases", "state_sha256"}},
            "leases": sorted(retained, key=lambda row: str(row["lease_id"])),
            "updated_at_utc": _utc_now(),
        }
        state["state_sha256"] = _stable_hash(state)
        _write_json_atomic(state_path, state)
    return {
        "schema_version": "cn_node_resource_lease_release_v1",
        "status": "NODE_RESOURCE_LEASE_RELEASED" if released else "LEASE_ALREADY_ABSENT",
        "lease_id": str(lease_id),
        "released_at_utc": _utc_now(),
        "state_sha256": state["state_sha256"],
    }


def inspect_node_resource_state(
    *,
    state_root: Path,
    capacity_manifest_path: Path,
) -> dict[str, Any]:
    root = Path(state_root)
    manifest_path = Path(capacity_manifest_path).resolve()
    manifest = load_capacity_manifest(manifest_path)
    state_path = root / "active_node_resource_leases.json"
    lock_path = root / "active_node_resource_leases.lock"
    with _exclusive_lock(lock_path):
        if state_path.is_file():
            state = _read_json(state_path)
            _validate_lease_state(
                state,
                capacity_manifest_sha256=str(
                    manifest["capacity_manifest_sha256"]
                ),
            )
            leases = [dict(row) for row in list(state.get("leases") or [])]
        else:
            leases = []
    rows = [
        {**row, "process_state": "ACTIVE" if _lease_is_active(row) else "STALE"}
        for row in leases
    ]
    active = [row for row in rows if row["process_state"] == "ACTIVE"]
    reserve = int(manifest.get("minimum_free_memory_bytes") or 0)
    payload = {
        "schema_version": "cn_node_resource_status_v1",
        "status": "NODE_RESOURCE_STATUS_OBSERVED",
        "authorized_host": socket.gethostname().upper(),
        "capacity_manifest_path": str(manifest_path),
        "capacity_manifest_sha256": str(
            manifest["capacity_manifest_sha256"]
        ),
        "logical_cpu_threads": int(manifest["logical_cpu_threads"]),
        "active_cpu_threads": sum(int(row["cpu_threads"]) for row in active),
        "available_cpu_threads": int(manifest["logical_cpu_threads"])
        - sum(int(row["cpu_threads"]) for row in active),
        "memory_capacity_bytes": int(manifest["memory_capacity_bytes"]),
        "minimum_free_memory_bytes": reserve,
        "active_memory_claim_bytes": sum(
            int(row["memory_claim_bytes"]) for row in active
        ),
        "unclaimed_memory_capacity_bytes": int(manifest["memory_capacity_bytes"])
        - reserve
        - sum(int(row["memory_claim_bytes"]) for row in active),
        "observed_available_memory_bytes": _available_memory_bytes(),
        "active_lease_count": len(active),
        "stale_lease_count": len(rows) - len(active),
        "leases": rows,
        "observed_at_utc": _utc_now(),
    }
    payload["status_sha256"] = _stable_hash(payload)
    return payload


def validate_node_resource_lease_receipt(
    receipt_path: Path,
    *,
    expected_role: str,
    expected_cpu_threads: int,
) -> dict[str, Any]:
    receipt = _read_json(Path(receipt_path))
    claimed = str(receipt.get("receipt_sha256") or "")
    body = dict(receipt)
    body.pop("receipt_sha256", None)
    if claimed != _stable_hash(body):
        raise NodeResourceLeaseDriftError("node resource receipt self-hash drift")
    lease = dict(receipt.get("lease") or {})
    drift = []
    if str(receipt.get("status") or "") != "NODE_RESOURCE_LEASE_ACTIVE":
        drift.append("status")
    if str(lease.get("role") or "").upper() != str(expected_role).upper():
        drift.append("role")
    if int(lease.get("cpu_threads") or 0) != int(expected_cpu_threads):
        drift.append("cpu_threads")
    if not _lease_is_active(lease):
        drift.append("owner_or_workload_process")
    state_value = str(receipt.get("state_path") or "")
    if not state_value:
        drift.append("state_path")
    else:
        state_path = Path(state_value)
        if not state_path.is_file():
            drift.append("active_state_missing")
        else:
            try:
                state = _read_json(state_path)
                _validate_lease_state(
                    state,
                    capacity_manifest_sha256=str(
                        receipt.get("capacity_manifest_sha256") or ""
                    ),
                )
                active = next(
                    (
                        dict(row)
                        for row in list(state.get("leases") or [])
                        if str(row.get("lease_id") or "")
                        == str(lease.get("lease_id") or "")
                    ),
                    None,
                )
                if active != lease:
                    drift.append("active_state_membership")
            except (OSError, ValueError, NodeResourceLeaseDriftError):
                drift.append("active_state_integrity")
    if drift:
        raise NodeResourceLeaseDriftError(
            "node resource lease drift: " + ",".join(sorted(drift))
        )
    return receipt
