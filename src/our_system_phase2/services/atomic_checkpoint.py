"""Atomic and idempotent checkpoint primitives for resumable batch work."""

from __future__ import annotations

import errno
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, IO, Iterable


def _canonical_bytes(payload: Any) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError as exc:
                if exc.errno != errno.EINVAL:
                    raise
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_bytes(path, _canonical_bytes(payload))


def durable_flush(handle: IO[Any]) -> bool:
    """Flush a stream; Windows EINVAL from fsync is non-fatal after flush."""

    handle.flush()
    try:
        os.fsync(handle.fileno())
    except OSError as exc:
        if exc.errno == errno.EINVAL:
            return False
        raise
    return True


class AtomicRecordStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _name(key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest() + ".json"

    def path_for(self, key: str) -> Path:
        return self.root / self._name(key)

    def put(self, key: str, payload: Any) -> bool:
        path = self.path_for(key)
        content = _canonical_bytes({"key": key, "payload": payload})
        if path.exists():
            if path.read_bytes() != content:
                raise ValueError(f"checkpoint conflict for key: {key}")
            return False
        atomic_write_bytes(path, content)
        return True

    def contains(self, key: str) -> bool:
        return self.path_for(key).is_file()

    def get(self, key: str) -> Any:
        return json.loads(self.path_for(key).read_text(encoding="utf-8"))["payload"]

    def records(self) -> Iterable[tuple[str, Any]]:
        for path in sorted(self.root.glob("*.json")):
            row = json.loads(path.read_text(encoding="utf-8"))
            yield str(row["key"]), row["payload"]
