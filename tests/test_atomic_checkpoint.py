from __future__ import annotations

import errno
from pathlib import Path

import pytest

from our_system_phase2.services.atomic_checkpoint import AtomicRecordStore, atomic_write_json, durable_flush


def test_atomic_record_store_is_idempotent_and_conflict_safe(tmp_path: Path) -> None:
    store = AtomicRecordStore(tmp_path / "records")

    assert store.put("candidate-1", {"rows": [1, 2]}) is True
    assert store.put("candidate-1", {"rows": [1, 2]}) is False
    assert store.get("candidate-1") == {"rows": [1, 2]}
    with pytest.raises(ValueError, match="checkpoint conflict"):
        store.put("candidate-1", {"rows": [3]})


def test_atomic_json_replaces_complete_payload(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    atomic_write_json(path, {"version": 1})
    atomic_write_json(path, {"version": 2, "complete": True})

    assert path.read_text(encoding="utf-8") == '{"complete":true,"version":2}\n'
    assert not list(tmp_path.glob("*.tmp"))


def test_windows_einval_after_flush_is_not_candidate_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "rows.csv"
    with path.open("w", encoding="utf-8") as handle:
        handle.write("a,b\n1,2\n")
        monkeypatch.setattr("os.fsync", lambda _: (_ for _ in ()).throw(OSError(errno.EINVAL, "invalid")))
        assert durable_flush(handle) is False

    assert path.read_text(encoding="utf-8").endswith("1,2\n")
