from __future__ import annotations

import json
from pathlib import Path

from scripts.build_nextgen_external_sidecars import _persist_attempt


def test_run_attempts_do_not_overwrite_prior_history(tmp_path: Path) -> None:
    attempts = tmp_path / "attempts"
    first = attempts / "attempt-1.json"
    second = attempts / "attempt-2.json"
    latest = tmp_path / "latest.json"
    first_record = {"status": "FAILED", "failure": {"message": "first failure"}}
    second_record = {"status": "COMPLETED", "failure": None}

    _persist_attempt(first, latest, first_record)
    original_first = first.read_bytes()
    _persist_attempt(second, latest, second_record)

    assert first.read_bytes() == original_first
    assert json.loads(second.read_text(encoding="utf-8"))["status"] == "COMPLETED"
    pointer = json.loads(latest.read_text(encoding="utf-8"))
    assert pointer["latest_attempt"] == str(second)
    assert pointer["status"] == "COMPLETED"
