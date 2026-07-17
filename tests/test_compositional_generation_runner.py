from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

import pytest


REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / "scripts/run_cn_compositional_generation_epoch.py"


def _module() -> object:
    spec = importlib.util.spec_from_file_location("cn_generation_runner_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _source_sha256(path: Path) -> str:
    data = path.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return hashlib.sha256(data).hexdigest()


def test_packaged_source_identity_requires_explicit_sha_and_code_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = _module()
    monkeypatch.setattr(runner.shutil, "which", lambda _: None)
    grammar = REPO / "src/our_system_phase2/services/compositional_grammar.py"
    plan = {
        "repo_sha": "a" * 40,
        "tree_sha": "b" * 40,
        "code_hashes": {"grammar": _source_sha256(grammar)},
    }

    assert runner._source_identity(
        plan,
        repo_sha="a" * 40,
        tree_sha="b" * 40,
    ) == ("a" * 40, "b" * 40)

    with pytest.raises(RuntimeError, match="requires explicit"):
        runner._source_identity(plan, repo_sha="", tree_sha="")

    bad = {**plan, "code_hashes": {"grammar": "0" * 64}}
    with pytest.raises(RuntimeError, match="code hash drift"):
        runner._source_identity(
            bad,
            repo_sha="a" * 40,
            tree_sha="b" * 40,
        )
