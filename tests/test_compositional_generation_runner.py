from __future__ import annotations

import hashlib
import importlib.util
import json
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def test_external_output_manifest_repair_is_hash_bound(tmp_path: Path) -> None:
    runner = _module()
    output = tmp_path / "external-output"
    output.mkdir()
    plan = tmp_path / "plan.json"
    registry = tmp_path / "registry.json"
    plan.write_text("{}\n", encoding="utf-8")
    registry.write_text("{}\n", encoding="utf-8")
    repo_sha = "a" * 40
    tree_sha = "b" * 40
    summary = {
        "repo_sha": repo_sha,
        "tree_sha": tree_sha,
        "plan_sha256": _sha256(plan),
        "registry_sha256": _sha256(registry),
    }
    for name in (
        "CN_PROPOSAL_EXPOSURE_LEDGER.parquet",
        "CN_PAIR_RECEIPTS.jsonl",
        "CN_STRUCTURAL_PREADMISSION.json",
        "CN_ADMISSION_WATERFALL.csv",
        "CN_ROUTE_SKELETON_METRICS.csv",
    ):
        (output / name).write_bytes(name.encode("utf-8"))
    (output / "CN_GENERATION_EPOCH_SUMMARY.json").write_text(
        json.dumps(summary),
        encoding="utf-8",
    )

    assert runner._finalize_existing_generation(
        output=output,
        plan_path=plan,
        registry_path=registry,
        repo_sha=repo_sha,
        tree_sha=tree_sha,
    ) == 0
    manifest = json.loads(
        (output / "CN_GENERATION_ARTIFACT_MANIFEST.json").read_text(encoding="utf-8")
    )
    assert manifest["status"] == "COMPLETED_WITH_MANIFEST_PATH_REPAIR"
    assert len(manifest["artifacts"]) == 6
    assert all(Path(row["path"]).is_absolute() for row in manifest["artifacts"])
