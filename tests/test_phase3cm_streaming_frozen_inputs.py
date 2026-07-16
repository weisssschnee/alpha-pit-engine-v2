from __future__ import annotations

from pathlib import Path
import shutil

import pytest

from our_system_phase2.services.phase3cm_streaming_frozen_inputs import (
    FrozenInputDriftError,
    build_frozen_input_binding,
)


REPO = Path(__file__).resolve().parents[1]
RUNTIME = REPO / "runtime/cn_compositional_nline_large_search_20260715"
BASE_CLOSURE = "b71fa6b239ceb602576f1769ef1f512603ce326b"


def test_real_frozen_preflight_pack_binds_exact_members_and_receipts() -> None:
    binding = build_frozen_input_binding(
        runtime_root=RUNTIME,
        freeze_manifest=RUNTIME / "CN_RESOURCE_PREFLIGHT_FREEZE.json",
        pack_path=RUNTIME / "CN_RESOURCE_PREFLIGHT_PACK.csv",
        source_closure_sha=BASE_CLOSURE,
    )

    assert binding["status"] == "CN_STREAMING_REPAIR_FROZEN_INPUT_BOUND"
    assert binding["source_closure_sha"] == BASE_CLOSURE
    assert binding["pack_identity"] == "dfb23c7887e3fa2233a3bb096f62f6c0fce583aed2a780bfb2357f5b19a918f3"
    assert binding["pair_count"] == 32
    assert binding["candidate_member_count"] == 64
    assert binding["clock_counts"] == {"active_bar": 18, "stock_session": 14}
    assert len(binding["pairs"]) == 32
    assert len({row["pair_id"] for row in binding["pairs"]}) == 32
    assert len({row["candidate_id"] for row in binding["candidate_members"]}) == 64
    assert all(row["receipt_hash"] for row in binding["candidate_members"])
    assert all(row["pair_receipt_hash"] for row in binding["pairs"])
    assert binding["sealed_reads"] == {
        "validation": 0,
        "holdout": 0,
        "forward_2026": 0,
    }


def test_frozen_input_binding_fails_closed_on_content_drift(tmp_path: Path) -> None:
    names = [
        "CN_RESOURCE_PREFLIGHT_FREEZE.json",
        "CN_RESOURCE_PREFLIGHT_PACK.csv",
        "preflight_active_candidates.csv",
        "preflight_active_candidate_receipts.jsonl",
        "preflight_active_pair_receipts.jsonl",
        "preflight_session_candidates.csv",
        "preflight_session_candidate_receipts.jsonl",
        "preflight_session_pair_receipts.jsonl",
    ]
    for name in names:
        shutil.copyfile(RUNTIME / name, tmp_path / name)
    with (tmp_path / "preflight_active_candidates.csv").open("ab") as handle:
        handle.write(b"\n")

    with pytest.raises(
        FrozenInputDriftError,
        match="CN_PHASE3CM_STREAMING_REPAIR_INVALID_INPUT_DRIFT: content hash drift",
    ):
        build_frozen_input_binding(
            runtime_root=tmp_path,
            freeze_manifest=tmp_path / "CN_RESOURCE_PREFLIGHT_FREEZE.json",
            pack_path=tmp_path / "CN_RESOURCE_PREFLIGHT_PACK.csv",
            source_closure_sha=BASE_CLOSURE,
        )
