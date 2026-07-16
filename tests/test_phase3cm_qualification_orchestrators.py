from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_phase_d_and_e_monitor_process_trees_and_use_atomic_exit_receipts() -> None:
    for name in (
        "run_cn_phase3cm_phase_d_32pair_scaling_77o.ps1",
        "run_cn_phase3cm_phase_e_qualification_77o.ps1",
    ):
        text = (ROOT / "scripts" / name).read_text(encoding="utf-8")
        assert "Get-CnProcessTreeRssSnapshot" in text
        assert 'rss_monitor = "PROCESS_TREE_RSS"' in text
        assert "CN_BACKEND_EXIT_RECEIPT.json" in text
        assert "$Active.ExitCode" not in text
        assert "$Session.ExitCode" not in text
        assert "$Process.WorkingSet64" not in text


def test_backend_wrapper_writes_exit_receipt_in_finally() -> None:
    text = (ROOT / "scripts" / "invoke_cn_phase3cm_backend_with_exit_receipt.ps1").read_text(
        encoding="utf-8"
    )
    assert "finally" in text
    assert "cn_phase3cm_backend_exit_receipt_v1" in text
    assert "Move-Item -LiteralPath $Temporary -Destination $ExitReceipt -Force" in text
