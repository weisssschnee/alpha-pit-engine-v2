from pathlib import Path


def test_phase_e_launcher_is_pair_count_generic_and_time_gate_is_explicit() -> None:
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "run_cn_phase3cm_phase_e_qualification_77o.ps1"
    ).read_text(encoding="utf-8-sig")

    assert '$TotalPairCount = $ActivePairCount + $SessionPairCount' in script
    assert 'CN_PHASE3CM_PHASE_E_STRICT_WAVE_PASS' in script
    assert 'total_pair_count = $TotalPairCount' in script
    assert '$WallSecondsHardMax = 0.0' in script
    assert '$WallGatePass = ($WallSecondsHardMax -le 0.0' in script
    assert '$Stopwatch.Elapsed.TotalSeconds -le 7200.0' not in script
    assert 'PHASE_E_32PAIR_QUALIFICATION' not in script
