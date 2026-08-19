from __future__ import annotations

from pathlib import Path

import pytest

import app
from our_system_phase2.runtime import (
    cn_program_disclosure_timing_mechanism_successor_v1 as runtime,
)
from our_system_phase2.services.project_control_admission import (
    ACTION_LAUNCH,
    ACTION_RETRY,
    CAMPAIGN_AUTHORIZATION_BOUND_ROUTES,
    ProjectControlDenied,
)
from scripts import run_cn_program_disclosure_timing_mechanism_successor_v1 as runner

REPO = Path(__file__).resolve().parents[1]
AUTH = REPO / runtime.AUTHORIZATION_RELATIVE_PATH
PREFREEZE = REPO / "runtime/run_plans/cn_disclosure_timing_mechanism_successor_prefreeze_20260819.json"
SPENT = REPO / "runtime/run_plans/cn_disclosure_timing_mechanism_spent_exact_freeze_20260819.json"


def _row(*, event: str, temporal: str, rep: str, pulse: str, productive: bool) -> dict:
    return {
        "event_component_id": event,
        "temporal_component_id": temporal,
        "event_representation_family": rep,
        "event_pulse_family": pulse,
        "admission": {"admitted": productive},
        "uplift": {
            "program_credit": {
                "matched_cumulative_net_return_increment": 1.0 if productive else -1.0,
                "matched_net_reward_increment": 1.0 if productive else -1.0,
            }
        },
    }


def test_route_is_high_cost_and_authorization_bound() -> None:
    assert app.ROUTES[runtime.ROUTE_ID] == (
        "our_system_phase2.runtime.cn_program_disclosure_timing_mechanism_successor_v1"
    )
    assert app.HIGH_COST_ROUTE_ACTIONS[runtime.ROUTE_ID] == {
        ACTION_LAUNCH,
        ACTION_RETRY,
    }
    assert runtime.ROUTE_ID in CAMPAIGN_AUTHORIZATION_BOUND_ROUTES


def test_frozen_authorization_prefreeze_and_spent_set_verify() -> None:
    authorization = runtime.verify_authorization(AUTH, repo_root=REPO)
    prefreeze = runner.verify_prefreeze(PREFREEZE)
    spent = runner.verify_spent_freeze(SPENT)
    assert authorization["mechanism_prefreeze"]["stage_a_records"] == 288
    assert authorization["mechanism_prefreeze"]["stage_b_records"] == 264
    assert prefreeze["resource_preview"]["union_field_count"] == 42
    assert len(prefreeze["candidates"]["stage_a"]) == 288
    assert len(prefreeze["candidates"]["stage_b"]) == 264
    assert len(spent["combined_spent_exact_identities"]) == 2150
    assert set(row["exact_identity"] for row in prefreeze["candidates"]["stage_a"] + prefreeze["candidates"]["stage_b"]).isdisjoint(
        spent["combined_spent_exact_identities"]
    )


def test_stage_a_gate_fails_single_primitive_lottery() -> None:
    prefreeze = runner.verify_prefreeze(PREFREEZE)
    rows = []
    for index in range(24):
        rows.append(
            _row(
                event=runner.ORIGINAL_EVENT_COMPONENT_ID,
                temporal=f"t{index % 3}",
                rep="event_first_hit",
                pulse="balance",
                productive=True,
            )
        )
    for event_index in range(11):
        for index in range(24):
            rows.append(
                _row(
                    event=f"sibling_{event_index}",
                    temporal=f"t{index % 3}",
                    rep=f"rep_{event_index % 4}",
                    pulse=f"pulse_{event_index % 3}",
                    productive=False,
                )
            )
    gate = runner._stage_a_gate(rows, prefreeze["gates"])
    assert gate["status"] == "FAIL"
    assert gate["checks"]["anchor_productive_rate"] is True
    assert gate["checks"]["sibling_aggregate_productive_rate"] is False


def test_stage_a_and_stage_b_systematic_gates_pass_broad_success() -> None:
    prefreeze = runner.verify_prefreeze(PREFREEZE)
    stage_a = []
    events = [runner.ORIGINAL_EVENT_COMPONENT_ID] + [f"e{i}" for i in range(11)]
    for event_index, event in enumerate(events):
        for index in range(24):
            stage_a.append(
                _row(
                    event=event,
                    temporal=f"seen_t{index % 3}",
                    rep=f"rep_{event_index % 4}",
                    pulse=f"pulse_{event_index % 3}",
                    productive=index < 18,
                )
            )
    gate_a = runner._stage_a_gate(stage_a, prefreeze["gates"])
    assert gate_a["status"] == "PASS"

    stage_b = []
    for event_index in range(11):
        for temporal_index in range(6):
            for policy_index in range(4):
                stage_b.append(
                    _row(
                        event=f"e{event_index}",
                        temporal=f"unseen_t{temporal_index}",
                        rep=f"rep_{event_index % 4}",
                        pulse=f"pulse_{event_index % 3}",
                        productive=policy_index < 3,
                    )
                )
    gate_b = runner._stage_b_gate(stage_b, prefreeze["gates"])
    assert gate_b["status"] == "PASS"


def test_runtime_injects_frozen_executor_workers(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    authorization = runtime.verify_authorization(AUTH, repo_root=REPO)
    captured: dict[str, int] = {}
    admission = {
        "repo_sha": "0" * 40,
        "campaign_authorization_path": str(AUTH.resolve()),
        "campaign_authorization_file_sha256": runtime.sha256_file(AUTH),
        "target_campaign_instance_id": runtime.CAMPAIGN_ID,
        "target_campaign_profile": runtime.CAMPAIGN_PROFILE,
    }
    monkeypatch.setattr(runtime, "consume_active_admission", lambda *_args, **_kwargs: admission)
    monkeypatch.setattr(runtime, "verify_consumed_admission_target", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runtime,
        "verify_campaign_authorization_binding",
        lambda *_args, **_kwargs: type(
            "Verified", (), {"path": AUTH.resolve(), "payload": authorization}
        )(),
    )
    monkeypatch.setattr(runtime, "validate_node_resource_lease_receipt", lambda *_args, **_kwargs: {})

    import scripts.run_cn_program_disclosure_timing_mechanism_successor_v1 as runner

    def fake_run(args, *, admission, authorization):
        captured["executor_workers"] = args.executor_workers
        return {"status": "NOOP"}

    monkeypatch.setattr(runner, "run", fake_run)
    argv = [
        "--campaign-authorization", str(AUTH),
        "--mechanism-prefreeze", str(PREFREEZE),
        "--spent-exact-freeze", str(SPENT),
        "--source-freeze-root", str(tmp_path / "source"),
        "--prior-exact-freeze", str(tmp_path / "prior.json"),
        "--execution-contract", str(tmp_path / "execution.json"),
        "--train-field-root", str(tmp_path / "fields"),
        "--train-price-root", str(tmp_path / "prices"),
        "--registry", str(tmp_path / "registry.json"),
        "--node-resource-capacity", str(tmp_path / "capacity.json"),
        "--node-resource-lease-receipt", str(tmp_path / "lease.json"),
        "--output-root", str(tmp_path / "output"),
    ]
    assert runtime.main(argv) == 0
    assert captured["executor_workers"] == 24


def test_direct_runtime_invocation_is_denied_before_argument_parsing() -> None:
    with pytest.raises(ProjectControlDenied, match="DIRECT_HIGH_COST_MODULE_EXECUTION_FORBIDDEN"):
        runtime.main([])
