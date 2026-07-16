from __future__ import annotations

import pytest

from our_system_phase2.services.phase3cm_streaming_telemetry import (
    PhaseTelemetryRecorder,
    ResourceSnapshot,
    ThreadBudgetError,
    aggregate_compute_phase_parallelism,
    build_phase_event,
    freeze_thread_budget,
    _process_snapshot,
)


def test_thread_budget_rejects_global_native_oversubscription() -> None:
    with pytest.raises(ThreadBudgetError, match="global native thread budget exceeded"):
        freeze_thread_budget(
            heavy_processes=2,
            compute_threads_per_process=13,
            primary_pool="numba",
        )


def test_compute_phase_records_effective_cores_and_parallelism_failure() -> None:
    event = build_phase_event(
        phase="cross_sectional_ranking",
        wall_seconds=10.0,
        cpu_seconds=11.0,
        allocated_compute_threads=8,
        compute_heavy=True,
        rss_before_bytes=100,
        rss_after_bytes=200,
        peak_rss_bytes=250,
        bytes_read=1024,
        rows_read=500,
        row_groups_read=2,
        blocks_processed=1,
    )

    assert event["effective_cores"] == pytest.approx(1.1)
    assert event["parallel_efficiency"] == pytest.approx(0.1375)
    assert event["parallelism_status"] == "PARALLELISM_NOT_ENGAGED"


def test_compute_gate_qualifies_complete_phase_not_single_short_block() -> None:
    events = [
        build_phase_event(
            phase="turnover_and_cost",
            wall_seconds=0.01,
            cpu_seconds=0.01,
            allocated_compute_threads=4,
            compute_heavy=True,
            rss_before_bytes=100,
            rss_after_bytes=100,
            peak_rss_bytes=100,
        ),
        build_phase_event(
            phase="turnover_and_cost",
            wall_seconds=0.99,
            cpu_seconds=3.99,
            allocated_compute_threads=4,
            compute_heavy=True,
            rss_before_bytes=100,
            rss_after_bytes=100,
            peak_rss_bytes=100,
        ),
    ]

    aggregate = aggregate_compute_phase_parallelism(events)["turnover_and_cost"]

    assert events[0]["parallelism_status"] == "PARALLELISM_NOT_ENGAGED"
    assert aggregate["effective_cores"] == pytest.approx(4.0)
    assert aggregate["parallelism_status"] == "PARALLELISM_ENGAGED"
    assert aggregate["subphase_event_failure_count"] == 1


def test_io_phase_requires_device_or_arrow_decode_evidence() -> None:
    no_evidence = build_phase_event(
        phase="panel_read",
        wall_seconds=10.0,
        cpu_seconds=1.0,
        allocated_compute_threads=8,
        compute_heavy=False,
        rss_before_bytes=100,
        rss_after_bytes=200,
        peak_rss_bytes=250,
        bytes_read=1000,
        device_sequential_bytes_per_second=1000.0,
    )
    device_bound = build_phase_event(
        phase="panel_read",
        wall_seconds=1.0,
        cpu_seconds=0.2,
        allocated_compute_threads=8,
        compute_heavy=False,
        rss_before_bytes=100,
        rss_after_bytes=200,
        peak_rss_bytes=250,
        bytes_read=800,
        device_sequential_bytes_per_second=1000.0,
    )
    decode_bound = build_phase_event(
        phase="arrow_decode",
        wall_seconds=2.0,
        cpu_seconds=8.0,
        allocated_compute_threads=8,
        compute_heavy=False,
        rss_before_bytes=100,
        rss_after_bytes=200,
        peak_rss_bytes=250,
        bytes_read=800,
        arrow_decode_cpu_seconds=6.0,
    )

    assert no_evidence["io_qualification_status"] == "EXECUTION_ARCHITECTURE_BOTTLENECK"
    assert device_bound["io_qualification_status"] == "IO_DEVICE_THROUGHPUT_ENGAGED"
    assert decode_bound["io_qualification_status"] == "ARROW_DECODE_CPU_ENGAGED"


def test_phase_recorder_writes_one_aggregated_event_without_per_row_logging(tmp_path) -> None:
    snapshots = iter(
        [
            ResourceSnapshot(wall_seconds=1.0, cpu_seconds=2.0, rss_bytes=100, peak_rss_bytes=120, bytes_read=10),
            ResourceSnapshot(wall_seconds=3.0, cpu_seconds=10.0, rss_bytes=180, peak_rss_bytes=220, bytes_read=1010),
        ]
    )
    output = tmp_path / "phase_timing.jsonl"
    recorder = PhaseTelemetryRecorder(
        output_path=output,
        allocated_compute_threads=8,
        snapshot_provider=lambda: next(snapshots),
    )

    with recorder.phase("expression_evaluation", compute_heavy=True) as span:
        span.add(rows_read=500, blocks_processed=2, dag_nodes_evaluated=7)

    events = recorder.events
    assert len(events) == 1
    assert events[0]["wall_seconds"] == pytest.approx(2.0)
    assert events[0]["cpu_seconds"] == pytest.approx(8.0)
    assert events[0]["bytes_read"] == 1000
    assert events[0]["rows_read"] == 500
    assert events[0]["dag_nodes_evaluated"] == 7
    assert output.read_text(encoding="utf-8").count("\n") == 1


def test_process_snapshot_exposes_real_windows_rss_without_psutil() -> None:
    snapshot = _process_snapshot()
    assert snapshot.wall_seconds > 0
    assert snapshot.cpu_seconds >= 0
    assert snapshot.rss_bytes > 0
    assert snapshot.peak_rss_bytes >= snapshot.rss_bytes
