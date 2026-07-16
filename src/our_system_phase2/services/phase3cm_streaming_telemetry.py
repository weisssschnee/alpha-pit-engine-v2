"""Resource telemetry and thread-budget contracts for streaming Phase3CM."""

from __future__ import annotations

import hashlib
import json
import os
import time
import ctypes
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import Any


THREAD_ENV_BY_POOL = {
    "numba": "NUMBA_NUM_THREADS",
    "arrow": "ARROW_NUM_THREADS",
    "omp": "OMP_NUM_THREADS",
    "mkl": "MKL_NUM_THREADS",
    "openblas": "OPENBLAS_NUM_THREADS",
    "numexpr": "NUMEXPR_MAX_THREADS",
}
GLOBAL_NATIVE_THREAD_MAX = 24


class ThreadBudgetError(ValueError):
    """Raised when a runtime would oversubscribe the frozen 77o budget."""


@dataclass(frozen=True)
class ResourceSnapshot:
    wall_seconds: float
    cpu_seconds: float
    rss_bytes: int
    peak_rss_bytes: int
    bytes_read: int


def _process_snapshot() -> ResourceSnapshot:
    rss = 0
    peak = 0
    read_bytes = 0
    try:
        import psutil  # type: ignore[import-not-found]

        process = psutil.Process()
        memory = process.memory_info()
        rss = int(memory.rss)
        peak = int(getattr(memory, "peak_wset", rss))
        read_bytes = int(getattr(process.io_counters(), "read_bytes", 0))
    except Exception:
        if os.name == "nt":
            try:
                kernel32 = ctypes.windll.kernel32
                psapi = ctypes.windll.psapi

                class FILETIME(ctypes.Structure):
                    _fields_ = [("low", ctypes.c_ulong), ("high", ctypes.c_ulong)]

                class IO_COUNTERS(ctypes.Structure):
                    _fields_ = [
                        ("read_operation_count", ctypes.c_ulonglong),
                        ("write_operation_count", ctypes.c_ulonglong),
                        ("other_operation_count", ctypes.c_ulonglong),
                        ("read_transfer_count", ctypes.c_ulonglong),
                        ("write_transfer_count", ctypes.c_ulonglong),
                        ("other_transfer_count", ctypes.c_ulonglong),
                    ]

                class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
                    _fields_ = [
                        ("cb", ctypes.c_ulong),
                        ("page_fault_count", ctypes.c_ulong),
                        ("peak_working_set_size", ctypes.c_size_t),
                        ("working_set_size", ctypes.c_size_t),
                        ("quota_peak_paged_pool_usage", ctypes.c_size_t),
                        ("quota_paged_pool_usage", ctypes.c_size_t),
                        ("quota_peak_non_paged_pool_usage", ctypes.c_size_t),
                        ("quota_non_paged_pool_usage", ctypes.c_size_t),
                        ("pagefile_usage", ctypes.c_size_t),
                        ("peak_pagefile_usage", ctypes.c_size_t),
                        ("private_usage", ctypes.c_size_t),
                    ]

                handle = kernel32.GetCurrentProcess()
                memory = PROCESS_MEMORY_COUNTERS_EX()
                memory.cb = ctypes.sizeof(memory)
                if psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb):
                    rss = int(memory.working_set_size)
                    peak = int(memory.peak_working_set_size)
                io = IO_COUNTERS()
                if kernel32.GetProcessIoCounters(handle, ctypes.byref(io)):
                    read_bytes = int(io.read_transfer_count)
                creation, exit_time, kernel, user = FILETIME(), FILETIME(), FILETIME(), FILETIME()
                if kernel32.GetProcessTimes(
                    handle,
                    ctypes.byref(creation),
                    ctypes.byref(exit_time),
                    ctypes.byref(kernel),
                    ctypes.byref(user),
                ):
                    as_ticks = lambda value: (int(value.high) << 32) | int(value.low)
                    process_cpu = (as_ticks(kernel) + as_ticks(user)) / 10_000_000.0
                else:
                    process_cpu = time.process_time()
            except Exception:
                process_cpu = time.process_time()
        else:
            process_cpu = time.process_time()
    else:
        process_cpu = sum(process.cpu_times()[:2])
    return ResourceSnapshot(
        wall_seconds=time.perf_counter(),
        cpu_seconds=process_cpu,
        rss_bytes=rss,
        peak_rss_bytes=peak,
        bytes_read=read_bytes,
    )


class _PhaseSpan:
    def __init__(self, recorder: "PhaseTelemetryRecorder", phase: str, compute_heavy: bool) -> None:
        self.recorder = recorder
        self.phase_name = str(phase)
        self.compute_heavy = bool(compute_heavy)
        self.counters: dict[str, Any] = {}
        self._before: ResourceSnapshot | None = None

    def add(self, **counters: Any) -> None:
        for key, value in counters.items():
            if isinstance(value, (int, float)) and isinstance(self.counters.get(key), (int, float)):
                self.counters[key] += value
            else:
                self.counters[key] = value

    def __enter__(self) -> "_PhaseSpan":
        self._before = self.recorder.snapshot_provider()
        return self

    def __exit__(self, exc_type, exc, traceback) -> bool:
        if self._before is None:
            raise RuntimeError("phase span exited before it was entered")
        after = self.recorder.snapshot_provider()
        standard = {
            "rows_read": int(self.counters.pop("rows_read", 0)),
            "row_groups_read": int(self.counters.pop("row_groups_read", 0)),
            "blocks_processed": int(self.counters.pop("blocks_processed", 0)),
        }
        event = build_phase_event(
            phase=self.phase_name,
            wall_seconds=max(0.0, after.wall_seconds - self._before.wall_seconds),
            cpu_seconds=max(0.0, after.cpu_seconds - self._before.cpu_seconds),
            allocated_compute_threads=self.recorder.allocated_compute_threads,
            compute_heavy=self.compute_heavy,
            rss_before_bytes=self._before.rss_bytes,
            rss_after_bytes=after.rss_bytes,
            peak_rss_bytes=max(self._before.peak_rss_bytes, after.peak_rss_bytes),
            bytes_read=max(0, after.bytes_read - self._before.bytes_read),
            **standard,
            **self.counters,
        )
        event["event_status"] = "ERROR" if exc_type is not None else "COMPLETE"
        if exc_type is not None:
            event["error_type"] = exc_type.__name__
        self.recorder._append(event)
        return False


class PhaseTelemetryRecorder:
    """Write one aggregated JSONL event per completed phase."""

    def __init__(
        self,
        *,
        output_path: Path,
        allocated_compute_threads: int,
        snapshot_provider: Callable[[], ResourceSnapshot] = _process_snapshot,
    ) -> None:
        self.output_path = Path(output_path)
        self.allocated_compute_threads = int(allocated_compute_threads)
        if self.allocated_compute_threads < 1:
            raise ValueError("allocated_compute_threads must be positive")
        self.snapshot_provider = snapshot_provider
        self.events: list[dict[str, Any]] = []

    def phase(self, phase: str, *, compute_heavy: bool) -> _PhaseSpan:
        return _PhaseSpan(self, phase, compute_heavy)

    def _append(self, event: dict[str, Any]) -> None:
        self.events.append(dict(event))
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with self.output_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")


def build_phase_event(
    *,
    phase: str,
    wall_seconds: float,
    cpu_seconds: float,
    allocated_compute_threads: int,
    compute_heavy: bool,
    rss_before_bytes: int,
    rss_after_bytes: int,
    peak_rss_bytes: int,
    bytes_read: int = 0,
    rows_read: int = 0,
    row_groups_read: int = 0,
    blocks_processed: int = 0,
    device_sequential_bytes_per_second: float = 0.0,
    arrow_decode_cpu_seconds: float = 0.0,
    **counters: Any,
) -> dict[str, Any]:
    """Build one deterministic phase summary and apply the CPU qualification rule."""

    wall = float(wall_seconds)
    cpu = float(cpu_seconds)
    allocated = int(allocated_compute_threads)
    if wall < 0 or cpu < 0 or allocated < 1:
        raise ValueError("phase timing and allocated thread counts must be non-negative")
    effective_cores = cpu / wall if wall > 0 else 0.0
    efficiency = effective_cores / allocated
    if compute_heavy:
        parallelism_status = (
            "PARALLELISM_ENGAGED"
            if effective_cores >= 0.5 * allocated
            else "PARALLELISM_NOT_ENGAGED"
        )
    else:
        parallelism_status = "NOT_APPLICABLE_IO_OR_SERIAL_PHASE"
    read_throughput = int(bytes_read) / wall if wall > 0 else 0.0
    device_bandwidth = float(device_sequential_bytes_per_second)
    device_utilization = read_throughput / device_bandwidth if device_bandwidth > 0 else 0.0
    decode_cpu = float(arrow_decode_cpu_seconds)
    if compute_heavy:
        io_status = "NOT_APPLICABLE_COMPUTE_PHASE"
    elif decode_cpu > 0:
        io_status = "ARROW_DECODE_CPU_ENGAGED"
    elif device_bandwidth > 0 and device_utilization >= 0.5:
        io_status = "IO_DEVICE_THROUGHPUT_ENGAGED"
    else:
        io_status = "EXECUTION_ARCHITECTURE_BOTTLENECK"
    event: dict[str, Any] = {
        "phase": str(phase),
        "compute_heavy": bool(compute_heavy),
        "wall_seconds": wall,
        "cpu_seconds": cpu,
        "allocated_compute_threads": allocated,
        "effective_cores": effective_cores,
        "parallel_efficiency": efficiency,
        "parallelism_status": parallelism_status,
        "rss_before_bytes": int(rss_before_bytes),
        "rss_after_bytes": int(rss_after_bytes),
        "peak_rss_bytes": int(peak_rss_bytes),
        "bytes_read": int(bytes_read),
        "read_throughput_bytes_per_second": read_throughput,
        "device_sequential_bytes_per_second": device_bandwidth,
        "device_throughput_utilization": device_utilization,
        "arrow_decode_cpu_seconds": decode_cpu,
        "io_qualification_status": io_status,
        "rows_read": int(rows_read),
        "row_groups_read": int(row_groups_read),
        "blocks_processed": int(blocks_processed),
    }
    event.update({str(key): value for key, value in counters.items()})
    return event


def freeze_thread_budget(
    *,
    heavy_processes: int,
    compute_threads_per_process: int,
    primary_pool: str,
) -> dict[str, Any]:
    processes = int(heavy_processes)
    per_process = int(compute_threads_per_process)
    if processes < 1 or processes > 2:
        raise ThreadBudgetError("heavy processes must be between one and two")
    if per_process < 1:
        raise ThreadBudgetError("compute threads per process must be positive")
    pool = str(primary_pool).lower()
    if pool not in THREAD_ENV_BY_POOL:
        raise ThreadBudgetError(f"unknown primary native thread pool: {primary_pool}")
    global_threads = processes * per_process
    if global_threads > GLOBAL_NATIVE_THREAD_MAX:
        raise ThreadBudgetError(
            f"global native thread budget exceeded: {global_threads} > {GLOBAL_NATIVE_THREAD_MAX}"
        )

    environment = {name: "1" for name in THREAD_ENV_BY_POOL.values()}
    environment[THREAD_ENV_BY_POOL[pool]] = str(per_process)
    payload: dict[str, Any] = {
        "heavy_processes": processes,
        "compute_threads_per_process": per_process,
        "global_active_native_compute_threads": global_threads,
        "global_native_compute_threads_max": GLOBAL_NATIVE_THREAD_MAX,
        "primary_pool": pool,
        "nested_parallelism": "FORBIDDEN",
        "environment": environment,
    }
    payload["thread_budget_hash"] = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return payload
