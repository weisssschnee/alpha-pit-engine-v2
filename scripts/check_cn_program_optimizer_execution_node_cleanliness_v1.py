from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
import hashlib
import json
import os
import platform
from typing import Any, Sequence

import psutil


STALE_COMMAND_MARKERS = (
    "cn_program_optimizer_tournament_fourth_retry_preflight",
    "run_cn_program_optimizer_tournament_v1.py",
    "cn-program-optimizer-tournament-v1",
)
ABNORMAL_PRIVATE_BYTES = 32 * 1024**3


class PerformanceInformation(ctypes.Structure):
    _fields_ = [
        ("cb", wintypes.DWORD),
        ("CommitTotal", ctypes.c_size_t),
        ("CommitLimit", ctypes.c_size_t),
        ("CommitPeak", ctypes.c_size_t),
        ("PhysicalTotal", ctypes.c_size_t),
        ("PhysicalAvailable", ctypes.c_size_t),
        ("SystemCache", ctypes.c_size_t),
        ("KernelTotal", ctypes.c_size_t),
        ("KernelPaged", ctypes.c_size_t),
        ("KernelNonpaged", ctypes.c_size_t),
        ("PageSize", ctypes.c_size_t),
        ("HandleCount", wintypes.DWORD),
        ("ProcessCount", wintypes.DWORD),
        ("ThreadCount", wintypes.DWORD),
    ]


def _resource_snapshot() -> dict[str, int]:
    if platform.system() != "Windows":
        raise RuntimeError("execution-node cleanliness check requires Windows")
    info = PerformanceInformation()
    info.cb = ctypes.sizeof(info)
    if not ctypes.windll.psapi.GetPerformanceInfo(ctypes.byref(info), info.cb):
        raise ctypes.WinError()
    page_size = int(info.PageSize)
    swap = psutil.swap_memory()
    return {
        "physical_total_bytes": int(info.PhysicalTotal) * page_size,
        "physical_available_bytes": int(info.PhysicalAvailable) * page_size,
        "committed_bytes": int(info.CommitTotal) * page_size,
        "commit_limit_bytes": int(info.CommitLimit) * page_size,
        "commit_headroom_bytes": int(info.CommitLimit - info.CommitTotal) * page_size,
        "pagefile_total_bytes": int(swap.total),
        "pagefile_used_bytes": int(swap.used),
        "pagefile_pages_in_bytes": int(swap.sin),
        "pagefile_pages_out_bytes": int(swap.sout),
    }


def _process_findings() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    own_pid = os.getpid()
    stale: list[dict[str, Any]] = []
    abnormal: list[dict[str, Any]] = []
    for process in psutil.process_iter(
        ["pid", "ppid", "name", "cmdline", "create_time", "memory_info"]
    ):
        try:
            if process.pid == own_pid:
                continue
            command = " ".join(process.info.get("cmdline") or ())
            private_bytes = int(process.info["memory_info"].private)
            row = {
                "pid": int(process.pid),
                "ppid": int(process.info.get("ppid") or 0),
                "name": str(process.info.get("name") or ""),
                "command_line": command,
                "create_time": float(process.info.get("create_time") or 0.0),
                "private_bytes": private_bytes,
            }
            if any(marker.lower() in command.lower() for marker in STALE_COMMAND_MARKERS):
                stale.append(row)
            if private_bytes >= ABNORMAL_PRIVATE_BYTES:
                abnormal.append(row)
        except (psutil.AccessDenied, psutil.NoSuchProcess, AttributeError):
            continue
    return stale, abnormal


def check(*, minimum_commit_headroom_bytes: int) -> dict[str, Any]:
    resource = _resource_snapshot()
    stale, abnormal = _process_findings()
    pagefile_sane = (
        resource["pagefile_total_bytes"] > 0
        and resource["commit_limit_bytes"] >= resource["physical_total_bytes"]
    )
    passed = (
        not stale
        and not abnormal
        and pagefile_sane
        and resource["commit_headroom_bytes"] >= minimum_commit_headroom_bytes
    )
    payload: dict[str, Any] = {
        "schema_version": "cn_program_optimizer_execution_node_cleanliness_v1",
        "status": "PASS" if passed else "FAIL",
        "host": platform.node().upper(),
        "stale_tournament_processes": stale,
        "abnormal_large_processes": abnormal,
        "abnormal_private_bytes_threshold": ABNORMAL_PRIVATE_BYTES,
        "minimum_commit_headroom_bytes": minimum_commit_headroom_bytes,
        "pagefile_sane": pagefile_sane,
        "resource": resource,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    payload["check_payload_sha256"] = hashlib.sha256(encoded).hexdigest()
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minimum-commit-headroom-gib", type=int, default=24)
    args = parser.parse_args(argv)
    if args.minimum_commit_headroom_gib < 1:
        parser.error("minimum-commit-headroom-gib must be positive")
    result = check(
        minimum_commit_headroom_bytes=args.minimum_commit_headroom_gib * 1024**3
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
