"""Smoke-test the CE1 typed primitive gate at mature G2 selector input.

This command does not run G2 selection. It only validates the input candidate
rows that would be handed to the selector.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any

from our_system_phase2.domain.models import utc_now_iso
from our_system_phase2.services.artifact_schema import write_json_artifact
from our_system_phase2.services.typed_primitive_gate import REGISTRY_VERSION, gate_g2_input_rows


DEFAULT_INPUT = Path(
    "reports/phase3ce1_factor_pack_preflight_gate_smoke_20260618/"
    "cn_factor_pack_candidate_integration.csv"
)
DEFAULT_OUTPUT = Path("reports/phase3ce1_g2_input_gate_smoke_20260618")
SMOKE_VERSION = "phase3ce1-g2-input-gate-smoke-v1-2026-06-18"


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return [dict(row) for row in csv.DictReader(fh)]


def _read_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    if isinstance(payload, list):
        return [dict(row) for row in payload]
    if not isinstance(payload, dict):
        raise ValueError(f"{path} is not a candidate list or object")
    for key in ("candidate_pool", "candidates", "rows", "selected", "default_selected"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return [dict(row) for row in rows]
    raise ValueError(f"{path} does not contain a recognized candidate list")


def _read_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv(path)
    if suffix == ".json":
        return _read_json(path)
    raise ValueError(f"Unsupported candidate input suffix: {path.suffix}")


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row.keys():
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def _counter(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(key) or "unknown") for row in rows).items()))


def _write_markdown(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CE1 G2 Input Gate Smoke",
        "",
        f"- created_at: {summary['created_at']}",
        f"- input: `{summary['input_path']}`",
        f"- registry_version: `{summary['registry_version']}`",
        f"- input rows: {summary['input_rows']}",
        f"- allowed rows: {summary['allowed_rows']}",
        f"- rejected rows: {summary['rejected_rows']}",
        f"- decision: {summary['decision']}",
        "",
        "## Rejected Decisions",
        "",
    ]
    for key, value in summary["rejected_decision_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(["", "## Rejected Reasons", ""])
    for key, value in summary["rejected_reason_counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Outputs",
            "",
            "- g2_input_allowed.csv",
            "- g2_input_rejected.csv",
            "- g2_input_gate_summary.json",
            "",
            "This smoke does not run selector scoring, replay, or official book promotion.",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    rows = _read_rows(args.input)
    allowed, rejected = gate_g2_input_rows(
        rows,
        entry_lineage="phase3ce1_g2_input_gate_smoke",
        materialization_stage="g2_selector_input_smoke",
        candidate_role="mature_g2_candidate",
    )
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)
    _write_csv(output_root / "g2_input_allowed.csv", allowed)
    _write_csv(output_root / "g2_input_rejected.csv", rejected)
    summary = {
        "phase3_version": SMOKE_VERSION,
        "created_at": utc_now_iso(),
        "input_path": str(args.input),
        "output_root": str(output_root),
        "registry_version": REGISTRY_VERSION,
        "scope": "typed_primitive_gate_at_g2_selector_input_no_selector_no_replay",
        "input_rows": len(rows),
        "allowed_rows": len(allowed),
        "rejected_rows": len(rejected),
        "input_existing_decision_counts": _counter(rows, "typed_gate_decision"),
        "allowed_decision_counts": _counter(allowed, "typed_gate_decision"),
        "rejected_decision_counts": _counter(rejected, "typed_gate_decision"),
        "rejected_reason_counts": _counter(rejected, "typed_gate_reason"),
        "rejected_gate_reason_counts": _counter(rejected, "g2_input_gate_reason"),
        "decision": "PASS_G2_INPUT_GATE_ACTIVE" if rejected or allowed else "HOLD_NO_INPUT_ROWS",
    }
    write_json_artifact(output_root / "g2_input_gate_summary.json", summary)
    _write_markdown(output_root / "PHASE3CE1_G2_INPUT_GATE_SMOKE_20260618.md", summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
