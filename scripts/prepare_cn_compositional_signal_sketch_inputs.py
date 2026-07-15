from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_signal_sketch_inputs import (  # noqa: E402
    classify_signal_sketch_receipts,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    UnifiedCapabilityRegistry,
    stable_hash,
)


RUNTIME = REPO / "runtime/cn_compositional_nline_large_search_20260715"
REGISTRY = (
    REPO
    / "reports/cn_unified_capability_discovery_20260714/completed_f8169e1/registry"
    / "unified_capability_registry.json"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def _write_csv(path: Path, rows: list[Mapping[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")


def _candidate_row(receipt: Mapping[str, Any]) -> dict[str, Any]:
    expression = str(receipt["canonical_expression"])
    return {
        "candidate_id": str(receipt["candidate_id"]),
        "exact_identity": str(receipt["exact_identity"]),
        "expression": expression,
        "expression_hash": hashlib.sha256(expression.encode("utf-8")).hexdigest(),
        "generator_arm": str(receipt["policy_id"]),
        "family_id": str(receipt["skeleton_id"]),
        "motif_id": str(receipt["route_id"]),
        "route_id": str(receipt["route_id"]),
        "seed": int(receipt["seed"]),
        "admission_reward_accessed": False,
    }


def _fidelity_sample(rows: list[dict[str, Any]], maximum: int) -> list[dict[str, Any]]:
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_route[str(row["route_id"])].append(row)
    ordered: list[dict[str, Any]] = []
    for route_id in sorted(by_route):
        ordered.extend(
            sorted(
                by_route[route_id],
                key=lambda row: stable_hash(
                    {"fidelity": "COMPOSITIONAL_V1", "exact_identity": row["exact_identity"]}
                ),
            )
        )
    selected: list[dict[str, Any]] = []
    active = {route_id: list(rows) for route_id, rows in by_route.items()}
    while active and len(selected) < maximum:
        for route_id in sorted(list(active)):
            if active[route_id] and len(selected) < maximum:
                selected.append(active[route_id].pop(0))
            if not active[route_id]:
                del active[route_id]
    return selected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipts", type=Path, default=RUNTIME / "CN_PAIR_RECEIPTS.jsonl")
    parser.add_argument("--preadmission", type=Path, default=RUNTIME / "CN_STRUCTURAL_PREADMISSION.json")
    parser.add_argument("--registry", type=Path, default=REGISTRY)
    parser.add_argument("--output-root", type=Path, default=RUNTIME / "signal_sketch")
    parser.add_argument("--fidelity-count", type=int, default=384)
    args = parser.parse_args()

    preadmission = json.loads(args.preadmission.read_text(encoding="utf-8"))
    selected = {str(value) for value in preadmission["exact_identities"]}
    registry = UnifiedCapabilityRegistry.read(args.registry)
    classification = classify_signal_sketch_receipts(
        _read_jsonl(args.receipts),
        selected,
        source_family_by_field={row.field_id: row.source_family for row in registry.fields},
    )
    output = args.output_root.resolve()
    active_rows = [_candidate_row(row) for row in classification.active_rows]
    session_rows = [_candidate_row(row) for row in classification.session_rows]
    _write_csv(output / "active_generation.csv", active_rows)
    _write_csv(output / "session_generation.csv", session_rows)
    _write_jsonl(output / "session_pair_receipts.jsonl", classification.session_rows)
    _write_csv(
        output / "active_exact_fidelity_candidates.csv",
        _fidelity_sample(active_rows, min(args.fidelity_count, len(active_rows))),
    )
    _write_csv(
        output / "session_exact_fidelity_candidates.csv",
        _fidelity_sample(session_rows, min(args.fidelity_count, len(session_rows))),
    )
    (output / "session_context_fields.txt").write_text(
        "\n".join(classification.session_context_fields) + "\n",
        encoding="utf-8",
    )
    manifest = {
        "status": "COMPOSITIONAL_SIGNAL_SKETCH_INPUTS_PREPARED",
        **classification.summary,
        "active_generation_sha256": _sha256(output / "active_generation.csv"),
        "session_generation_sha256": _sha256(output / "session_generation.csv"),
        "session_receipts_sha256": _sha256(output / "session_pair_receipts.jsonl"),
        "session_exact_fidelity_sha256": _sha256(
            output / "session_exact_fidelity_candidates.csv"
        ),
        "preadmission_sha256": _sha256(args.preadmission),
        "pair_receipts_sha256": _sha256(args.receipts),
        "registry_hash": registry.registry_hash,
        "canonical_fundamental_fields": list(classification.canonical_fundamental_fields),
        "session_context_fields": list(classification.session_context_fields),
        "labels_or_returns_read": False,
        "data_roles_accessed": [],
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "input_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
