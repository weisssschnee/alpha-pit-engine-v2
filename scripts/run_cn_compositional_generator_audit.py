from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.compositional_generator_audit import (  # noqa: E402
    audit_generator_expressivity,
)
from our_system_phase2.services.compositional_grammar import (  # noqa: E402
    GRAMMAR_VERSION,
    skeleton_registry,
)
from our_system_phase2.services.matched_control_pairs import (  # noqa: E402
    COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX,
)
from our_system_phase2.services.typed_route_compiler import (  # noqa: E402
    COMPILER_VERSION,
    ROUTE_PRIMITIVE_ALLOWLIST,
)
from our_system_phase2.services.unified_capability_registry import (  # noqa: E402
    UnifiedCapabilityRegistry,
    stable_hash,
)


DEFAULT_REGISTRY = (
    REPO
    / "runtime/field_registry/cn_unified_capability_registry_v3_20260717"
    / "unified_capability_registry.json"
)
DEFAULT_OUTPUT = REPO / "runtime/cn_compositional_nline_large_search_20260715"


def _write_json(path: Path, payload: Any) -> str:
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--attempts-per-generator", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=1729)
    args = parser.parse_args()

    registry = UnifiedCapabilityRegistry.read(args.registry)
    audit = audit_generator_expressivity(
        registry,
        attempts_per_generator=args.attempts_per_generator,
        seed=args.seed,
    )
    skeletons = {
        route_id: [asdict(row) for row in rows]
        for route_id, rows in skeleton_registry().items()
    }
    grammar = {
        "grammar_version": GRAMMAR_VERSION,
        "compiler_version": COMPILER_VERSION,
        "unified_registry_version": registry.registry_version,
        "unified_registry_hash": registry.registry_hash,
        "authority_scope": "EXPERIMENTAL_BOUNDED_DEVELOPMENT_SEARCH",
        "formal_global_search_unfrozen": False,
        "plate_industry_pit_route": "DISABLED_FAIL_CLOSED",
        "broad_event_policy": "FROZEN_REFERENCE_ONLY_NO_NEW_SEARCH_BUDGET",
        "candidate_limits": {
            "leaf_fields_min": 1,
            "leaf_fields_max": 3,
            "expression_depth_min": 1,
            "expression_depth_max": 4,
            "temporal_windows_max": 2,
            "condition_gates_max": 1,
            "cross_sectional_outer_mappings_max": 1,
        },
        "route_primitive_allowlists": {
            route_id: sorted(values)
            for route_id, values in ROUTE_PRIMITIVE_ALLOWLIST.items()
        },
        "control_constructors": COMPOSITIONAL_CONTROL_CONSTRUCTOR_MATRIX,
        "skeleton_registry_hash": stable_hash(skeletons),
        "behavior_claim_ceiling": "STRUCTURAL_GRAMMAR_DOES_NOT_PROVE_SIGNAL_BEHAVIOR_IDENTITY",
    }
    output = args.output_dir.resolve()
    hashes = {
        "CN_GENERATOR_EXPRESSIVITY_AUDIT.json": _write_json(
            output / "CN_GENERATOR_EXPRESSIVITY_AUDIT.json", audit
        ),
        "CN_TYPED_COMPOSITIONAL_GRAMMAR_V2.json": _write_json(
            output / "CN_TYPED_COMPOSITIONAL_GRAMMAR_V2.json", grammar
        ),
        "CN_SKELETON_REGISTRY.json": _write_json(
            output / "CN_SKELETON_REGISTRY.json", skeletons
        ),
    }
    print(
        json.dumps(
            {
                "status": "CN_GENERATOR_EXPRESSIVITY_AUDIT_COMPLETED",
                "attempts_per_generator": args.attempts_per_generator,
                "economic_evaluator_accessed": False,
                "output_dir": str(output),
                "artifact_sha256": hashes,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
