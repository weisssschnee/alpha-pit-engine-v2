"""Large Fresh V3: Primitive-local primary + Uniform reserve + Typed Evolution challenger."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_program_optimizer_large_fresh_v1 as base
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import ENHANCED_TEMPLATES
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v2 import (
    PRIMARY_EXECUTOR_WORKERS,
    RESOURCE_CANARY_FIELD_COLUMNS,
    RESOURCE_CANARY_PROBE_SECONDS,
    RESOURCE_FALLBACK_EXECUTOR_WORKERS,
)
from our_system_phase2.services.program_optimizer_large_fresh_v3 import (
    ARMS,
    LargeFreshProgramBanditV3,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.program_search_optimizer_v1 import UNIFORM_CONTROL
from our_system_phase2.services.program_search_primitive_credit_v1 import (
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1,
    primitive_program_metadata_v1,
    verify_primitive_stats_v1,
)


CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V3"
CAMPAIGN_PROFILE = "cn_program_optimizer_large_fresh_development_v3"
FORMAL_SEARCH_AUTHORITY = "HIERARCHICAL_PRIMITIVE_CREDIT_AVAILABILITY_V1"
STATUS_COMPLETE = "LARGE_FRESH_DEVELOPMENT_SEARCH_V3_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V3_COMPLETE.json"
CLOSURE_SCHEMA_VERSION = "cn_program_optimizer_large_fresh_development_complete_v3"
PRIMITIVE_STATS_RELATIVE_PATH = Path(
    "runtime/run_plans/cn_stage_c_primitive_credit_stats_20260820.json"
)

SEEDS = {
    UNIFORM_CONTROL: 731003,
    PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1: 731007,
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2: 731013,
}
EVOLUTION_CONFIG = {
    "warmup": 32,
    "tournament_size": 4,
    "population_limit": 256,
    "template_cell_limit": 64,
    "gene_mutation_probability": 0.55,
    "skeleton_mutation_probability": 0.25,
    "crossover_probability": 0.20,
    "minimum_mutated_factors": 1,
    "maximum_mutated_factors": 3,
    "duplicate_resample_limit": 64,
}


def _checkpoint_arm_v3(macro_index: int, template_index: int) -> str:
    """Allocate 5/7 checkpoints to Primitive, 1/7 Uniform, 1/7 Evolution."""
    template_count = len(ENHANCED_TEMPLATES)
    uniform_index = int(macro_index) % template_count
    evolution_index = (int(macro_index) + 3) % template_count
    if int(template_index) == uniform_index:
        return UNIFORM_CONTROL
    if int(template_index) == evolution_index:
        return CATALOG_TYPED_EVOLUTION_PROGRAM_V2
    return PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1


def _load_primitive_stats() -> dict[str, Any]:
    repo_root = Path(__file__).resolve().parents[1]
    path = (repo_root / PRIMITIVE_STATS_RELATIVE_PATH).resolve()
    payload = json.loads(path.read_text(encoding="utf-8-sig"))
    return verify_primitive_stats_v1(payload)


def _filtered_bandit_v3(authority: Mapping[str, Any]) -> LargeFreshProgramBanditV3:
    prior = set(map(str, authority["prior_ids"]))
    entries = tuple(
        entry
        for entry in authority["entries"]
        if entry.exact_identity not in prior
        and str(entry.genes["program_template_id"]) in set(ENHANCED_TEMPLATES)
    )
    stats = _load_primitive_stats()
    metadata = primitive_program_metadata_v1(
        entries=entries,
        catalog_by_exact=authority["catalog_by_exact"],
    )
    primitive_config = {
        "metadata_by_exact_identity": metadata,
        "primitive_stats": stats,
        "primitive_stats_payload_sha256": str(stats["stats_payload_sha256"]),
    }
    return LargeFreshProgramBanditV3(
        campaign_id=CAMPAIGN_ID,
        entries_by_arm={arm: entries for arm in ARMS},
        seeds=SEEDS,
        primitive_config=primitive_config,
        evolution_config=EVOLUTION_CONFIG,
    )


def _configure_base_runner() -> None:
    base.CAMPAIGN_ID = CAMPAIGN_ID
    base.CAMPAIGN_PROFILE = CAMPAIGN_PROFILE
    base.FORMAL_OPTIMIZER_ARM = PRIMITIVE_LOCAL_HIERARCHICAL_PROGRAM_V1
    base.FORMAL_SEARCH_AUTHORITY = FORMAL_SEARCH_AUTHORITY
    base.STATUS_COMPLETE = STATUS_COMPLETE
    base.CLOSURE_NAME = CLOSURE_NAME
    base.CLOSURE_SCHEMA_VERSION = CLOSURE_SCHEMA_VERSION
    base.PRIMARY_EXECUTOR_WORKERS = PRIMARY_EXECUTOR_WORKERS
    base.RESOURCE_FALLBACK_EXECUTOR_WORKERS = RESOURCE_FALLBACK_EXECUTOR_WORKERS
    base.RESOURCE_CANARY_FIELD_COLUMNS = RESOURCE_CANARY_FIELD_COLUMNS
    base.RESOURCE_CANARY_REQUIRE_MINIMUM_FREE_PHYSICAL = False
    base.RESOURCE_CANARY_PROBE_SECONDS = RESOURCE_CANARY_PROBE_SECONDS
    base.RESOURCE_CANARY_PREVIEW_FIRST_CHECKPOINT = True
    base._checkpoint_arm = _checkpoint_arm_v3
    base._filtered_bandit = _filtered_bandit_v3


_BASE_RUNNER_MUTABLE_FIELDS = (
    "CAMPAIGN_ID",
    "CAMPAIGN_PROFILE",
    "FORMAL_OPTIMIZER_ARM",
    "FORMAL_SEARCH_AUTHORITY",
    "STATUS_COMPLETE",
    "CLOSURE_NAME",
    "CLOSURE_SCHEMA_VERSION",
    "PRIMARY_EXECUTOR_WORKERS",
    "RESOURCE_FALLBACK_EXECUTOR_WORKERS",
    "RESOURCE_CANARY_FIELD_COLUMNS",
    "RESOURCE_CANARY_REQUIRE_MINIMUM_FREE_PHYSICAL",
    "RESOURCE_CANARY_PROBE_SECONDS",
    "RESOURCE_CANARY_PREVIEW_FIRST_CHECKPOINT",
    "_checkpoint_arm",
    "_filtered_bandit",
)


def run(
    args: Any,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    original = {name: getattr(base, name) for name in _BASE_RUNNER_MUTABLE_FIELDS}
    _configure_base_runner()
    try:
        return base.run(args, admission=admission, authorization=authorization)
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def main(argv: Sequence[str] | None = None) -> int:
    raise SystemExit("run through a Project-Control-authorized V3 route")


if __name__ == "__main__":
    main()


__all__ = [
    "CAMPAIGN_ID",
    "CAMPAIGN_PROFILE",
    "FORMAL_SEARCH_AUTHORITY",
    "SEEDS",
    "EVOLUTION_CONFIG",
    "_checkpoint_arm_v3",
    "_filtered_bandit_v3",
    "run",
]
