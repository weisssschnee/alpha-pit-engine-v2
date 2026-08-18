"""Execute Large Fresh V2: Catalog Typed Evolution + rotating Uniform reserve."""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Mapping, Sequence

from scripts import run_cn_program_optimizer_large_fresh_v1 as base
from our_system_phase2.runtime.cn_program_optimizer_large_fresh_v1 import (
    ENHANCED_TEMPLATES,
)
from our_system_phase2.services.program_optimizer_large_fresh_v2 import (
    LargeFreshProgramBanditV2,
)
from our_system_phase2.services.program_search_optimizer_historical_v2 import (
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2,
)
from our_system_phase2.services.program_search_optimizer_v1 import UNIFORM_CONTROL

CAMPAIGN_ID = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V2"
CAMPAIGN_PROFILE = "cn_program_optimizer_large_fresh_development_v2"
FORMAL_SEARCH_AUTHORITY = "CATALOG_TYPED_EVOLUTION_AVAILABILITY_V2"
STATUS_COMPLETE = "LARGE_FRESH_DEVELOPMENT_SEARCH_V2_COMPLETE"
CLOSURE_NAME = "CN_PROGRAM_OPTIMIZER_LARGE_FRESH_DEVELOPMENT_V2_COMPLETE.json"
CLOSURE_SCHEMA_VERSION = "cn_program_optimizer_large_fresh_development_complete_v2"

SEEDS = {
    UNIFORM_CONTROL: 721003,
    CATALOG_TYPED_EVOLUTION_PROGRAM_V2: 721013,
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


def _checkpoint_arm_v2(macro_index: int, template_index: int) -> str:
    return (
        UNIFORM_CONTROL
        if template_index == macro_index % len(ENHANCED_TEMPLATES)
        else CATALOG_TYPED_EVOLUTION_PROGRAM_V2
    )


def _filtered_bandit_v2(authority: Mapping[str, Any]) -> LargeFreshProgramBanditV2:
    prior = set(map(str, authority["prior_ids"]))
    entries = tuple(
        entry
        for entry in authority["entries"]
        if entry.exact_identity not in prior
        and str(entry.genes["program_template_id"]) in set(ENHANCED_TEMPLATES)
    )
    return LargeFreshProgramBanditV2(
        campaign_id=CAMPAIGN_ID,
        entries_by_arm={
            UNIFORM_CONTROL: entries,
            CATALOG_TYPED_EVOLUTION_PROGRAM_V2: entries,
        },
        seeds=SEEDS,
        evolution_config=EVOLUTION_CONFIG,
    )


def _configure_base_runner() -> None:
    base.CAMPAIGN_ID = CAMPAIGN_ID
    base.CAMPAIGN_PROFILE = CAMPAIGN_PROFILE
    base.FORMAL_OPTIMIZER_ARM = CATALOG_TYPED_EVOLUTION_PROGRAM_V2
    base.FORMAL_SEARCH_AUTHORITY = FORMAL_SEARCH_AUTHORITY
    base.STATUS_COMPLETE = STATUS_COMPLETE
    base.CLOSURE_NAME = CLOSURE_NAME
    base.CLOSURE_SCHEMA_VERSION = CLOSURE_SCHEMA_VERSION
    base._checkpoint_arm = _checkpoint_arm_v2
    base._filtered_bandit = _filtered_bandit_v2


_BASE_RUNNER_MUTABLE_FIELDS = (
    "CAMPAIGN_ID",
    "CAMPAIGN_PROFILE",
    "FORMAL_OPTIMIZER_ARM",
    "FORMAL_SEARCH_AUTHORITY",
    "STATUS_COMPLETE",
    "CLOSURE_NAME",
    "CLOSURE_SCHEMA_VERSION",
    "_checkpoint_arm",
    "_filtered_bandit",
)


def run(
    args: argparse.Namespace,
    *,
    admission: Mapping[str, Any],
    authorization: Mapping[str, Any],
) -> dict[str, Any]:
    # V1 owns the evaluator/checkpoint machinery. V2 changes only policy state,
    # campaign identity, and closure semantics. Restore the imported V1 module
    # even on failure so a later route in the same interpreter cannot inherit
    # V2 globals.
    original = {name: getattr(base, name) for name in _BASE_RUNNER_MUTABLE_FIELDS}
    _configure_base_runner()
    try:
        return base.run(args, admission=admission, authorization=authorization)
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-freeze-root", type=Path, required=True)
    parser.add_argument("--prior-exact-freeze", type=Path, required=True)
    parser.add_argument("--execution-contract", type=Path, required=True)
    parser.add_argument("--train-field-root", type=Path, required=True)
    parser.add_argument("--train-price-root", type=Path, required=True)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--node-resource-capacity", type=Path, required=True)
    parser.add_argument("--node-resource-lease-receipt", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.error("run through the Project-Control V2 route, not this helper directly")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
