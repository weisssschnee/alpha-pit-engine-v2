from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.broad_event_semantics import load_and_build, validate_semantic_registry


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field-registry", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    registry = load_and_build(args.field_registry)
    validate_semantic_registry(registry)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(registry, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"fields": registry["scanned_field_count"], "external": registry["external_sidecar_field_count"], "hash": registry["registry_hash"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
