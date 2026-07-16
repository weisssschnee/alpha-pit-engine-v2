"""Write the immutable input binding for the Phase3CM streaming repair."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from our_system_phase2.services.phase3cm_streaming_frozen_inputs import (
    build_frozen_input_binding,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--freeze-manifest", type=Path, required=True)
    parser.add_argument("--pack", type=Path, required=True)
    parser.add_argument("--source-closure-sha", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    binding = build_frozen_input_binding(
        runtime_root=args.runtime_root,
        freeze_manifest=args.freeze_manifest,
        pack_path=args.pack,
        source_closure_sha=args.source_closure_sha,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(binding, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "status": binding["status"],
        "binding_hash": binding["binding_hash"],
        "pairs": binding["pair_count"],
        "candidate_members": binding["candidate_member_count"],
        "output": str(args.output),
    }, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
