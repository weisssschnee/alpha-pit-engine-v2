from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.evaluation_asset_authority import (  # noqa: E402
    verify_historical_challenge_destructive_use,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Fail-closed destructive-use admission for the 2023 challenge."
    )
    parser.add_argument("--role-registry", type=Path, required=True)
    parser.add_argument("--access-started", type=Path, required=True)
    parser.add_argument("--outcome", type=Path, required=True)
    args = parser.parse_args(argv)
    proof = verify_historical_challenge_destructive_use(
        role_registry_path=args.role_registry,
        access_started_path=args.access_started,
        outcome_path=args.outcome,
    )
    print(json.dumps(proof, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
