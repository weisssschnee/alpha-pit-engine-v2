"""Materialize the deterministic Search V2 prospective canary authorization."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.runtime.cn_joint_program_search_v2_canary import (  # noqa: E402
    CANARY_AUTHORIZATION_RELATIVE_PATH,
    authorization_payload_v1,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=REPO / CANARY_AUTHORIZATION_RELATIVE_PATH,
    )
    parser.add_argument("--replace", action="store_true")
    args = parser.parse_args(argv)
    output = args.output.resolve()
    payload = authorization_payload_v1()
    serialized = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    if output.exists() and not args.replace:
        if output.read_text(encoding="utf-8") != serialized:
            raise FileExistsError(f"Search V2 frozen plan differs: {output}")
        print(json.dumps({"status": "UNCHANGED", "path": str(output)}))
        return 0
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(serialized, encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "SEARCH_V2_CANARY_FROZEN_NOT_RUN",
                "path": str(output),
                "authorization_payload_sha256": payload[
                    "authorization_payload_sha256"
                ],
                "financial_data_reads": 0,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
