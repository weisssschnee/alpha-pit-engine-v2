"""Regenerate the self-hashed Program optimizer tournament authorization."""

from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    AUTHORIZATION_RELATIVE_PATH,
    authorization_payload_v1,
)


def main() -> int:
    path = ROOT / AUTHORIZATION_RELATIVE_PATH
    payload = authorization_payload_v1()
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "path": str(path),
                "authorization_payload_sha256": payload[
                    "authorization_payload_sha256"
                ],
                "maximum_ask_plan_sha256": payload[
                    "maximum_ask_plan_sha256"
                ],
                "financial_reads": 0,
                "project_control_admission": "NOT_REQUESTED",
                "tournament": "NOT_RUN",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
