"""Verify the committed prospective Program tournament authorization."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
import sys

if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from our_system_phase2.runtime.cn_program_optimizer_tournament_v1 import (
    AUTHORIZATION_RELATIVE_PATH,
    authorization_payload_v1,
    verify_authorization,
)


def main() -> int:
    path = ROOT / AUTHORIZATION_RELATIVE_PATH
    payload = verify_authorization(path)
    if payload != authorization_payload_v1():
        raise RuntimeError("Program tournament authorization canonical replay drift")
    if not bool(payload["execution_authorized"]):
        raise RuntimeError("Program tournament is frozen but not requestable")
    if str(payload["tournament_status"]) != "FROZEN_NOT_RUN":
        raise RuntimeError("Program tournament status drift")
    if (
        int(payload["frozen_program_space_entry_count"]) != 3616
        or str(payload["frozen_program_space_sha256"])
        != "86d9bce8f7bdc75e55c791b6e101ec4beefe093e346abfc39c76ee1c30f354e0"
        or len(dict(payload["frozen_program_space_source_sha256"])) != 3
    ):
        raise RuntimeError("Program tournament exact Program space drift")
    if any(
        int(payload[key])
        for key in (
            "financial_data_reads",
            "validation_reads",
            "holdout_reads",
            "historical_2023_reads",
            "forward_b_reads",
            "forward_2026_reads",
        )
    ):
        raise RuntimeError("Program tournament freeze read boundary drift")
    print(
        json.dumps(
            {
                "status": "PASS",
                "authorization_payload_sha256": payload[
                    "authorization_payload_sha256"
                ],
                "execution_authorized": payload["execution_authorized"],
                "tournament": payload["tournament_status"],
                "financial_reads": payload["financial_data_reads"],
                "frozen_program_space_entry_count": payload[
                    "frozen_program_space_entry_count"
                ],
                "frozen_program_space_sha256": payload[
                    "frozen_program_space_sha256"
                ],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
