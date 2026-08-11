from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SRC = REPO / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from our_system_phase2.services.project_control_admission import (  # noqa: E402
    ALLOWED_ACTIONS,
    materialize_admission,
    sha256_file,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Freeze an action-specific CN Project Control admission."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-record", type=Path, required=True)
    parser.add_argument("--receipt-sha256", required=True)
    parser.add_argument("--action", choices=sorted(ALLOWED_ACTIONS), required=True)
    parser.add_argument("--target-campaign-id", required=True)
    parser.add_argument("--target-run-id", required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--parent-post-batch-run-record", type=Path)
    parser.add_argument("--parent-post-batch-receipt-sha256", default="")
    parser.add_argument("--recovery-kind", default="")
    parser.add_argument("--recovery-of-target-run-id", default="")
    parser.add_argument("--incident-id", default="")
    args = parser.parse_args(argv)
    payload = materialize_admission(
        output_path=args.output,
        project_control_run_record_path=args.run_record,
        expected_project_control_receipt_sha256=args.receipt_sha256,
        requested_action=args.action,
        target_campaign_id=args.target_campaign_id,
        target_run_id=args.target_run_id,
        repo_sha=args.repo_sha,
        parent_post_batch_run_record_path=args.parent_post_batch_run_record,
        expected_parent_post_batch_receipt_sha256=(
            args.parent_post_batch_receipt_sha256
        ),
        recovery_kind=args.recovery_kind,
        recovery_of_target_run_id=args.recovery_of_target_run_id,
        incident_id=args.incident_id,
    )
    print(
        json.dumps(
            {
                "status": "PROJECT_CONTROL_ADMISSION_MATERIALIZED",
                "output": str(args.output.resolve()),
                "file_sha256": sha256_file(args.output),
                "payload_sha256": payload["admission_payload_sha256"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
