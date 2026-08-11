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
    build_execution_request,
)
from materialize_cn_search_preflight_authorization import (  # noqa: E402
    planned_authorization_binding,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build a target-bound request for a Harness task_spec.json."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--action", choices=sorted(ALLOWED_ACTIONS), required=True)
    parser.add_argument("--target-campaign-id", required=True)
    parser.add_argument("--target-run-id", required=True)
    parser.add_argument("--target-output-root", type=Path, required=True)
    parser.add_argument("--repo-sha", required=True)
    parser.add_argument("--expires-at", required=True)
    parser.add_argument("--campaign-authorization-path", type=Path)
    parser.add_argument("--preflight-authorization-source", type=Path)
    parser.add_argument("--campaign-authorization-file-sha256", default="")
    parser.add_argument("--target-campaign-instance-id", default="")
    parser.add_argument("--target-campaign-profile", default="")
    parser.add_argument("--parent-project-control-run-id", default="")
    parser.add_argument("--parent-target-campaign-id", default="")
    parser.add_argument("--parent-target-run-id", default="")
    parser.add_argument("--recovery-kind", default="")
    parser.add_argument("--recovery-of-target-run-id", default="")
    parser.add_argument("--original-admission-path", default="")
    parser.add_argument("--original-admission-file-sha256", default="")
    parser.add_argument("--incident-id", default="")
    parser.add_argument("--incident-path", default="")
    parser.add_argument("--incident-file-sha256", default="")
    args = parser.parse_args(argv)
    if args.preflight_authorization_source:
        if not args.campaign_authorization_path:
            parser.error(
                "--preflight-authorization-source requires the future "
                "--campaign-authorization-path"
            )
        if any(
            (
                args.campaign_authorization_file_sha256,
                args.target_campaign_instance_id,
                args.target_campaign_profile,
            )
        ):
            parser.error(
                "preflight authorization hash/identity fields are derived, "
                "not caller supplied"
            )
        authorization_binding = planned_authorization_binding(
            args.preflight_authorization_source,
            args.campaign_authorization_path,
        )
    else:
        authorization_binding = {
            "campaign_authorization_path": args.campaign_authorization_path or "",
            "campaign_authorization_file_sha256": (
                args.campaign_authorization_file_sha256
            ),
            "target_campaign_instance_id": args.target_campaign_instance_id,
            "target_campaign_profile": args.target_campaign_profile,
        }
    payload = build_execution_request(
        requested_action=args.action,
        target_campaign_id=args.target_campaign_id,
        target_run_id=args.target_run_id,
        target_output_root=args.target_output_root,
        repo_sha=args.repo_sha,
        expires_at=args.expires_at,
        **authorization_binding,
        parent_project_control_run_id=args.parent_project_control_run_id,
        parent_target_campaign_id=args.parent_target_campaign_id,
        parent_target_run_id=args.parent_target_run_id,
        recovery_kind=args.recovery_kind,
        recovery_of_target_run_id=args.recovery_of_target_run_id,
        original_admission_path=args.original_admission_path,
        original_admission_file_sha256=args.original_admission_file_sha256,
        incident_id=args.incident_id,
        incident_path=args.incident_path,
        incident_file_sha256=args.incident_file_sha256,
    )
    output = args.output.expanduser().resolve()
    if output.exists():
        raise FileExistsError(f"execution request output exists: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({"status": "EXECUTION_REQUEST_READY", **payload}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
