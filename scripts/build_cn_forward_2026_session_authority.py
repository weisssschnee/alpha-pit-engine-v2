"""Build the one-shot report-only 2026 forward session authority.

This is a role-specific wrapper around the already-qualified A-share session
authority builder.  It keeps forward reads explicit and never aliases them to
validation or holdout evidence.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.build_cn_validation_session_authority import (
    build_validation_session_authority,
)


SCHEMA_VERSION = "cn_forward_2026_session_authority_v1"
STATUS = "FORWARD_2026_SESSION_AUTHORITY_CLOSED_IMMUTABLE"
DATE_MIN = "2026-01-05"
DATE_MAX = "2026-04-10"
EVIDENCE_SCOPE = "ONE_SHOT_FORWARD_2026_CONFIRMATION_INPUT"


def build_forward_2026_session_authority(**kwargs):
    return build_validation_session_authority(
        **kwargs,
        evaluation_role="forward_2026",
        date_min=DATE_MIN,
        date_max=DATE_MAX,
        schema_version=SCHEMA_VERSION,
        status=STATUS,
        evidence_scope=EVIDENCE_SCOPE,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--field-manifest", type=Path, required=True)
    parser.add_argument("--public-source-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--expected-field-manifest-sha256", required=True)
    parser.add_argument("--expected-source-manifest-sha256", required=True)
    parser.add_argument("--historical-daily-st-source", type=Path, required=True)
    parser.add_argument("--expected-daily-st-source-sha256", required=True)
    parser.add_argument("--builder-commit-sha", required=True)
    args = parser.parse_args()
    result = build_forward_2026_session_authority(
        field_manifest_path=args.field_manifest,
        public_source_root=args.public_source_root,
        output_root=args.output_root,
        expected_field_manifest_sha256=args.expected_field_manifest_sha256,
        expected_source_manifest_sha256=args.expected_source_manifest_sha256,
        historical_daily_st_source=args.historical_daily_st_source,
        expected_daily_st_source_sha256=args.expected_daily_st_source_sha256,
        builder_commit_sha=args.builder_commit_sha,
    )
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
