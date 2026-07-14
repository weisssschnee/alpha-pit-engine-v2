"""Single fixed-calendar split authority for formal CN evaluation runtimes.

The module deliberately contains no fallback splitter.  A formal caller must
provide the frozen 485-session manifest and every observed timestamp must be
covered by that manifest.  Validation and holdout roles are report-only and
can never be projected into an optimizer/feedback payload.
"""

from __future__ import annotations

import csv
import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd


SPLIT_AUTHORITY_VERSION = "cn_fixed_485_day_split_authority_v1"
OFFICIAL_DATE_COUNT = 485
OFFICIAL_SPLIT_COUNTS = {"train": 364, "validation": 73, "holdout": 48}
REPORT_ONLY_ROLES = frozenset({"validation", "holdout"})
FEEDBACK_ROLES = frozenset({"train"})


class SplitAuthorityError(RuntimeError):
    """Raised when a formal runtime cannot prove its fixed split role."""


class ForbiddenSplitAccessError(SplitAuthorityError):
    """Raised for sealed/forward access or report-only feedback attempts."""


def _date_key(value: Any) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return ""
    return pd.Timestamp(parsed).date().isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class FixedSplitAuthority:
    manifest_path: Path
    manifest_hash: str
    rows: tuple[dict[str, str], ...]
    split_by_date: Mapping[str, str]

    @classmethod
    def read(cls, path: Path, *, require_official: bool = True) -> "FixedSplitAuthority":
        resolved = Path(path).resolve()
        if not resolved.is_file():
            raise SplitAuthorityError(f"fixed split manifest does not exist: {resolved}")
        with resolved.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = [dict(row) for row in csv.DictReader(handle)]
        if not rows:
            raise SplitAuthorityError(f"fixed split manifest is empty: {resolved}")

        split_order = {"train": 0, "validation": 1, "holdout": 2}
        split_by_date: dict[str, str] = {}
        normalized: list[dict[str, str]] = []
        previous_date = ""
        previous_rank = -1
        for raw in rows:
            trade_date = _date_key(raw.get("trade_date"))
            split = str(raw.get("split") or "").strip().lower()
            if not trade_date or split not in split_order:
                raise SplitAuthorityError(f"invalid fixed split manifest row: {raw}")
            if trade_date >= "2026-01-01":
                raise ForbiddenSplitAccessError(f"forward/sealed date in split manifest: {trade_date}")
            if trade_date in split_by_date:
                raise SplitAuthorityError(f"duplicate fixed split manifest date: {trade_date}")
            if previous_date and trade_date <= previous_date:
                raise SplitAuthorityError("fixed split manifest dates are not strictly increasing")
            rank = split_order[split]
            if rank < previous_rank:
                raise SplitAuthorityError("fixed split manifest roles are not chronologically contiguous")
            previous_date = trade_date
            previous_rank = rank
            split_by_date[trade_date] = split
            normalized.append({**raw, "trade_date": trade_date, "split": split})

        counts = {role: sum(value == role for value in split_by_date.values()) for role in split_order}
        if require_official and (len(normalized) != OFFICIAL_DATE_COUNT or counts != OFFICIAL_SPLIT_COUNTS):
            raise SplitAuthorityError(
                "formal CN split manifest must be the fixed 485-session 364/73/48 contract; "
                f"observed dates={len(normalized)} counts={counts}"
            )
        return cls(
            manifest_path=resolved,
            manifest_hash=file_sha256(resolved),
            rows=tuple(normalized),
            split_by_date=split_by_date,
        )

    def role_for(self, value: Any) -> str:
        trade_date = _date_key(value)
        if not trade_date:
            raise SplitAuthorityError(f"cannot resolve trade date from value: {value!r}")
        if trade_date >= "2026-01-01":
            raise ForbiddenSplitAccessError(f"FORWARD_2026_SEALED: {trade_date}")
        try:
            return str(self.split_by_date[trade_date])
        except KeyError as exc:
            raise SplitAuthorityError(f"date is not covered by fixed split manifest: {trade_date}") from exc

    def map_times(self, values: Iterable[Any]) -> dict[pd.Timestamp, str]:
        return {pd.Timestamp(value): self.role_for(value) for value in values}

    def assign_rows(self, rows: Iterable[dict[str, Any]]) -> None:
        for row in rows:
            value = row.get("trade_date") or row.get("trade_time")
            row["split"] = self.role_for(value)

    def assert_feedback_rows(self, rows: Iterable[Mapping[str, Any]]) -> None:
        for row in rows:
            role = str(
                row.get("optimizer_reward_split")
                or row.get("feedback_data_role")
                or row.get("split")
                or ""
            ).strip().lower()
            if role == "development":
                role = "train"
            if role in REPORT_ONLY_ROLES:
                raise ForbiddenSplitAccessError(f"report-only role cannot feed search policy: {role}")
            if role not in FEEDBACK_ROLES:
                raise SplitAuthorityError(f"feedback row has no authorized development/train role: {role or '<missing>'}")
