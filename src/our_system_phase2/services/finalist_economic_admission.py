"""Train-only economic admission rules for CN alpha finalists.

Development search feedback remains owned by Phase3CM.  These predicates are
used only after an immutable A-share replay has closed, so relative control
outperformance cannot by itself turn a loss-making primary into a finalist.
"""

from __future__ import annotations

import math
from typing import Any, Mapping


POLICY_ID = "CN_FINALIST_ABSOLUTE_ECONOMIC_ADMISSION_V1"
MINIMUM_PRIMARY_NET_REWARD = 0.0
MINIMUM_MATCHED_NET_INCREMENT = 0.0
MAXIMUM_TERMINAL_HOLDINGS_WEIGHT = 0.05


def _finite_float(value: Any) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    return numeric if math.isfinite(numeric) else None


def strict_replay_blockers(row: Mapping[str, Any]) -> tuple[str, ...]:
    """Return stable blockers for strict executable-train admission."""
    blockers: list[str] = []
    if str(row.get("a_share_replay_status") or "") != "PAIR_REPLAY_COMPLETE":
        blockers.append("PAIR_REPLAY_NOT_COMPLETE")
    primary = _finite_float(row.get("primary_a_share_executable_net_reward"))
    if primary is None or primary <= MINIMUM_PRIMARY_NET_REWARD:
        blockers.append("PRIMARY_ABSOLUTE_NET_REWARD_NOT_POSITIVE")
    increment = _finite_float(row.get("a_share_executable_net_increment"))
    if increment is None or increment <= MINIMUM_MATCHED_NET_INCREMENT:
        blockers.append("MATCHED_NET_INCREMENT_NOT_POSITIVE")
    return tuple(blockers)


def mark_to_market_blockers(row: Mapping[str, Any]) -> tuple[str, ...]:
    """Return blockers for bounded-terminal-exposure MTM admission.

    Passing this predicate permits only train-only finalist consideration for a
    separately authorized report-only OOS run.  It does not prove strict
    executability or authorize promotion.
    """
    blockers: list[str] = []
    if str(row.get("pair_mark_to_market_status") or "") != (
        "PAIR_MARK_TO_MARKET_COMPLETE"
    ):
        blockers.append("PAIR_MARK_TO_MARKET_NOT_COMPLETE")
    primary = _finite_float(row.get("primary_mark_to_market_net_reward"))
    if primary is None or primary <= MINIMUM_PRIMARY_NET_REWARD:
        blockers.append("PRIMARY_ABSOLUTE_NET_REWARD_NOT_POSITIVE")
    increment = _finite_float(row.get("mark_to_market_net_increment"))
    if increment is None or increment <= MINIMUM_MATCHED_NET_INCREMENT:
        blockers.append("MATCHED_NET_INCREMENT_NOT_POSITIVE")
    for role in ("primary", "control"):
        weight = _finite_float(row.get(f"{role}_ending_holdings_weight"))
        if weight is None:
            blockers.append(f"{role.upper()}_TERMINAL_HOLDINGS_WEIGHT_MISSING")
        elif weight > MAXIMUM_TERMINAL_HOLDINGS_WEIGHT:
            blockers.append(f"{role.upper()}_TERMINAL_HOLDINGS_WEIGHT_EXCEEDED")
    return tuple(blockers)


def strict_replay_eligible(row: Mapping[str, Any]) -> bool:
    return not strict_replay_blockers(row)


def mark_to_market_eligible(row: Mapping[str, Any]) -> bool:
    return not mark_to_market_blockers(row)
