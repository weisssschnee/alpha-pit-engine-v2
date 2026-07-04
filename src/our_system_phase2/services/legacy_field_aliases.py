"""Compatibility rewrites for legacy true-1min field aliases.

These aliases are intentionally not materialized into parquet. Rewriting keeps
old diagnostic candidates runnable while preserving the current explicit
true-1min schema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


EPS = "0.000001"


@dataclass(frozen=True)
class LegacyAliasRewrite:
    alias: str
    replacement: str
    policy: str
    note: str


def _replace_field(expression: str, alias: str, replacement: str) -> tuple[str, bool]:
    pattern = re.compile(rf"\${re.escape(alias)}\b")
    return pattern.subn(replacement, expression, count=0)[0:2]


def _find_top_level_operator(expression: str, operators: tuple[str, ...]) -> tuple[int, str] | None:
    depth = 0
    quote: str | None = None
    for idx, char in enumerate(expression):
        if quote:
            if char == quote:
                quote = None
            continue
        if char in {"'", '"'}:
            quote = char
            continue
        if char == "(":
            depth += 1
            continue
        if char == ")":
            depth = max(0, depth - 1)
            continue
        if depth == 0 and char in operators:
            if char in "+-" and idx == 0:
                continue
            return idx, char
    return None


def _rewrite_top_level_infix(expression: str) -> tuple[str, list[LegacyAliasRewrite]]:
    text = str(expression or "").strip()
    rewrites: list[LegacyAliasRewrite] = []
    operator_names = {"+": "Add", "-": "Sub", "*": "Mul", "/": "Div"}
    # Low-precedence operators first, then multiplicative operators.
    for operators in (("+", "-"), ("*", "/")):
        found = _find_top_level_operator(text, operators)
        if found is None:
            continue
        idx, operator = found
        left, left_rewrites = _rewrite_top_level_infix(text[:idx])
        right, right_rewrites = _rewrite_top_level_infix(text[idx + 1 :])
        rewritten = f"{operator_names[operator]}({left},{right})"
        rewrites.extend(left_rewrites)
        rewrites.extend(right_rewrites)
        rewrites.append(
            LegacyAliasRewrite(
                alias=f"top_level_infix_{operator}",
                replacement=operator_names[operator],
                policy="legacy_expression_syntax_rewrite",
                note="legacy binary infix expression rewritten to function-call AST syntax",
            )
        )
        return rewritten, rewrites
    return text, rewrites


def rewrite_legacy_field_aliases(
    expression: str,
    *,
    m1_first_ret_replacement: str = "m1_first5_last_return_vs_open",
) -> tuple[str, list[LegacyAliasRewrite]]:
    """Rewrite known legacy aliases to explicit current-schema expressions.

    `range_location` has an unambiguous bar-location definition. `m1_first_ret`
    is inherently ambiguous, so it is rewritten to the configured explicit
    first-N field and tagged for downstream reporting.
    """

    out, rewrites = _rewrite_top_level_infix(str(expression or ""))

    range_expr = f"Div(Sub($close,$low),Add(Abs(Sub($high,$low)),{EPS}))"
    out, count = _replace_field(out, "range_location", range_expr)
    if count:
        rewrites.append(
            LegacyAliasRewrite(
                alias="range_location",
                replacement=range_expr,
                policy="safe_expression_rewrite",
                note="bar close location within high-low range",
            )
        )

    m1_replacement = str(m1_first_ret_replacement or "").strip().lstrip("$")
    if m1_replacement:
        out, count = _replace_field(out, "m1_first_ret", f"${m1_replacement}")
        if count:
            rewrites.append(
                LegacyAliasRewrite(
                    alias="m1_first_ret",
                    replacement=f"${m1_replacement}",
                    policy="legacy_diagnostic_explicit_firstn_mapping",
                    note="ambiguous legacy alias mapped to explicit configured firstN return field",
                )
            )

    return out, rewrites


def rewrite_summary(rewrites: list[LegacyAliasRewrite]) -> str:
    return "|".join(f"{item.alias}->{item.replacement}" for item in rewrites)
