"""Value-domain analysis and exact normalization for the true1min DSL."""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from enum import Enum


class ExpressionParseError(ValueError):
    pass


class ValueDomain(str, Enum):
    UNKNOWN = "unknown"
    SIGNED_REAL = "signed_real"
    NON_NEGATIVE = "non_negative"
    STRICT_POSITIVE = "strict_positive"
    STRICT_POSITIVE_UNIT = "strict_positive_unit_interval"
    STRICT_NEGATIVE = "strict_negative"
    SIGN_UNIT = "negative_zero_positive_one"
    CONSTANT_ZERO = "constant_zero"
    CONSTANT_ONE = "constant_one"
    CONSTANT_ONE_VALID_MASK = "constant_one_on_valid_rows"


@dataclass(frozen=True, slots=True)
class ExpressionNode:
    token: str
    args: tuple["ExpressionNode", ...] = ()

    @property
    def is_call(self) -> bool:
        return bool(self.args)

    def render(self) -> str:
        if not self.args:
            return self.token
        return f"{self.token}({','.join(arg.render() for arg in self.args)})"


@dataclass(frozen=True, slots=True)
class SemanticIssue:
    code: str
    severity: str
    path: str
    detail: str


@dataclass(frozen=True, slots=True)
class ExpressionSemanticAnalysis:
    original_expression: str
    canonical_expression: str
    output_domain: ValueDomain
    issues: tuple[SemanticIssue, ...]

    @property
    def changed(self) -> bool:
        return re.sub(r"\s+", "", self.original_expression) != self.canonical_expression

    @property
    def hard_blocked(self) -> bool:
        return any(issue.severity == "block" for issue in self.issues)

    @property
    def issue_codes(self) -> tuple[str, ...]:
        return tuple(issue.code for issue in self.issues)

    @property
    def decision(self) -> str:
        if self.hard_blocked:
            return "REJECT_SEMANTIC_DEGENERACY"
        if self.changed:
            return "ALLOW_REWRITTEN"
        if self.issues:
            return "ALLOW_WITH_AUDIT"
        return "ALLOW"

    @property
    def semantic_key(self) -> str:
        digest = hashlib.sha1(self.canonical_expression.encode("utf-8")).hexdigest()[:16]
        return f"semantic-{digest}"

    def to_row(self) -> dict[str, str]:
        return {
            "semantic_gate_decision": self.decision,
            "semantic_canonical_expression": self.canonical_expression,
            "semantic_output_domain": self.output_domain.value,
            "semantic_issue_codes": "|".join(self.issue_codes),
            "semantic_key": self.semantic_key,
        }


_CANONICAL_OPERATORS = {
    name.lower(): name
    for name in (
        "Abs",
        "Acceleration",
        "Add",
        "Corr",
        "Cov",
        "CSRank",
        "CSResidual",
        "Delay",
        "Delta",
        "Div",
        "DrawdownPath",
        "Duration",
        "EventAge",
        "EventCount",
        "EventTransition",
        "EventWindow",
        "FirstHit",
        "Kurt",
        "Log",
        "LastHit",
        "MaskedCorr",
        "MaskedZScore",
        "Mean",
        "Med",
        "Mom",
        "Mul",
        "MultiScaleRelation",
        "Neg",
        "Rank",
        "RecoveryPath",
        "SafeCSResidual",
        "SafeDiv",
        "Sign",
        "SinceLastEvent",
        "Skew",
        "Slope",
        "StateAge",
        "StateDwell",
        "StateTransition",
        "Std",
        "Sub",
        "PathShape",
        "Persistence",
        "Positive",
        "TimeSince",
        "Transition",
        "ValidRatioGate",
        "WindowStateCount",
        "Wma",
        "ZScore",
    )
}


def _split_args(payload: str) -> list[str]:
    args: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(payload):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ExpressionParseError("unbalanced closing parenthesis")
        elif char == "," and depth == 0:
            args.append(payload[start:index].strip())
            start = index + 1
    if depth != 0:
        raise ExpressionParseError("unbalanced parenthesis")
    args.append(payload[start:].strip())
    if any(not item for item in args):
        raise ExpressionParseError("empty expression argument")
    return args


def parse_expression(expression: str) -> ExpressionNode:
    text = expression.strip()
    if not text:
        raise ExpressionParseError("empty expression")
    first_open = text.find("(")
    if first_open < 0:
        if ")" in text or "," in text:
            raise ExpressionParseError(f"invalid atom: {text}")
        return ExpressionNode(text)
    if not text.endswith(")"):
        raise ExpressionParseError("call does not end with a closing parenthesis")
    operator = text[:first_open].strip()
    if not operator or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", operator):
        raise ExpressionParseError(f"invalid operator: {operator}")
    canonical_operator = _CANONICAL_OPERATORS.get(operator.lower(), operator)
    payload = text[first_open + 1 : -1]
    return ExpressionNode(
        canonical_operator,
        tuple(parse_expression(item) for item in _split_args(payload)),
    )


def _numeric_value(node: ExpressionNode) -> float | None:
    if node.args:
        return None
    try:
        value = float(node.token)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _is_zero(node: ExpressionNode) -> bool:
    value = _numeric_value(node)
    return value is not None and value == 0.0


def _is_one(node: ExpressionNode) -> bool:
    value = _numeric_value(node)
    return value is not None and value == 1.0


def _epsilon_guard(node: ExpressionNode) -> float | None:
    if node.token.lower() != "add" or len(node.args) != 2:
        return None
    for abs_node, epsilon_node in ((node.args[0], node.args[1]), (node.args[1], node.args[0])):
        epsilon = _numeric_value(epsilon_node)
        if (
            abs_node.token.lower() == "abs"
            and len(abs_node.args) == 1
            and epsilon is not None
            and epsilon > 0.0
        ):
            return epsilon
    return None


def infer_value_domain(node: ExpressionNode) -> ValueDomain:
    numeric = _numeric_value(node)
    if numeric is not None:
        if numeric == 0.0:
            return ValueDomain.CONSTANT_ZERO
        if numeric == 1.0:
            return ValueDomain.CONSTANT_ONE
        if numeric > 0.0:
            return ValueDomain.STRICT_POSITIVE
        return ValueDomain.STRICT_NEGATIVE
    if not node.args:
        return ValueDomain.UNKNOWN

    operator = node.token.lower()
    child_domains = tuple(infer_value_domain(child) for child in node.args)
    non_negative = {
        ValueDomain.NON_NEGATIVE,
        ValueDomain.STRICT_POSITIVE,
        ValueDomain.STRICT_POSITIVE_UNIT,
        ValueDomain.CONSTANT_ZERO,
        ValueDomain.CONSTANT_ONE,
        ValueDomain.CONSTANT_ONE_VALID_MASK,
    }
    strict_positive = {
        ValueDomain.STRICT_POSITIVE,
        ValueDomain.STRICT_POSITIVE_UNIT,
        ValueDomain.CONSTANT_ONE,
        ValueDomain.CONSTANT_ONE_VALID_MASK,
    }

    if operator in {"csrank", "rank"}:
        return ValueDomain.STRICT_POSITIVE_UNIT
    if operator == "abs":
        return ValueDomain.NON_NEGATIVE
    if operator == "positive":
        return ValueDomain.NON_NEGATIVE
    if operator == "sign":
        return ValueDomain.CONSTANT_ONE_VALID_MASK if child_domains[0] in strict_positive else ValueDomain.SIGN_UNIT
    if operator in {"zscore", "maskedzscore", "maskedcorr", "csresidual", "safecsresidual", "corr", "cov"}:
        return ValueDomain.SIGNED_REAL
    if operator in {
        "eventage",
        "sincelastevent",
        "eventcount",
        "stateage",
        "statedwell",
        "windowstatecount",
        "duration",
        "stateage",
        "timesince",
        "firsthit",
        "lasthit",
        "persistence",
        "std",
    }:
        return ValueDomain.NON_NEGATIVE
    if operator == "validratiogate":
        return child_domains[0]
    if operator in {"mean", "wma", "med", "delay"}:
        return child_domains[0]
    if operator == "add" and all(domain in non_negative for domain in child_domains):
        if all(domain in strict_positive for domain in child_domains):
            return ValueDomain.STRICT_POSITIVE
        return ValueDomain.NON_NEGATIVE
    if operator == "neg":
        if child_domains[0] in strict_positive:
            return ValueDomain.STRICT_NEGATIVE
        if child_domains[0] == ValueDomain.STRICT_NEGATIVE:
            return ValueDomain.STRICT_POSITIVE
        return ValueDomain.SIGNED_REAL
    if operator == "mul" and all(
        domain in {*strict_positive, ValueDomain.STRICT_NEGATIVE}
        for domain in child_domains
    ):
        negative_count = sum(domain == ValueDomain.STRICT_NEGATIVE for domain in child_domains)
        return ValueDomain.STRICT_NEGATIVE if negative_count % 2 else ValueDomain.STRICT_POSITIVE
    if operator in {
        "sub", "delta", "mom", "log", "skew", "kurt", "slope",
        "acceleration", "pathshape", "drawdownpath", "recoverypath",
        "eventwindow", "multiscalerelation", "transition", "safediv",
    }:
        return ValueDomain.SIGNED_REAL
    return ValueDomain.UNKNOWN


def _issue(issues: list[SemanticIssue], code: str, severity: str, path: str, detail: str) -> None:
    issues.append(SemanticIssue(code=code, severity=severity, path=path, detail=detail))


def _simplify(
    node: ExpressionNode,
    *,
    path: str,
    issues: list[SemanticIssue],
) -> ExpressionNode:
    if not node.args:
        return node
    args = tuple(
        _simplify(child, path=f"{path}.{index}", issues=issues)
        for index, child in enumerate(node.args)
    )
    current = ExpressionNode(node.token, args)
    operator = current.token.lower()

    if operator == "abs" and len(args) == 1:
        child_domain = infer_value_domain(args[0])
        if child_domain in {
            ValueDomain.NON_NEGATIVE,
            ValueDomain.STRICT_POSITIVE,
            ValueDomain.STRICT_POSITIVE_UNIT,
            ValueDomain.CONSTANT_ZERO,
            ValueDomain.CONSTANT_ONE,
            ValueDomain.CONSTANT_ONE_VALID_MASK,
        }:
            _issue(
                issues,
                "REDUNDANT_ABS_OF_NONNEGATIVE",
                "rewrite",
                path,
                f"Abs is an identity on {child_domain.value}",
            )
            return args[0]

    if operator == "sign" and len(args) == 1:
        child_domain = infer_value_domain(args[0])
        if child_domain in {
            ValueDomain.STRICT_POSITIVE,
            ValueDomain.STRICT_POSITIVE_UNIT,
            ValueDomain.CONSTANT_ONE,
            ValueDomain.CONSTANT_ONE_VALID_MASK,
        }:
            _issue(
                issues,
                "DEGENERATE_SIGN_OF_POSITIVE_RANK",
                "block",
                path,
                "Sign is +1 on every finite row and only preserves availability",
            )
        elif child_domain == ValueDomain.STRICT_NEGATIVE:
            _issue(
                issues,
                "DEGENERATE_SIGN_OF_NEGATIVE_RANK",
                "block",
                path,
                "Sign is -1 on every finite row and only preserves availability",
            )

    if operator in {"csrank", "rank"} and len(args) == 1 and args[0].token.lower() in {"csrank", "rank"}:
        _issue(
            issues,
            "IDEMPOTENT_NESTED_RANK",
            "rewrite",
            path,
            "ranking an existing percentile rank does not change its ordering or values",
        )
        return args[0]

    if operator == "neg" and len(args) == 1 and args[0].token.lower() == "neg" and len(args[0].args) == 1:
        _issue(issues, "DOUBLE_NEGATION", "rewrite", path, "double negation is an identity")
        return args[0].args[0]
    if operator == "add" and len(args) == 2:
        if _is_zero(args[0]):
            return args[1]
        if _is_zero(args[1]):
            return args[0]
    if operator == "sub" and len(args) == 2 and _is_zero(args[1]):
        return args[0]
    if operator == "mul" and len(args) == 2:
        if _is_one(args[0]):
            return args[1]
        if _is_one(args[1]):
            return args[0]
    if operator == "div" and len(args) == 2:
        denominator = args[1]
        if _is_one(denominator):
            return args[0]
        denominator_domain = infer_value_domain(denominator)
        if denominator_domain == ValueDomain.STRICT_POSITIVE_UNIT:
            _issue(
                issues,
                "UNBOUNDED_RANK_DENOMINATOR",
                "block",
                f"{path}.1",
                "percentile-rank denominators can amplify the lowest rank by universe width",
            )
        epsilon = _epsilon_guard(denominator)
        if epsilon is not None:
            _issue(
                issues,
                "EPSILON_ONLY_DIV_GUARD",
                "audit",
                f"{path}.1",
                f"epsilon={epsilon:g} prevents zero division but does not bound ratio tails",
            )
        elif _numeric_value(denominator) is None:
            _issue(
                issues,
                "UNGUARDED_DIVISION",
                "block",
                f"{path}.1",
                "division requires an explicit positive denominator guard",
            )
    if operator == "safediv" and len(args) == 3:
        floor = _numeric_value(args[2])
        if floor is None or floor <= 0.0:
            _issue(
                issues,
                "INVALID_SAFEDIV_FLOOR",
                "block",
                f"{path}.2",
                "SafeDiv requires a finite positive denominator floor",
            )

    return current


def analyze_expression(expression: str) -> ExpressionSemanticAnalysis:
    original = expression.strip()
    try:
        parsed = parse_expression(original)
    except ExpressionParseError as exc:
        issue = SemanticIssue(
            code="SEMANTIC_PARSE_ERROR",
            severity="block",
            path="root",
            detail=str(exc),
        )
        return ExpressionSemanticAnalysis(
            original_expression=original,
            canonical_expression=re.sub(r"\s+", "", original),
            output_domain=ValueDomain.UNKNOWN,
            issues=(issue,),
        )
    issues: list[SemanticIssue] = []
    simplified = _simplify(parsed, path="root", issues=issues)
    return ExpressionSemanticAnalysis(
        original_expression=original,
        canonical_expression=simplified.render(),
        output_domain=infer_value_domain(simplified),
        issues=tuple(issues),
    )
