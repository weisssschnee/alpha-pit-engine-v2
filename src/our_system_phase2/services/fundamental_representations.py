"""Semantically qualified fundamental roots and their PIT materializer.

Only exact, source-schema-backed fields are asserted.  Everything else remains
present in the source glossary but fails closed with
``SOURCE_UNIT_GLOSSARY_NOT_ASSERTED``.  No performance metric participates in
the qualification or representation definitions.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd

from our_system_phase2.services.pit_fundamental_fabric import (
    FundamentalFieldRequest,
    PITFundamentalFabricAdapter,
)
from our_system_phase2.services.unified_capability_registry import representation_id


FUNDAMENTAL_SOURCE_RELEASE = "cn_fundamental_akshare_fullA_partitioned_pit_v1_20260603"

PROVIDER_BY_TABLE = {
    "balance_sheet_report_em": "AKSHARE_EASTMONEY",
    "profit_sheet_report_em": "AKSHARE_EASTMONEY",
    "cash_flow_sheet_report_em": "AKSHARE_EASTMONEY",
    "zygc_em": "AKSHARE_EASTMONEY",
    "main_stock_holder_sina": "AKSHARE_SINA",
}


@dataclass(frozen=True, slots=True)
class UnitAssertion:
    value_kind: str
    flow_or_stock: str
    period_basis: str
    consolidation_scope: str
    sign_semantics: str
    normalizer_candidates: tuple[str, ...]
    applicable_company_types: tuple[str, ...]
    source_reported_change: bool
    unit_certainty: str = "ASSERTED_FROM_EXACT_SOURCE_FIELD_SEMANTICS"
    semantic_certainty: str = "ASSERTED_FROM_EXACT_SOURCE_FIELD_SEMANTICS"


_ALL_COMPANY_TYPES = ("banking", "insurance", "broker", "general_industrial")
_GENERAL_COMPANY_TYPES = ("general_industrial",)


def _amount(*, stock: bool, normalizers: Sequence[str], all_types: bool = True) -> UnitAssertion:
    return UnitAssertion(
        value_kind="currency",
        flow_or_stock="stock" if stock else "flow",
        period_basis="point_in_time" if stock else "YTD",
        consolidation_scope="source_reported_consolidated_or_unknown",
        sign_semantics="source_signed_numeric_no_abs_rewrite",
        normalizer_candidates=tuple(normalizers),
        applicable_company_types=_ALL_COMPANY_TYPES if all_types else _GENERAL_COMPANY_TYPES,
        source_reported_change=False,
    )


QUALIFIED_BASE_FIELDS: dict[tuple[str, str], tuple[UnitAssertion, str]] = {
    ("balance_sheet_report_em", "TOTAL_ASSETS"): (_amount(stock=True, normalizers=("market_cap",)), "size"),
    ("balance_sheet_report_em", "TOTAL_LIABILITIES"): (_amount(stock=True, normalizers=("TOTAL_ASSETS",)), "leverage"),
    ("balance_sheet_report_em", "TOTAL_EQUITY"): (_amount(stock=True, normalizers=("TOTAL_ASSETS",)), "leverage"),
    ("balance_sheet_report_em", "TOTAL_CURRENT_ASSETS"): (_amount(stock=True, normalizers=("TOTAL_CURRENT_LIAB", "TOTAL_ASSETS"), all_types=False), "liquidity_solvency"),
    ("balance_sheet_report_em", "TOTAL_CURRENT_LIAB"): (_amount(stock=True, normalizers=("TOTAL_CURRENT_ASSETS", "TOTAL_ASSETS"), all_types=False), "liquidity_solvency"),
    ("balance_sheet_report_em", "MONETARYFUNDS"): (_amount(stock=True, normalizers=("TOTAL_ASSETS", "TOTAL_CURRENT_LIAB")), "liquidity_solvency"),
    ("balance_sheet_report_em", "ACCOUNTS_RECE"): (_amount(stock=True, normalizers=("TOTAL_ASSETS", "OPERATE_INCOME"), all_types=False), "working_capital"),
    ("balance_sheet_report_em", "INVENTORY"): (_amount(stock=True, normalizers=("TOTAL_ASSETS", "OPERATE_COST"), all_types=False), "working_capital"),
    ("balance_sheet_report_em", "ACCOUNTS_PAYABLE"): (_amount(stock=True, normalizers=("TOTAL_ASSETS", "OPERATE_COST"), all_types=False), "working_capital"),
    ("balance_sheet_report_em", "FIXED_ASSET"): (_amount(stock=True, normalizers=("TOTAL_ASSETS",)), "capital_investment"),
    ("profit_sheet_report_em", "OPERATE_INCOME"): (_amount(stock=False, normalizers=("TOTAL_ASSETS",)), "profitability"),
    ("profit_sheet_report_em", "OPERATE_COST"): (_amount(stock=False, normalizers=("OPERATE_INCOME",)), "profitability"),
    ("profit_sheet_report_em", "OPERATE_PROFIT"): (_amount(stock=False, normalizers=("OPERATE_INCOME", "TOTAL_ASSETS")), "profitability"),
    ("profit_sheet_report_em", "PARENT_NETPROFIT"): (_amount(stock=False, normalizers=("OPERATE_INCOME", "TOTAL_ASSETS", "TOTAL_EQUITY")), "profitability"),
    ("profit_sheet_report_em", "DEDUCT_PARENT_NETPROFIT"): (_amount(stock=False, normalizers=("OPERATE_INCOME", "TOTAL_ASSETS")), "profitability"),
    ("cash_flow_sheet_report_em", "NETCASH_OPERATE"): (_amount(stock=False, normalizers=("OPERATE_INCOME", "PARENT_NETPROFIT", "TOTAL_ASSETS")), "cashflow_quality"),
    ("cash_flow_sheet_report_em", "NETCASH_INVEST"): (_amount(stock=False, normalizers=("TOTAL_ASSETS",)), "capital_investment"),
    ("cash_flow_sheet_report_em", "NETCASH_FINANCE"): (_amount(stock=False, normalizers=("TOTAL_ASSETS",)), "capital_investment"),
    ("cash_flow_sheet_report_em", "CONSTRUCT_LONG_ASSET"): (_amount(stock=False, normalizers=("TOTAL_ASSETS", "NETCASH_OPERATE"), all_types=False), "capital_investment"),
}


HOLDER_ASSERTIONS: dict[str, UnitAssertion] = {
    "股东总数": UnitAssertion("count", "stock", "point_in_time", "not_applicable", "non_negative_count", ("log1p",), _ALL_COMPANY_TYPES, False),
    "平均持股数": UnitAssertion("shares", "stock", "point_in_time", "not_applicable", "non_negative_shares", ("shares_outstanding", "log1p"), _ALL_COMPANY_TYPES, False),
    "持股数量": UnitAssertion("shares", "stock", "point_in_time", "not_applicable", "non_negative_shares", ("shares_outstanding",), _ALL_COMPANY_TYPES, False),
    "持股比例": UnitAssertion("ratio", "stock", "point_in_time", "not_applicable", "source_native_non_negative_rate", (), _ALL_COMPANY_TYPES, False),
}


def qualify_source_field(row: Mapping[str, Any]) -> dict[str, Any]:
    """Add a conservative unit/applicability assertion to one source row."""

    table = str(row["source_table"])
    field = str(row["source_field"])
    output = dict(row)
    assertion: UnitAssertion | None = None
    semantic_family = ""
    base_field = field[:-4] if field.endswith("_YOY") else field
    base = QUALIFIED_BASE_FIELDS.get((table, base_field))
    if base:
        assertion, semantic_family = base
        if field.endswith("_YOY"):
            assertion = UnitAssertion(
                value_kind="rate",
                flow_or_stock=assertion.flow_or_stock,
                period_basis=assertion.period_basis,
                consolidation_scope=assertion.consolidation_scope,
                sign_semantics="source_native_signed_yoy_rate_no_scale_conversion",
                normalizer_candidates=(),
                applicable_company_types=assertion.applicable_company_types,
                source_reported_change=True,
                unit_certainty="ASSERTED_RATE_KIND_SOURCE_NATIVE_SCALE",
                semantic_certainty=assertion.semantic_certainty,
            )
    elif table == "main_stock_holder_sina" and field in HOLDER_ASSERTIONS:
        assertion = HOLDER_ASSERTIONS[field]
        semantic_family = "shareholder_structure"

    metadata_blocked = str(row.get("semantic_role", row.get("semantic_roles", ""))) == "METADATA_BLOCKED"
    pit_unresolved = str(row.get("pit_status", "")) == "PIT_CONTRACT_UNRESOLVED"
    if assertion is None:
        output.update(
            {
                "value_kind": "metadata" if metadata_blocked else "unknown",
                "flow_or_stock": "not_applicable" if metadata_blocked else "unknown",
                "period_basis": "not_applicable" if metadata_blocked else "unknown",
                "consolidation_scope": "not_applicable" if metadata_blocked else "unknown",
                "sign_semantics": "not_applicable" if metadata_blocked else "unknown",
                "normalizer_candidates": "",
                "applicable_company_types": "",
                "source_reported_change": False,
                "coverage": int(row.get("development_safe_non_null") or 0),
                "unit_certainty": "NOT_APPLICABLE_METADATA" if metadata_blocked else "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED",
                "semantic_certainty": "METADATA_EXACT" if metadata_blocked else "SOURCE_SEMANTICS_NOT_ASSERTED",
                "semantic_family": "metadata" if metadata_blocked else "unqualified",
                "search_eligible": False,
                "qualification_blocker": (
                    "PIT_CONTRACT_UNRESOLVED" if pit_unresolved else
                    "METADATA_BLOCKED" if metadata_blocked else
                    "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
                ),
            }
        )
        return output

    output.update(
        {
            "value_kind": assertion.value_kind,
            "flow_or_stock": assertion.flow_or_stock,
            "period_basis": assertion.period_basis,
            "consolidation_scope": assertion.consolidation_scope,
            "sign_semantics": assertion.sign_semantics,
            "normalizer_candidates": "|".join(assertion.normalizer_candidates),
            "applicable_company_types": "|".join(assertion.applicable_company_types),
            "source_reported_change": assertion.source_reported_change,
            "coverage": int(row.get("development_safe_non_null") or 0),
            "unit_certainty": assertion.unit_certainty,
            "semantic_certainty": assertion.semantic_certainty,
            "semantic_family": semantic_family,
            "search_eligible": not pit_unresolved,
            "qualification_blocker": "" if not pit_unresolved else "PIT_CONTRACT_UNRESOLVED",
        }
    )
    return output


def qualify_source_universe(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [qualify_source_field(row) for row in rows]


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def canonical_representation_specs(
    source_ids: Mapping[tuple[str, str], str],
) -> list[dict[str, Any]]:
    """Return non-performance-selected fundamental representations."""

    rows: list[dict[str, Any]] = []

    def add(
        *,
        field_id: str,
        family: str,
        representation_type: str,
        sources: Sequence[tuple[str, str]],
        route_id: str,
        operation: str,
        parameters: Mapping[str, Any] | None = None,
        field_role: str = "primary",
        support_unit: str = "stock-session cross-section",
        search_eligible: bool = True,
        blocked_reason: str = "",
    ) -> None:
        ids = [source_ids[key] for key in sources]
        params = dict(parameters or {})
        rows.append(
            {
                "field_id": field_id,
                "representation_id": representation_id(
                    representation_type=representation_type,
                    source_field_ids=ids,
                    parameters={"operation": operation, **params},
                ),
                "representation_type": representation_type,
                "semantic_family": family,
                "source_fields": [
                    {"source_table": table, "source_field": field, "source_field_id": source_ids[(table, field)]}
                    for table, field in sources
                ],
                "source_field_ids": ids,
                "route_id": route_id,
                "operation": operation,
                "parameters": params,
                "entity_scope": "STOCK",
                "temporal_semantics": (
                    "DISCLOSURE_PULSE" if route_id == "DISCLOSURE_EVENT" else
                    "SLOW_CHANGE" if route_id == "SLOW_TEMPORAL_CHANGE" else
                    "ASOF_LEVEL"
                ),
                "observable_clock": "next_session_after_source_disclosure",
                "maturity_rule": "max_dependency_observable_time",
                "pit_status": "PIT_SAFE_CURRENT_SNAPSHOT_ONLY",
                "support_unit": support_unit,
                "field_role": field_role,
                "unit_status": "SOURCE_UNIT_GLOSSARY_ASSERTED",
                "search_eligible": bool(search_eligible),
                "blocked_reason": blocked_reason,
                "matched_control_required": route_id == "DISCLOSURE_EVENT",
            }
        )

    base_items = [
        (table, field, assertion, family)
        for (table, field), (assertion, family) in QUALIFIED_BASE_FIELDS.items()
    ]
    for table, field, assertion, family in base_items:
        # Currency levels are not raw roots.  Their changes are scale-aware
        # within company and their source-reported YoY values have a separate
        # identity from internally derived changes.
        for transform in ("delta", "yoy", "slope", "persistence", "acceleration"):
            add(
                field_id=f"fund_{_slug(table[:2])}_{_slug(field)}_{transform}",
                family="growth" if transform == "yoy" else family,
                representation_type=f"internally_derived_{transform}",
                sources=[(table, field)],
                route_id="SLOW_TEMPORAL_CHANGE",
                operation="disclosed_change",
                parameters={"transform": transform, "period_basis": assertion.period_basis},
            )
        yoy_key = (table, field + "_YOY")
        if yoy_key in source_ids:
            add(
                field_id=f"fund_{_slug(table[:2])}_{_slug(field)}_source_yoy",
                family="growth",
                representation_type="source_reported_yoy",
                sources=[yoy_key],
                route_id="SLOW_TEMPORAL_CHANGE",
                operation="source_level",
                parameters={"scale": "source_native_rate_no_conversion"},
            )

    bs = "balance_sheet_report_em"
    ps = "profit_sheet_report_em"
    cf = "cash_flow_sheet_report_em"
    ratios = (
        ("fund_leverage_liabilities_assets", "leverage", (bs, "TOTAL_LIABILITIES"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_leverage_equity_assets", "leverage", (bs, "TOTAL_EQUITY"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_liquidity_current_ratio", "liquidity_solvency", (bs, "TOTAL_CURRENT_ASSETS"), (bs, "TOTAL_CURRENT_LIAB"), "ratio"),
        ("fund_liquidity_cash_current_liab", "liquidity_solvency", (bs, "MONETARYFUNDS"), (bs, "TOTAL_CURRENT_LIAB"), "ratio"),
        ("fund_working_capital_receivables_assets", "working_capital", (bs, "ACCOUNTS_RECE"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_working_capital_inventory_assets", "working_capital", (bs, "INVENTORY"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_working_capital_payables_assets", "working_capital", (bs, "ACCOUNTS_PAYABLE"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_profit_parent_margin", "profitability", (ps, "PARENT_NETPROFIT"), (ps, "OPERATE_INCOME"), "ratio"),
        ("fund_profit_operating_margin", "profitability", (ps, "OPERATE_PROFIT"), (ps, "OPERATE_INCOME"), "ratio"),
        ("fund_profit_gross_margin", "profitability", (ps, "OPERATE_INCOME"), (ps, "OPERATE_COST"), "one_minus_ratio"),
        ("fund_cashflow_cfo_profit", "cashflow_quality", (cf, "NETCASH_OPERATE"), (ps, "PARENT_NETPROFIT"), "ratio"),
        ("fund_cashflow_cfo_revenue", "cashflow_quality", (cf, "NETCASH_OPERATE"), (ps, "OPERATE_INCOME"), "ratio"),
        ("fund_accruals_profit_minus_cfo_assets", "accruals", (ps, "PARENT_NETPROFIT"), (cf, "NETCASH_OPERATE"), "difference_over_assets"),
        ("fund_capex_construct_assets", "capital_investment", (cf, "CONSTRUCT_LONG_ASSET"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_efficiency_revenue_assets", "operating_efficiency", (ps, "OPERATE_INCOME"), (bs, "TOTAL_ASSETS"), "ratio"),
        ("fund_efficiency_fixed_asset_intensity", "operating_efficiency", (bs, "FIXED_ASSET"), (bs, "TOTAL_ASSETS"), "ratio"),
    )
    for field_id, family, left, right, operation in ratios:
        sources = [left, right]
        if operation == "difference_over_assets":
            sources.append((bs, "TOTAL_ASSETS"))
        add(
            field_id=field_id,
            family=family,
            representation_type="scale_normalized_ratio",
            sources=sources,
            route_id="SLOW_CROSS_SECTIONAL_LEVEL",
            operation=operation,
        )

    holder = "main_stock_holder_sina"
    holder_specs = (
        ("fund_holder_count_log", "股东总数", "latest", "log1p"),
        ("fund_holder_average_shares_log", "平均持股数", "latest", "log1p"),
        ("fund_holder_top10_share_ratio", "持股比例", "top10_sum", "source_level"),
        ("fund_holder_top10_shares_log", "持股数量", "top10_sum", "log1p"),
    )
    for field_id, field, aggregation, operation in holder_specs:
        add(
            field_id=field_id,
            family="shareholder_structure",
            representation_type="aggregated_holder_level",
            sources=[(holder, field)],
            route_id="SLOW_CROSS_SECTIONAL_LEVEL",
            operation=operation,
            parameters={"aggregation": aggregation},
        )
        add(
            field_id=field_id + "_change",
            family="shareholder_structure",
            representation_type="aggregated_holder_change",
            sources=[(holder, field)],
            route_id="SLOW_TEMPORAL_CHANGE",
            operation="session_change",
            parameters={"aggregation": aggregation},
        )

    disclosure_sources = (
        (bs, "TOTAL_ASSETS"),
        (ps, "OPERATE_INCOME"),
        (cf, "NETCASH_OPERATE"),
        (holder, "股东总数"),
    )
    for table, field in disclosure_sources:
        tag = {bs: "balance", ps: "profit", cf: "cashflow", holder: "holder"}[table]
        add(
            field_id=f"fund_disclosure_{tag}_pulse",
            family="disclosure_timing_staleness",
            representation_type="disclosure_pulse",
            sources=[(table, field)],
            route_id="DISCLOSURE_EVENT",
            operation="disclosure_pulse",
            field_role="condition-only",
            support_unit="disclosure episode",
        )
        add(
            field_id=f"fund_disclosure_{tag}_age_sessions",
            family="disclosure_timing_staleness",
            representation_type="staleness_age",
            sources=[(table, field)],
            route_id="SLOW_CROSS_SECTIONAL_LEVEL",
            operation="staleness_sessions",
            field_role="condition-only",
        )

    # The family is represented in the canonical layer but remains blocked;
    # this prevents absence from being misreported as a negative result.
    zygc_source = ("zygc_em", "主营收入")
    add(
        field_id="fund_business_composition_concentration_blocked",
        family="business_composition_concentration",
        representation_type="blocked_unresolved_business_composition",
        sources=[zygc_source],
        route_id="SLOW_CROSS_SECTIONAL_LEVEL",
        operation="blocked",
        search_eligible=False,
        blocked_reason="PIT_CONTRACT_UNRESOLVED_NO_CREDIBLE_DISCLOSURE_CLOCK",
    )
    rows[-1]["pit_status"] = "PIT_CONTRACT_UNRESOLVED"
    rows[-1]["unit_status"] = "SOURCE_UNIT_GLOSSARY_NOT_ASSERTED"
    return rows


class CanonicalFundamentalMaterializer:
    """Materialize one registered representation on stock-session coordinates."""

    def __init__(self, adapter: PITFundamentalFabricAdapter) -> None:
        self.adapter = adapter

    @staticmethod
    def _request(source: Mapping[str, Any], *, route: str, transform: str = "latest") -> FundamentalFieldRequest:
        return FundamentalFieldRequest(
            source_table=str(source["source_table"]),
            source_field=str(source["source_field"]),
            route=route,
            transform=transform,
        )

    def _level(self, source: Mapping[str, Any], coordinates: pd.DataFrame, *, aggregation: str = "latest") -> pd.DataFrame:
        request = self._request(source, route="FUNDAMENTAL_LEVEL", transform=aggregation)
        return self.adapter.materialize_level(request, coordinates)

    def materialize(self, spec: Mapping[str, Any], coordinates: pd.DataFrame) -> pd.DataFrame:
        if not bool(spec.get("search_eligible", False)):
            raise PermissionError(str(spec.get("blocked_reason") or "representation is not search eligible"))
        sources = list(spec["source_fields"])
        operation = str(spec["operation"])
        output_name = str(spec["field_id"])
        params = dict(spec.get("parameters") or {})

        if operation == "source_level":
            frame = self._level(sources[0], coordinates)
            value = [name for name in frame if name.startswith("fund_")][-1]
            return frame.rename(columns={value: output_name})
        if operation == "disclosed_change":
            request = self._request(
                sources[0], route="FUNDAMENTAL_CHANGE", transform=str(params["transform"])
            )
            frame = self.adapter.materialize_change(request, coordinates)
            value = [name for name in frame if name.startswith("fund_")][-1]
            return frame.rename(columns={value: output_name})
        if operation == "disclosure_pulse":
            request = self._request(sources[0], route="DISCLOSURE_EVENT")
            episodes = self.adapter.disclosure_episodes(request, codes=coordinates["code"])
            output = coordinates.copy()
            output[output_name] = 0.0
            if episodes.empty:
                return output
            episode_keys = set(
                zip(
                    episodes["code"].astype(str),
                    pd.to_datetime(episodes["maturity_time"]).dt.normalize(),
                )
            )
            coord_keys = zip(
                output["code"].astype(str),
                pd.to_datetime(output["session_time"]).dt.normalize(),
            )
            output[output_name] = [1.0 if key in episode_keys else 0.0 for key in coord_keys]
            return output

        aggregation = str(params.get("aggregation", "latest"))
        frames: list[pd.DataFrame] = []
        for index, source in enumerate(sources):
            level = self._level(source, coordinates, aggregation=aggregation if index == 0 else "latest")
            value = [name for name in level if name.startswith("fund_")][-1]
            keep = ["code", "session_time", value]
            if "observable_time" in level:
                level = level.rename(columns={"observable_time": f"observable_time_{index}"})
                keep.append(f"observable_time_{index}")
            frames.append(level[keep].rename(columns={value: f"value_{index}"}))
        output = frames[0]
        for frame in frames[1:]:
            output = output.merge(frame, on=["code", "session_time"], how="inner", validate="one_to_one")
        values = [pd.to_numeric(output[f"value_{i}"], errors="coerce") for i in range(len(frames))]
        if operation == "ratio":
            result = values[0] / values[1].replace(0.0, np.nan)
        elif operation == "one_minus_ratio":
            result = (values[0] - values[1]) / values[0].replace(0.0, np.nan)
        elif operation == "difference_over_assets":
            result = (values[0] - values[1]) / values[2].replace(0.0, np.nan)
        elif operation == "log1p":
            result = np.log1p(values[0].where(values[0] >= 0.0))
        elif operation == "session_change":
            base = np.log1p(values[0].where(values[0] >= 0.0))
            result = base.groupby(output["code"], sort=False).diff()
        elif operation == "staleness_sessions":
            clocks = pd.to_datetime(output["observable_time_0"], errors="coerce")
            result = (pd.to_datetime(output["session_time"]) - clocks).dt.days.astype(float)
        else:
            raise ValueError(f"unsupported canonical representation operation: {operation}")
        output[output_name] = result
        clock_columns = [name for name in output if name.startswith("observable_time_")]
        if clock_columns:
            output["observable_time"] = output[clock_columns].max(axis=1)
        return output[[name for name in output if not name.startswith("value_") and not name.startswith("observable_time_")]]
