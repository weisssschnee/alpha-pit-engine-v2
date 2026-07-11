"""Versioned, PIT-aware feature/state materialization for NEXTGEN-DARK.

The fabric is deliberately independent from reward and candidate ranking.  It
turns a declared field registry plus raw frames into deterministic, auditable
materializations and refuses blocked fields or observations that are not yet
observable at the row timestamp.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


FABRIC_VERSION = "nextgen_dark_feature_state_fabric_v1"


class FieldRole(str, Enum):
    PRIMARY = "primary"
    INTERACTION_ONLY = "interaction-only"
    CONDITION_ONLY = "condition-only"
    STATE_ONLY = "state-only"
    BENCHMARK_ONLY = "benchmark-only"
    BLOCKED = "blocked"


class ObservableClock(str, Enum):
    BAR_CLOSE = "bar_close"
    FIRST_N_END = "first_n_end"
    PREVIOUS_SESSION = "previous_session"
    SOURCE_EFFECTIVE_TIME = "source_effective_time"
    EVENT_TIME = "event_time"
    METADATA_ONLY = "metadata_only"


class MissingPolicy(str, Enum):
    PROPAGATE = "propagate"
    ZERO = "zero"
    FALSE = "false"
    FORWARD_FILL_AFTER_EFFECTIVE = "forward_fill_after_effective"
    BLOCK = "block"


@dataclass(frozen=True, slots=True)
class FieldSpec:
    name: str
    dtype: str
    family: str
    role: FieldRole
    observable_clock: ObservableClock
    maturity: int
    maturity_unit: str
    missing_policy: MissingPolicy
    source_fields: tuple[str, ...]
    transform: str
    source_lag: int = 0
    source_lag_unit: str = "bars"
    observable_time_field: str = ""
    cacheable: bool = True
    blocked_reason: str = ""
    source_session_field: str = ""

    def validate(self) -> None:
        if not self.name or not self.family or not self.transform:
            raise ValueError(f"incomplete field spec: {self.name!r}")
        if self.maturity < 0 or self.source_lag < 0:
            raise ValueError(f"negative maturity/source lag: {self.name}")
        if self.role is FieldRole.BLOCKED and not self.blocked_reason:
            raise ValueError(f"blocked field needs a reason: {self.name}")
        if self.missing_policy is MissingPolicy.BLOCK and self.role is not FieldRole.BLOCKED:
            raise ValueError(f"missing_policy=block requires blocked role: {self.name}")
        if self.observable_clock is ObservableClock.FIRST_N_END:
            if self.maturity <= 0 or self.maturity_unit != "minutes":
                raise ValueError(f"firstN field needs positive minute maturity: {self.name}")
        if self.observable_clock is ObservableClock.PREVIOUS_SESSION:
            if self.source_lag < 1 or self.source_lag_unit != "sessions":
                raise ValueError(f"previous-session field needs a session source lag: {self.name}")

    def canonical(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["role"] = self.role.value
        payload["observable_clock"] = self.observable_clock.value
        payload["missing_policy"] = self.missing_policy.value
        payload["source_fields"] = list(self.source_fields)
        if not self.source_session_field:
            payload.pop("source_session_field")
        return payload


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class FieldRegistry:
    def __init__(self, version: str, fields: Iterable[FieldSpec]) -> None:
        self.version = str(version)
        rows = list(fields)
        if not self.version or not rows:
            raise ValueError("field registry needs a version and at least one field")
        self._fields: dict[str, FieldSpec] = {}
        for spec in rows:
            spec.validate()
            if spec.name in self._fields:
                raise ValueError(f"duplicate field spec: {spec.name}")
            self._fields[spec.name] = spec

    @property
    def fields(self) -> tuple[FieldSpec, ...]:
        return tuple(self._fields[name] for name in sorted(self._fields))

    @property
    def registry_hash(self) -> str:
        return hashlib.sha256(_stable_json(self.to_dict(include_hash=False)).encode()).hexdigest()

    def get(self, name: str) -> FieldSpec:
        try:
            return self._fields[name]
        except KeyError as exc:
            raise KeyError(f"field is not registered: {name}") from exc

    def by_role(self, *roles: FieldRole) -> tuple[FieldSpec, ...]:
        allowed = set(roles)
        return tuple(spec for spec in self.fields if spec.role in allowed)

    def to_dict(self, *, include_hash: bool = True) -> dict[str, Any]:
        payload = {
            "fabric_version": FABRIC_VERSION,
            "registry_version": self.version,
            "field_count": len(self._fields),
            "fields": [spec.canonical() for spec in self.fields],
        }
        if include_hash:
            payload["registry_hash"] = self.registry_hash
        return payload

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    @classmethod
    def read(cls, path: Path) -> "FieldRegistry":
        payload = json.loads(path.read_text(encoding="utf-8"))
        fields = []
        for row in payload["fields"]:
            item = dict(row)
            item["role"] = FieldRole(item["role"])
            item["observable_clock"] = ObservableClock(item["observable_clock"])
            item["missing_policy"] = MissingPolicy(item["missing_policy"])
            item["source_fields"] = tuple(item["source_fields"])
            fields.append(FieldSpec(**item))
        registry = cls(payload["registry_version"], fields)
        if payload.get("registry_hash") and payload["registry_hash"] != registry.registry_hash:
            raise ValueError("field registry hash mismatch")
        return registry


RAW_PRIMARY = {
    "open", "high", "low", "close", "vol", "volume", "amount", "amount_yuan",
    "vwap", "pct_chg", "ret_1m", "intraday_ret_from_open",
}
BLOCKED_METADATA = {
    "code", "trade_time", "date", "exec_date", "signal_time",
    "dataset_route_id", "label_horizon",
}


def infer_field_spec(name: str) -> FieldSpec:
    lower = name.lower()
    if name in BLOCKED_METADATA:
        return FieldSpec(
            name, "string" if name in {"code", "dataset_route_id"} else "metadata",
            "metadata", FieldRole.BLOCKED, ObservableClock.METADATA_ONLY, 0, "rows",
            MissingPolicy.BLOCK, (name,), "identity",
            blocked_reason="identifier/label/clock metadata is not a searchable feature",
        )
    if name in RAW_PRIMARY:
        return FieldSpec(
            name, "float64", "raw_1min", FieldRole.PRIMARY, ObservableClock.BAR_CLOSE,
            0, "bars", MissingPolicy.PROPAGATE, (name,), "identity",
        )
    if lower.startswith("m1_first"):
        digits = "".join(ch for ch in lower.split("_")[1] if ch.isdigit())
        maturity = int(digits or 0)
        return FieldSpec(
            name, "float64", "firstN", FieldRole.PRIMARY, ObservableClock.FIRST_N_END,
            maturity, "minutes", MissingPolicy.PROPAGATE, (name,), "identity",
        )
    if lower.startswith("evt_"):
        return FieldSpec(
            name, "float64", "event_state", FieldRole.STATE_ONLY, ObservableClock.EVENT_TIME,
            0, "minutes", MissingPolicy.PROPAGATE, (name,), "identity",
            observable_time_field="signal_time",
        )
    if lower.startswith("ctx_"):
        if any(token in lower for token in ("is_st", "prev_is_limit", "sent_", "zls_")):
            role = FieldRole.CONDITION_ONLY if "is_st" in lower else FieldRole.STATE_ONLY
        else:
            role = FieldRole.INTERACTION_ONLY
        return FieldSpec(
            name, "float64", "lagged_daily_context", role, ObservableClock.PREVIOUS_SESSION,
            1, "sessions", MissingPolicy.PROPAGATE, (name,), "identity",
            source_lag=1, source_lag_unit="sessions", observable_time_field="signal_time",
        )
    return FieldSpec(
        name, "float64", "unclassified", FieldRole.BLOCKED, ObservableClock.METADATA_ONLY,
        0, "rows", MissingPolicy.BLOCK, (name,), "identity",
        blocked_reason="unclassified field requires explicit NEXTGEN registry review",
    )


def registry_from_schema_fields(names: Sequence[str], *, version: str) -> FieldRegistry:
    return FieldRegistry(version, [infer_field_spec(str(name)) for name in names])


def _frame_fingerprint(frame: pd.DataFrame, columns: Sequence[str], keys: Sequence[str]) -> str:
    selected = [column for column in [*keys, *columns] if column in frame.columns]
    canonical = frame[selected].sort_values(list(keys), kind="mergesort").reset_index(drop=True)
    hashed = pd.util.hash_pandas_object(canonical, index=False).to_numpy(dtype=np.uint64, copy=False)
    digest = hashlib.sha256()
    digest.update("|".join(selected).encode())
    digest.update(hashed.tobytes())
    return digest.hexdigest()


class DeterministicFeatureCache:
    def __init__(self) -> None:
        self._values: dict[str, pd.Series] = {}
        self._diagnostics: dict[str, dict[str, Any]] = {}

    def get(self, key: str) -> pd.Series | None:
        value = self._values.get(key)
        return value.copy() if value is not None else None

    def get_diagnostics(self, key: str) -> dict[str, Any] | None:
        value = self._diagnostics.get(key)
        return dict(value) if value is not None else None

    def put(self, key: str, value: pd.Series, diagnostics: Mapping[str, Any] | None = None) -> None:
        self._values[key] = value.copy()
        if diagnostics is not None:
            self._diagnostics[key] = dict(diagnostics)

    def __len__(self) -> int:
        return len(self._values)


Transform = Callable[[pd.DataFrame], pd.Series]


class FeatureStateFabric:
    def __init__(
        self,
        registry: FieldRegistry,
        *,
        transforms: Mapping[str, Transform] | None = None,
        cache: DeterministicFeatureCache | None = None,
        row_keys: Sequence[str] = ("code", "trade_time"),
    ) -> None:
        self.registry = registry
        self.transforms = dict(transforms or {})
        self.cache = cache or DeterministicFeatureCache()
        self.row_keys = tuple(row_keys)

    def _apply_missing_policy(self, frame: pd.DataFrame, spec: FieldSpec, value: pd.Series) -> pd.Series:
        if spec.missing_policy is MissingPolicy.ZERO:
            return value.fillna(0.0)
        if spec.missing_policy is MissingPolicy.FALSE:
            return value.fillna(False).astype(bool)
        if spec.missing_policy is MissingPolicy.FORWARD_FILL_AFTER_EFFECTIVE:
            return value.groupby(frame["code"], sort=False).ffill()
        return value

    def _apply_temporal_guards(
        self,
        frame: pd.DataFrame,
        spec: FieldSpec,
        value: pd.Series,
    ) -> tuple[pd.Series, dict[str, Any]]:
        diagnostics: dict[str, Any] = {
            "observable_time_violation_count": 0,
            "pre_maturity_value_count": 0,
            "source_lag_violation_count": 0,
            "source_lag_evidence": "NOT_APPLICABLE",
        }
        row_time = pd.to_datetime(frame["trade_time"], errors="coerce", format="mixed")
        if not spec.observable_time_field:
            guarded = value
        else:
            if spec.observable_time_field not in frame.columns:
                raise ValueError(f"{spec.name} needs observable-time field {spec.observable_time_field}")
            observed = pd.to_datetime(
                frame[spec.observable_time_field], errors="coerce", format="mixed"
            )
            allowed = observed.notna() & row_time.notna() & (observed <= row_time)
            diagnostics["observable_time_violation_count"] = int((value.notna() & ~allowed).sum())
            guarded = value.where(allowed)

        if spec.observable_clock is ObservableClock.FIRST_N_END:
            session = row_time.dt.normalize()
            market_open = session + pd.Timedelta(hours=9, minutes=30)
            maturity_time = market_open + pd.to_timedelta(spec.maturity - 1, unit="min")
            mature = row_time.notna() & row_time.ge(maturity_time)
            diagnostics["pre_maturity_value_count"] = int((guarded.notna() & ~mature).sum())
            guarded = guarded.where(mature)

        if spec.observable_clock is ObservableClock.PREVIOUS_SESSION:
            session = row_time.dt.normalize()
            variation = guarded.groupby([frame["code"], session], sort=False).nunique(dropna=True)
            if variation.gt(1).any():
                raise ValueError(f"lagged context {spec.name} changes within a session")
            if spec.source_session_field:
                if spec.source_session_field not in frame.columns:
                    raise ValueError(f"{spec.name} needs source-session field {spec.source_session_field}")
                source_session = pd.to_datetime(
                    frame[spec.source_session_field], errors="coerce", format="mixed"
                ).dt.normalize()
                lagged = source_session.notna() & session.notna() & source_session.lt(session)
                diagnostics["source_lag_violation_count"] = int((guarded.notna() & ~lagged).sum())
                diagnostics["source_lag_evidence"] = "SOURCE_SESSION_FIELD"
                guarded = guarded.where(lagged)
            else:
                raise ValueError(
                    f"lagged context {spec.name} requires source-session evidence; "
                    "prelagged values without their source clock are blocked"
                )
        return guarded, diagnostics

    def materialize(
        self,
        frame: pd.DataFrame,
        fields: Sequence[str],
        *,
        allow_blocked: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        missing_keys = [key for key in self.row_keys if key not in frame.columns]
        if missing_keys:
            raise ValueError(f"materialization missing row keys: {missing_keys}")
        canonical = frame.sort_values(list(self.row_keys), kind="mergesort").reset_index(drop=True).copy()
        if canonical.duplicated(list(self.row_keys)).any():
            raise ValueError("duplicate row keys are not allowed")
        output = canonical[list(self.row_keys)].copy()
        fingerprint_columns: list[str] = []
        for name in fields:
            spec = self.registry.get(name)
            fingerprint_columns.extend(spec.source_fields)
            if spec.observable_time_field:
                fingerprint_columns.append(spec.observable_time_field)
            if spec.source_session_field:
                fingerprint_columns.append(spec.source_session_field)
        input_fingerprint = _frame_fingerprint(
            canonical,
            sorted(set(fingerprint_columns)),
            self.row_keys,
        )
        cache_hits = 0
        temporal_guards: dict[str, dict[str, Any]] = {}
        for name in fields:
            spec = self.registry.get(name)
            if spec.role is FieldRole.BLOCKED and not allow_blocked:
                raise PermissionError(f"blocked field cannot materialize: {name}: {spec.blocked_reason}")
            cache_key = hashlib.sha256(
                f"{self.registry.registry_hash}|{name}|{input_fingerprint}".encode()
            ).hexdigest()
            value = self.cache.get(cache_key) if spec.cacheable else None
            if value is not None:
                cache_hits += 1
                diagnostics = self.cache.get_diagnostics(cache_key) or {}
            else:
                if spec.transform == "identity":
                    if name not in canonical.columns:
                        raise KeyError(f"input field missing: {name}")
                    value = canonical[name].copy()
                else:
                    transform = self.transforms.get(spec.transform)
                    if transform is None:
                        raise KeyError(f"unregistered transform: {spec.transform}")
                    value = transform(canonical)
                value = self._apply_missing_policy(canonical, spec, value)
                value, diagnostics = self._apply_temporal_guards(canonical, spec, value)
                if spec.cacheable:
                    self.cache.put(cache_key, value, diagnostics)
            temporal_guards[name] = diagnostics
            output[name] = value.to_numpy(copy=False)
        manifest = {
            "fabric_version": FABRIC_VERSION,
            "registry_version": self.registry.version,
            "registry_hash": self.registry.registry_hash,
            "field_count": len(fields),
            "fields": list(fields),
            "row_count": len(output),
            "row_keys": list(self.row_keys),
            "input_fingerprint": input_fingerprint,
            "output_fingerprint": _frame_fingerprint(output, list(fields), self.row_keys),
            "cache_hits": cache_hits,
            "temporal_guards": temporal_guards,
            "reward_or_performance_used": False,
        }
        return output, manifest

    def materialize_shards(
        self,
        shards: Sequence[pd.DataFrame],
        fields: Sequence[str],
        *,
        allow_blocked: bool = False,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        return self.materialize(
            pd.concat(list(shards), ignore_index=True),
            fields,
            allow_blocked=allow_blocked,
        )
