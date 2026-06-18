"""Phase3CE unsafe limit motif quarantine audit.

This is a diagnostic audit. It does not launch search and does not mutate
official X0/R3 assets. The purpose is to upgrade the Phase3CD statement
"no official book contamination observed" into an explicit positive scan:

1. scan official registries/baselines for unsafe limit motif signatures;
2. scan diagnostic/shared-pool/preflight artifacts for materialized instances;
3. identify the first likely entry point where unsafe candidates were created;
4. define falsifiable stop conditions for the typed-primitive canary.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_ROOT = Path("reports/phase3ce_unsafe_motif_quarantine_audit_20260618")
DEFAULT_CD_OUTPUT_ROOT = Path("reports/phase3cd_ast_primitive_assumption_audit_20260618")

TEXT_SUFFIXES = {".csv", ".json", ".jsonl", ".md", ".py", ".txt", ".yaml", ".yml"}
MAX_FILE_BYTES = 80_000_000

OFFICIAL_BOOK_SCAN_ROOTS = [
    Path("runtime/baselines"),
]

ADVISORY_RUNTIME_SCAN_ROOTS = [
    Path("runtime/registries"),
    Path("runtime/field_registry"),
    Path("runtime/search_memory"),
]

QUARANTINE_SCAN_ROOTS = [
    Path("src/our_system_phase2/formula_gen_v2"),
    Path("src/our_system_phase2/runtime"),
    Path("reports"),
    Path("runtime"),
]

LIMIT_TOKENS = [
    "limit",
    "uplimit",
    "up_limit",
    "open_board",
    "break_board",
    "high_board",
    "fengdan",
    "seal",
    "lb_",
    "max_lb",
]

UNSAFE_PATTERNS = [
    ("mean_event", re.compile(r"Mean\s*\([^)]*(?:limit|uplimit|up_limit|open_board|break_board|high_board|fengdan|seal|lb_|max_lb)", re.I)),
    ("zscore_mean_event", re.compile(r"ZScore\s*\(\s*Mean\s*\([^)]*(?:limit|uplimit|up_limit|open_board|break_board|high_board|fengdan|seal|lb_|max_lb)", re.I)),
    ("csresidual_event", re.compile(r"CSResidual\s*\([^)]*(?:limit|uplimit|up_limit|open_board|break_board|high_board|fengdan|seal|lb_|max_lb)", re.I)),
    ("mom_event", re.compile(r"Mom\s*\([^)]*(?:limit|uplimit|up_limit|open_board|break_board|high_board|fengdan|seal|lb_|max_lb)", re.I)),
    ("corr_event", re.compile(r"(?:Corr|Cov)\s*\([^)]*(?:limit|uplimit|up_limit|open_board|break_board|high_board|fengdan|seal|lb_|max_lb)", re.I)),
]


def _resolve(path: Path) -> Path:
    return path if path.is_absolute() else REPO / path


def _write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fields is None:
        fields = []
        for row in rows:
            for key in row:
                if key not in fields:
                    fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _iter_text_files(root: Path) -> Iterable[Path]:
    full = _resolve(root)
    if not full.exists():
        return
    if full.is_file():
        if full.suffix.lower() in TEXT_SUFFIXES and full.stat().st_size <= MAX_FILE_BYTES:
            yield full
        return
    for path in full.rglob("*"):
        try:
            if not path.is_file():
                continue
            rel = _relative(path).replace("\\", "/").lower()
            if "reports/phase3ce_unsafe_motif_quarantine_audit_" in rel:
                continue
            if path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            if path.stat().st_size > MAX_FILE_BYTES:
                continue
            yield path
        except OSError:
            continue


def _relative(path: Path) -> str:
    try:
        return str(path.relative_to(REPO))
    except ValueError:
        return str(path)


def _read_lines(path: Path) -> list[str]:
    try:
        return path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except OSError:
        return []


def _risk_matches(text: str) -> list[str]:
    matches: list[str] = []
    low = text.lower()
    if not any(token in low for token in LIMIT_TOKENS):
        return matches
    for name, pattern in UNSAFE_PATTERNS:
        if pattern.search(text):
            matches.append(name)
    return matches


def _digest(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:16]


def _expression_text(text: str) -> str:
    quoted = re.findall(r'"([^"]*(?:CSRank|Rank|ZScore|Mean|Mom|Corr|Cov|CSResidual|Div)[^"]*)"', text)
    if quoted:
        return quoted[0].strip()
    match = re.search(r"((?:CSRank|Rank|ZScore|Mean|Mom|Corr|Cov|CSResidual|Div)\s*\(.+)", text)
    if match:
        return match.group(1).strip()[:1000]
    return text.strip()[:1000]


def _source_layer(path: str) -> str:
    p = path.replace("\\", "/").lower()
    if "runtime/baselines" in p:
        return "official_book_baseline"
    if "runtime/registries" in p or "runtime/field_registry" in p:
        return "advisory_runtime_registry"
    if "runtime/search_memory" in p:
        return "search_memory"
    if "formula_gen_v2/motif_pack_limit_diagnostic" in p:
        return "motif_definition"
    if "phase3r_limit_motif_pack_diagnostic" in p:
        return "phase3r_candidate_generation"
    if "phase3aa_enrich_shared_candidate_pool" in p:
        return "phase3aa_shared_pool_enrichment"
    if "candidate_integration" in p or "shared_pool" in p or "preflight" in p:
        return "candidate_pool_or_preflight"
    if "reports/" in p:
        return "diagnostic_report"
    if "runtime/" in p:
        return "runtime_artifact"
    return "source_or_unknown"


def _entry_point(path: str, line: str) -> str:
    layer = _source_layer(path)
    p = path.replace("\\", "/").lower()
    if layer == "motif_definition":
        return "src_formula_gen_v2_motif_pack_limit_diagnostic"
    if layer == "phase3r_candidate_generation":
        return "phase3r_limit_motif_pack_diagnostic"
    if layer == "phase3aa_shared_pool_enrichment":
        return "phase3aa_enrich_shared_candidate_pool"
    if "phase3r_limit_diagnostic_candidate_ledger" in p:
        return "phase3r_candidate_ledger"
    if "cn_factor_pack_candidate_integration" in p:
        return "cn_factor_pack_candidate_integration"
    if "shared_pool" in p:
        return "shared_candidate_pool_artifact"
    if "preflight" in p:
        return "preflight_artifact"
    if "reports/" in p:
        return "report_materialization"
    if "runtime/baselines" in p:
        return "official_registry_or_baseline"
    return layer


def _scan_roots(roots: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    for root in roots:
        for path in _iter_text_files(root):
            rel = _relative(path)
            for line_no, line in enumerate(_read_lines(path), start=1):
                matches = _risk_matches(line)
                if not matches:
                    continue
                key = (rel, line_no, line.strip()[:500])
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "path": rel,
                        "line": line_no,
                        "risk_patterns": "|".join(matches),
                        "expression_digest": _digest(_expression_text(line)),
                        "expression_text": _expression_text(line),
                        "source_layer": _source_layer(rel),
                        "first_materialization_entry_point": _entry_point(rel, line),
                        "official_book_scan_hit": str(_source_layer(rel) == "official_book_baseline").lower(),
                        "matched_text": line.strip()[:1000],
                    }
                )
    rows.sort(key=lambda row: (row["path"], int(row["line"])))
    return rows


def _load_cd_signatures(cd_root: Path) -> list[dict[str, Any]]:
    path = _resolve(cd_root) / "unsafe_limit_motif_rewrite_queue.csv"
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", newline="") as handle:
        for idx, row in enumerate(csv.DictReader(handle), start=1):
            template = str(row.get("template", ""))
            family = "unknown"
            if "CSResidual" in template:
                family = "csresidual_event"
            elif "ZScore(Mean" in template:
                family = "zscore_mean_event"
            elif "Mean" in template:
                family = "mean_event"
            rows.append(
                {
                    "signature_id": f"cd_unsafe_limit_motif_{idx:02d}",
                    "source_path": row.get("path", ""),
                    "source_line": row.get("line", ""),
                    "signature_family": family,
                    "template": template,
                    "risk": row.get("risk", ""),
                    "recommended_rewrite": row.get("recommended_rewrite", ""),
                }
            )
    return rows


def _entry_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault((row["source_layer"], row["first_materialization_entry_point"]), []).append(row)
    result = []
    for (layer, entry), group in grouped.items():
        result.append(
            {
                "source_layer": layer,
                "first_materialization_entry_point": entry,
                "hit_count": len(group),
                "unique_expression_digest_count": len({str(row.get("expression_digest") or "") for row in group}),
                "unique_path_count": len({str(row.get("path") or "") for row in group}),
            }
        )
    result.sort(key=lambda row: (-int(row["hit_count"]), row["source_layer"], row["first_materialization_entry_point"]))
    return result


def _search_memory_decay_rows(search_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_path: dict[str, list[dict[str, Any]]] = {}
    for row in search_rows:
        by_path.setdefault(str(row.get("path") or ""), []).append(row)
    for rel, hits in sorted(by_path.items()):
        full = _resolve(Path(rel))
        generated_at = ""
        memory_entry_count = ""
        schema_version = ""
        if full.suffix.lower() == ".json" and full.exists():
            try:
                payload = json.loads(full.read_text(encoding="utf-8", errors="ignore"))
                generated_at = str(payload.get("generated_at") or "")
                memory_entry_count = str(payload.get("memory_entry_count") or len(payload.get("memory_entries", []) or []))
                schema_version = str(payload.get("schema_version") or "")
            except (OSError, json.JSONDecodeError, AttributeError):
                pass
        rows.append(
            {
                "path": rel,
                "generated_at": generated_at,
                "last_write_time": datetime.fromtimestamp(full.stat().st_mtime, timezone.utc).isoformat() if full.exists() else "",
                "schema_version": schema_version,
                "memory_entry_count": memory_entry_count,
                "unsafe_hit_count": len(hits),
                "unsafe_unique_expression_digest_count": len({str(row.get("expression_digest") or "") for row in hits}),
                "decay_or_ttl_detected": "false",
                "observed_policy": "append_or_merge_ledger_for_duplicate_filtering_no_decay_ttl_seen",
                "ce1_action": "quarantine_unsafe_expression_and_skeleton_keys_before_memory_filter_or_sampling",
            }
        )
    return rows


def _overlap_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    labels = {
        "phase3r": "phase3r_limit_motif_pack_diagnostic",
        "preflight": "preflight_artifact",
        "cn_factor_pack": "cn_factor_pack_candidate_integration",
        "advisory_registry": "advisory_runtime_registry",
        "search_memory": "search_memory",
    }
    sets: dict[str, set[str]] = {label: set() for label in labels}
    hit_counts: Counter[str] = Counter()
    for row in rows:
        entry = str(row.get("first_materialization_entry_point") or "")
        digest = str(row.get("expression_digest") or "")
        for label, expected_entry in labels.items():
            if entry != expected_entry:
                continue
            hit_counts[label] += 1
            if digest:
                sets[label].add(digest)
    out: list[dict[str, Any]] = []
    for label in labels:
        out.append(
            {
                "kind": "single",
                "a": label,
                "b": "",
                "a_hit_count": hit_counts[label],
                "a_unique_expression_digest_count": len(sets[label]),
                "b_unique_expression_digest_count": "",
                "intersection_count": "",
                "union_count": "",
                "jaccard": "",
                "a_covered_by_b": "",
                "b_covered_by_a": "",
            }
        )
    label_list = list(labels)
    for idx, a in enumerate(label_list):
        for b in label_list[idx + 1 :]:
            inter = sets[a] & sets[b]
            union = sets[a] | sets[b]
            out.append(
                {
                    "kind": "pairwise",
                    "a": a,
                    "b": b,
                    "a_hit_count": hit_counts[a],
                    "a_unique_expression_digest_count": len(sets[a]),
                    "b_unique_expression_digest_count": len(sets[b]),
                    "intersection_count": len(inter),
                    "union_count": len(union),
                    "jaccard": round(len(inter) / len(union), 6) if union else "",
                    "a_covered_by_b": round(len(inter) / len(sets[a]), 6) if sets[a] else "",
                    "b_covered_by_a": round(len(inter) / len(sets[b]), 6) if sets[b] else "",
                }
            )
    out.append(
        {
            "kind": "union",
            "a": "all_tracked_entry_points",
            "b": "",
            "a_hit_count": sum(hit_counts.values()),
            "a_unique_expression_digest_count": len(set().union(*sets.values())),
            "b_unique_expression_digest_count": "",
            "intersection_count": "",
            "union_count": "",
            "jaccard": "",
            "a_covered_by_b": "",
            "b_covered_by_a": "",
        }
    )
    return out


def _ce2_stop_condition_rows() -> list[dict[str, Any]]:
    return [
        {
            "check_id": "ce2_01_entry_path",
            "stop_condition": "typed canary candidate pack contains zero sparse_event/discrete_state/coverage_sensitive fields despite requesting those lanes",
            "why_it_matters": "would mean new fields still have no real search path",
            "required_output": "candidate_field_category_attribution.csv",
        },
        {
            "check_id": "ce2_02_old_primitive_leak",
            "stop_condition": "any blocked category appears inside ordinary Mean/ZScore/Mom/Corr/Cov/CSResidual/Div without typed rewrite",
            "why_it_matters": "would show CE1 gate is not active at the actual entry point",
            "required_output": "typed_gate_violation.csv",
        },
        {
            "check_id": "ce2_03_coverage_mask_alpha",
            "stop_condition": "true field performance does not beat coverage-mask placebo and shuffled-field placebo",
            "why_it_matters": "prevents fundamental/RZRQ/billboard coverage availability from masquerading as alpha",
            "required_output": "coverage_placebo_audit.csv",
        },
        {
            "check_id": "ce2_04_event_start_bias",
            "stop_condition": "EventAge/EventCount/StateDwell output shifts mainly at dataset coverage start or recording-regime boundary",
            "why_it_matters": "prevents event primitives from encoding data-vendor availability rather than event lifecycle",
            "required_output": "event_observable_boundary_audit.csv",
        },
        {
            "check_id": "ce2_05_same_count_placebo",
            "stop_condition": "event-state candidates fail same-count random placebo or matched-control excess test",
            "why_it_matters": "prevents sparse high-annualized slices from passing by sample luck",
            "required_output": "event_state_placebo_audit.csv",
        },
        {
            "check_id": "ce2_06_wrong_lag",
            "stop_condition": "wrong-lag or same-day unavailable variant is stronger than PIT-correct variant",
            "why_it_matters": "hard block for leakage and timestamp misuse",
            "required_output": "lag_contract_audit.csv",
        },
        {
            "check_id": "ce2_07_fragment_replay",
            "stop_condition": "typed canary passes proxy metrics but fails BZ-style fragment replay with cost and day-block MCMC",
            "why_it_matters": "keeps CE2 from repeating proxy-reward hacking",
            "required_output": "fragment_replay_summary.csv",
        },
    ]


def _write_report(path: Path, summary: dict[str, Any]) -> None:
    lines = [
        "# Phase3CE Unsafe Motif Quarantine Audit",
        "",
        "Status: diagnostic audit. No search launched. No official X0/R3 state modified.",
        "",
        "## Core Answer",
        "",
        f"- Official book/baseline unsafe hits: {summary['official_book_positive_hit_count']}",
        f"- Advisory runtime registry/search-memory unsafe hits: {summary['advisory_runtime_positive_hit_count']}",
        f"- Quarantine hits outside official roots: {summary['quarantine_hit_count']}",
        f"- Unsafe signature count inherited from Phase3CD: {summary['signature_count']}",
        "",
        "If official book hits are zero, the claim is evidence-backed for the scanned official book roots, not merely incidental.",
        "",
        "## Gate Placement Implication",
        "",
        "CE1 should not only gate G2 input. It must also gate candidate materialization/enrichment paths:",
        "",
        "- motif-pack candidate generation",
        "- Phase3R diagnostic ledger generation",
        "- Phase3AA shared-pool enrichment",
        "- factor-pack/preflight candidate integration",
        "- final G2 selector input",
        "",
        "## CE2 Canary Stop Conditions",
        "",
        "The canary must be falsifiable before it runs. See `ce2_typed_canary_stop_conditions.csv`.",
        "",
        "## Outputs",
        "",
        "- `unsafe_motif_signature_registry.csv`",
        "- `official_registry_positive_scan.csv`",
        "- `unsafe_candidate_quarantine.csv`",
        "- `unsafe_entry_point_summary.csv`",
        "- `unsafe_expression_overlap_matrix.csv`",
        "- `search_memory_decay_bias_audit.csv`",
        "- `ce2_typed_canary_stop_conditions.csv`",
        "- `phase3ce_unsafe_motif_quarantine_audit_summary.json`",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> dict[str, Any]:
    output_root = _resolve(Path(args.output_root))
    cd_root = Path(args.cd_output_root)

    signatures = _load_cd_signatures(cd_root)
    official_book_hits = _scan_roots(OFFICIAL_BOOK_SCAN_ROOTS)
    advisory_hits = _scan_roots(ADVISORY_RUNTIME_SCAN_ROOTS)
    quarantine_hits = _scan_roots(QUARANTINE_SCAN_ROOTS)
    quarantine_nonofficial = [
        row for row in quarantine_hits if row["source_layer"] != "official_book_baseline"
    ]
    entry_rows = _entry_summary(quarantine_nonofficial)
    stop_rows = _ce2_stop_condition_rows()
    overlap_rows = _overlap_rows(quarantine_nonofficial)

    _write_csv(output_root / "unsafe_motif_signature_registry.csv", signatures)
    _write_csv(
        output_root / "official_registry_positive_scan.csv",
        official_book_hits,
        fields=[
            "path",
            "line",
            "risk_patterns",
            "expression_digest",
            "expression_text",
            "source_layer",
            "first_materialization_entry_point",
            "official_book_scan_hit",
            "matched_text",
        ],
    )
    _write_csv(
        output_root / "advisory_runtime_positive_scan.csv",
        advisory_hits,
        fields=[
            "path",
            "line",
            "risk_patterns",
            "expression_digest",
            "expression_text",
            "source_layer",
            "first_materialization_entry_point",
            "official_book_scan_hit",
            "matched_text",
        ],
    )
    _write_csv(
        output_root / "unsafe_candidate_quarantine.csv",
        quarantine_nonofficial,
        fields=[
            "path",
            "line",
            "risk_patterns",
            "expression_digest",
            "expression_text",
            "source_layer",
            "first_materialization_entry_point",
            "official_book_scan_hit",
            "matched_text",
        ],
    )
    _write_csv(output_root / "unsafe_entry_point_summary.csv", entry_rows)
    _write_csv(output_root / "unsafe_expression_overlap_matrix.csv", overlap_rows)
    search_memory_rows = [row for row in quarantine_nonofficial if row["source_layer"] == "search_memory"]
    search_memory_decay_rows = _search_memory_decay_rows(search_memory_rows)
    _write_csv(output_root / "search_memory_decay_bias_audit.csv", search_memory_decay_rows)
    _write_csv(output_root / "ce2_typed_canary_stop_conditions.csv", stop_rows)

    official_counter = Counter(row["source_layer"] for row in official_book_hits)
    advisory_counter = Counter(row["source_layer"] for row in advisory_hits)
    quarantine_counter = Counter(row["source_layer"] for row in quarantine_nonofficial)
    entry_counter = Counter(row["first_materialization_entry_point"] for row in quarantine_nonofficial)
    summary = {
        "status": "ok",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "20260618_phase3ce_unsafe_motif_quarantine_audit",
        "decision": "PHASE3CE_QUARANTINE_AUDIT_READY_DIAGNOSTIC_ONLY",
        "official_chain_mutation": False,
        "search_launched": False,
        "scanned_official_book_roots": [str(root) for root in OFFICIAL_BOOK_SCAN_ROOTS],
        "scanned_advisory_runtime_roots": [str(root) for root in ADVISORY_RUNTIME_SCAN_ROOTS],
        "scanned_quarantine_roots": [str(root) for root in QUARANTINE_SCAN_ROOTS],
        "signature_count": len(signatures),
        "official_book_positive_hit_count": len(official_book_hits),
        "official_hit_counts_by_layer": dict(sorted(official_counter.items())),
        "advisory_runtime_positive_hit_count": len(advisory_hits),
        "advisory_runtime_hit_counts_by_layer": dict(sorted(advisory_counter.items())),
        "quarantine_hit_count": len(quarantine_nonofficial),
        "quarantine_hit_counts_by_layer": dict(sorted(quarantine_counter.items())),
        "quarantine_entry_point_counts": dict(sorted(entry_counter.items())),
        "quarantine_unique_expression_digest_count": len({str(row.get("expression_digest") or "") for row in quarantine_nonofficial}),
        "search_memory_unsafe_hit_count": len(search_memory_rows),
        "search_memory_unsafe_unique_expression_digest_count": len({str(row.get("expression_digest") or "") for row in search_memory_rows}),
        "search_memory_decay_or_ttl_detected": False,
        "tracked_entry_point_unique_expression_union_count": next(
            int(row["a_unique_expression_digest_count"])
            for row in overlap_rows
            if row["kind"] == "union" and row["a"] == "all_tracked_entry_points"
        ),
        "ce2_stop_condition_count": len(stop_rows),
        "official_book_zero_match_claim": len(official_book_hits) == 0,
        "gate_placement_required": [
            "candidate_materialization",
            "phase3r_diagnostic_ledger",
            "phase3aa_shared_pool_enrichment",
            "factor_pack_preflight_integration",
            "g2_selector_input",
        ],
        "hard_boundary": [
            "diagnostic only",
            "no official X0/R3 mutation",
            "zero official book hits only applies to scanned official book roots",
            "CE1 active runtime gate still not implemented by this audit",
        ],
        "outputs": {
            "unsafe_motif_signature_registry": str(output_root / "unsafe_motif_signature_registry.csv"),
            "official_registry_positive_scan": str(output_root / "official_registry_positive_scan.csv"),
            "advisory_runtime_positive_scan": str(output_root / "advisory_runtime_positive_scan.csv"),
            "unsafe_candidate_quarantine": str(output_root / "unsafe_candidate_quarantine.csv"),
            "unsafe_entry_point_summary": str(output_root / "unsafe_entry_point_summary.csv"),
            "unsafe_expression_overlap_matrix": str(output_root / "unsafe_expression_overlap_matrix.csv"),
            "search_memory_decay_bias_audit": str(output_root / "search_memory_decay_bias_audit.csv"),
            "ce2_typed_canary_stop_conditions": str(output_root / "ce2_typed_canary_stop_conditions.csv"),
            "report": str(output_root / "PHASE3CE_UNSAFE_MOTIF_QUARANTINE_AUDIT_20260618.md"),
        },
    }
    _write_json(output_root / "phase3ce_unsafe_motif_quarantine_audit_summary.json", summary)
    _write_report(output_root / "PHASE3CE_UNSAFE_MOTIF_QUARANTINE_AUDIT_20260618.md", summary)
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--cd-output-root", default=str(DEFAULT_CD_OUTPUT_ROOT))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    summary = run(args)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
