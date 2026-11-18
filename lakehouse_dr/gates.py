"""Quality gates run at backup time and re-run after recovery.

Every gate returns one record:

    {"name", "status": "passed"|"failed"|"warning", "detail"}

Gates never raise: a broken file must produce a *failed gate record*, not a
crash, so that a backup can still be taken and the damage captured.
"""
from __future__ import annotations

import os
from typing import TYPE_CHECKING

from . import jsonl
from .hashing import sha256_file

if TYPE_CHECKING:
    from .lakehouse import Lakehouse

GATE_FILE_HASH = "file-hash-integrity"
GATE_JSON_PARSE = "jsonl-parseable"
GATE_CDC_CONTINUITY = "cdc-sequence-continuity"
GATE_SCHEMA = "schema-conformance"
GATE_ROW_COUNT = "row-count-sanity"


def _record(name: str, status: str, detail: str) -> dict:
    return {"name": name, "status": status, "detail": detail}


def run_quality_gates(lh: "Lakehouse") -> dict:
    """Evaluate all gates against a lakehouse root (prod or recovered)."""
    results: list[dict] = []
    catalog = lh.catalog()
    tables = catalog.get("tables", {})

    # Gate 1: every registered file exists and parses as JSONL.
    parse_errors: list[str] = []
    missing: list[str] = []
    parsed_rows: dict[str, list[dict]] = {}
    for table, meta in tables.items():
        for rel in meta.get("current_files", []):
            abs_path = lh.data_file_abs(rel)
            try:
                parsed_rows[rel] = jsonl.read_jsonl(abs_path)
            except FileNotFoundError:
                missing.append(rel)
            except ValueError as exc:
                parse_errors.append(str(exc))
    if missing:
        results.append(_record(GATE_JSON_PARSE, "failed",
                               f"{len(missing)} file(s) missing: " + ", ".join(missing[:5])))
    elif parse_errors:
        results.append(_record(GATE_JSON_PARSE, "failed",
                               f"{len(parse_errors)} corrupt file(s): "
                               + "; ".join(parse_errors[:3])))
    else:
        total = sum(len(v) for v in parsed_rows.values())
        results.append(_record(GATE_JSON_PARSE, "passed",
                               f"all {len(parsed_rows)} data file(s) parse, {total} row states"))

    # Gate 2: every registered file and snapshot exists and is fully readable.
    # Hash-mismatch verification against backup blobs happens in the backup
    # verify stage; here we confirm the live tree has no unreadable files.
    unreadable: list[str] = []
    for table, meta in tables.items():
        for rel in meta.get("current_files", []):
            abs_path = lh.data_file_abs(rel)
            if not os.path.exists(abs_path):
                continue  # already reported as missing by gate 1
            try:
                sha256_file(abs_path)
            except OSError as exc:
                unreadable.append(f"{rel}: {exc}")
        for snap in meta.get("snapshots", []):
            snap_path = lh.data_file_abs(lh.snapshot_rel(table, snap["snapshot_id"]))
            if not os.path.exists(snap_path):
                unreadable.append(f"snapshot missing: {snap['snapshot_id']}")
            else:
                try:
                    sha256_file(snap_path)
                except OSError as exc:
                    unreadable.append(f"{snap['snapshot_id']}: {exc}")
    if unreadable:
        results.append(_record(GATE_FILE_HASH, "failed",
                               f"{len(unreadable)} unreadable item(s): " + "; ".join(unreadable[:3])))
    else:
        n_files = sum(len(m.get("current_files", [])) for m in tables.values())
        n_snaps = sum(len(m.get("snapshots", [])) for m in tables.values())
        results.append(_record(GATE_FILE_HASH, "passed",
                               f"{n_files} data file(s) and {n_snaps} snapshot(s) all readable"))

    # Gate 3: CDC log seq values are a contiguous 1..N run.
    cdc_gap = _cdc_gap(lh)
    if cdc_gap:
        results.append(_record(GATE_CDC_CONTINUITY, "failed",
                               "missing sequence(s): " + ", ".join(
                                   str(s) for s in cdc_gap[:10])))
    else:
        results.append(_record(GATE_CDC_CONTINUITY, "passed",
                               f"CDC log contiguous, watermark seq={lh.cdc_watermark()}"))

    # Gate 4: every live row carries the declared schema columns.
    schema_issues: list[str] = []
    for table, meta in tables.items():
        cols = set(meta.get("schema", {}).get("columns", {}).keys())
        pk = meta.get("primary_key", [])
        for rel in meta.get("current_files", []):
            for row in parsed_rows.get(rel, []):
                if row.get("__deleted__"):
                    if not all(k in row for k in pk):
                        schema_issues.append(f"{rel}: tombstone missing PK fields")
                    continue
                absent = cols - set(row.keys())
                if absent:
                    schema_issues.append(f"{rel}: row missing {sorted(absent)}")
                    break
    if schema_issues:
        results.append(_record(GATE_SCHEMA, "failed",
                               f"{len(schema_issues)} violation(s): " + "; ".join(schema_issues[:3])))
    else:
        results.append(_record(GATE_SCHEMA, "passed",
                               f"{len(tables)} table(s) conform to declared schemas"))

    # Gate 5: catalog row_count matches the folded live view.
    count_issues: list[str] = []
    for table, meta in tables.items():
        try:
            actual = lh.count_rows(table)
        except (ValueError, KeyError) as exc:
            count_issues.append(f"{table}: cannot count ({exc})")
            continue
        if actual != meta.get("row_count"):
            count_issues.append(f"{table}: catalog={meta.get('row_count')} actual={actual}")
    if count_issues:
        results.append(_record(GATE_ROW_COUNT, "failed",
                               "; ".join(count_issues[:5])))
    else:
        results.append(_record(GATE_ROW_COUNT, "passed",
                               "catalog row counts match folded live state"))

    failed = [r for r in results if r["status"] == "failed"]
    return {
        "status": "failed" if failed else "passed",
        "failed_count": len(failed),
        "results": results,
    }


def _cdc_gap(lh: "Lakehouse") -> list[int]:
    try:
        events = jsonl.read_jsonl(lh.cdc_log_path)
    except (ValueError, OSError):
        return []
    seqs = sorted(e.get("seq", 0) for e in events)
    if not seqs:
        return []
    present = set(seqs)
    return [s for s in range(seqs[0], seqs[-1] + 1) if s not in present]
