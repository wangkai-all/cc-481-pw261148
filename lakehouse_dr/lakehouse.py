"""A tiny local lakehouse simulation.

Layout under ``root``::

    .lakehouse-prod-marker
    catalog.json                      # table schemas, current files, snapshots
    cdc/cdc-log.jsonl                 # globally ordered change events
    tables/<name>/data/00001.jsonl    # append-only batch files (row states)
    tables/<name>/snapshots/*.json    # per-table metadata snapshots

Data files contain row states in JSON Lines form. Deletes are represented by
tombstone rows ``{"__deleted__": true, ...primary_key}``; the live view of a
table is obtained by folding files in order with last-writer-wins on the
primary key.
"""
from __future__ import annotations

import os

from . import jsonl
from .config import (
    CATALOG_NAME,
    CDC_DIR,
    DATA_DIR,
    PROD_MARKER,
    SNAPSHOT_DIR,
    TABLES_DIR,
)
from .hashing import sha256_file
from .state import atomic_write_json, ensure_dir, read_json, utcnow_iso

TOMBSTONE = "__deleted__"


class LakehouseError(RuntimeError):
    pass


class Lakehouse:
    def __init__(self, root: str):
        self.root = root
        self.catalog_path = os.path.join(root, CATALOG_NAME)
        self.cdc_log_path = os.path.join(root, CDC_DIR, "cdc-log.jsonl")

    # ------------------------------------------------------------------ init
    def init(self) -> None:
        ensure_dir(self.root)
        marker = os.path.join(self.root, PROD_MARKER)
        if not os.path.exists(marker):
            with open(marker, "w", encoding="utf-8") as fh:
                fh.write("production lakehouse\n")
        ensure_dir(os.path.join(self.root, CDC_DIR))
        ensure_dir(os.path.join(self.root, TABLES_DIR))
        if not os.path.exists(self.catalog_path):
            atomic_write_json(
                self.catalog_path,
                {"format": "mini-lakehouse/1", "tables": {}},
            )

    # --------------------------------------------------------------- catalog
    def catalog(self) -> dict:
        return read_json(self.catalog_path, default={"tables": {}})

    def _save_catalog(self, catalog: dict) -> None:
        atomic_write_json(self.catalog_path, catalog)

    def list_tables(self) -> list[str]:
        return sorted(self.catalog()["tables"].keys())

    def table_meta(self, table: str) -> dict:
        tables = self.catalog()["tables"]
        if table not in tables:
            raise LakehouseError(f"unknown table: {table}")
        return tables[table]

    def create_table(self, name: str, schema: dict, primary_key: list[str]) -> None:
        catalog = self.catalog()
        if name in catalog["tables"]:
            raise LakehouseError(f"table already exists: {name}")
        ensure_dir(os.path.join(self.root, TABLES_DIR, name, DATA_DIR))
        ensure_dir(os.path.join(self.root, TABLES_DIR, name, SNAPSHOT_DIR))
        now = utcnow_iso()
        catalog["tables"][name] = {
            "name": name,
            "schema": schema,
            "primary_key": list(primary_key),
            "current_files": [],
            "snapshot_counter": 0,
            "snapshots": [],
            "row_count": 0,
            "created_at": now,
            "updated_at": now,
        }
        self._save_catalog(catalog)

    def table_dir(self, table: str) -> str:
        return os.path.join(self.root, TABLES_DIR, table)

    def data_file_rel(self, table: str, seq_in_table: int) -> str:
        return os.path.join(TABLES_DIR, table, DATA_DIR, f"{seq_in_table:05d}.jsonl")

    def data_file_abs(self, rel_path: str) -> str:
        return os.path.join(self.root, rel_path)

    def snapshot_rel(self, table: str, snapshot_id: str) -> str:
        return os.path.join(TABLES_DIR, table, SNAPSHOT_DIR, f"{snapshot_id}.json")

    # -------------------------------------------------------------------- cdc
    def cdc_watermark(self) -> int:
        events = jsonl.read_jsonl(self.cdc_log_path)
        return events[-1]["seq"] if events else 0

    def _next_seq(self, count: int) -> int:
        """Return the first seq for a batch of ``count`` events."""
        return self.cdc_watermark() + 1

    def _append_cdc(self, events: list[dict]) -> None:
        jsonl.append_jsonl(self.cdc_log_path, events)

    # ------------------------------------------------------------- table view
    def _merged_rows(self, table: str) -> dict[tuple, dict]:
        meta = self.table_meta(table)
        merged: dict[tuple, dict] = {}
        for rel in meta["current_files"]:
            for row in jsonl.read_jsonl(self.data_file_abs(rel)):
                key = tuple(row[k] for k in meta["primary_key"])
                merged[key] = row
        return merged

    def get_rows(self, table: str) -> list[dict]:
        meta = self.table_meta(table)
        merged = self._merged_rows(table)
        return [
            {k: v for k, v in row.items() if k != TOMBSTONE}
            for row in merged.values()
            if not row.get(TOMBSTONE)
        ]

    def count_rows(self, table: str) -> int:
        return len(self.get_rows(table))

    def file_hashes(self, table: str) -> dict[str, str]:
        meta = self.table_meta(table)
        return {
            rel: sha256_file(self.data_file_abs(rel)) for rel in meta["current_files"]
        }

    # ---------------------------------------------------------------- writes
    def _commit_batch(
        self,
        table: str,
        batch_rows: list[dict],
        cdc_events: list[dict],
        seq_override: int | None = None,
    ) -> dict:
        """Write one data file + CDC events + a new table snapshot.

        Normally seq values are allocated from the global CDC watermark.
        During recovery replay the original seq values must be preserved, so
        callers may pass ``seq_override`` (the first seq to use).
        """
        catalog = self.catalog()
        meta = catalog["tables"][table]
        next_file_no = len(meta["current_files"]) + 1
        rel_path = self.data_file_rel(table, next_file_no)
        jsonl.write_jsonl(self.data_file_abs(rel_path), batch_rows)

        first_seq = (
            seq_override if seq_override is not None else self._next_seq(len(cdc_events))
        )
        for offset, event in enumerate(cdc_events):
            event["seq"] = first_seq + offset
            if "ts" not in event:
                event["ts"] = utcnow_iso()
        self._append_cdc(cdc_events)
        last_seq = first_seq + len(cdc_events) - 1

        meta["snapshot_counter"] += 1
        snapshot_id = f"snap-{table}-{meta['snapshot_counter']:05d}"
        meta["current_files"].append(rel_path)
        # recompute live row count from folded state. _merged_rows re-reads
        # the on-disk catalog, so persist the new file list first to include
        # the batch we just wrote.
        catalog["tables"][table] = meta
        self._save_catalog(catalog)
        live = [
            row
            for row in self._merged_rows(table).values()
            if not row.get(TOMBSTONE)
        ]
        meta["row_count"] = len(live)
        meta["updated_at"] = utcnow_iso()
        snapshot = {
            "snapshot_id": snapshot_id,
            "table": table,
            "files": list(meta["current_files"]),
            "row_count": len(live),
            "cdc_seq": last_seq,
            "created_at": meta["updated_at"],
        }
        meta["snapshots"].append(snapshot)
        atomic_write_json(self.data_file_abs(self.snapshot_rel(table, snapshot_id)), snapshot)
        self._save_catalog(catalog)
        return {
            "data_file": rel_path,
            "snapshot_id": snapshot_id,
            "cdc_first_seq": first_seq,
            "cdc_last_seq": last_seq,
            "events": len(cdc_events),
        }

    def ingest_rows(self, table: str, rows: list[dict]) -> dict:
        """Upsert rows on the table primary key, emitting insert/update CDC."""
        if not rows:
            raise LakehouseError("ingest called with no rows")
        meta = self.table_meta(table)
        existing = self._merged_rows(table)
        events: list[dict] = []
        for row in rows:
            key = tuple(row[k] for k in meta["primary_key"])
            old = existing.get(key)
            op = "insert" if old is None or old.get(TOMBSTONE) else "update"
            events.append(
                {
                    "table": table,
                    "op": op,
                    "key": dict(zip(meta["primary_key"], key)),
                    "before": None if op == "insert" else {
                        k: v for k, v in old.items() if k != TOMBSTONE
                    },
                    "after": dict(row),
                }
            )
        return self._commit_batch(table, [dict(r) for r in rows], events)

    def delete_rows(self, table: str, keys: list[dict]) -> dict:
        if not keys:
            raise LakehouseError("delete called with no keys")
        meta = self.table_meta(table)
        existing = self._merged_rows(table)
        events: list[dict] = []
        tombstones: list[dict] = []
        for key in keys:
            tup = tuple(key[k] for k in meta["primary_key"])
            old = existing.get(tup)
            if old is None or old.get(TOMBSTONE):
                continue  # nothing to delete
            events.append(
                {
                    "table": table,
                    "op": "delete",
                    "key": dict(key),
                    "before": {k: v for k, v in old.items() if k != TOMBSTONE},
                    "after": None,
                }
            )
            tomb = dict(key)
            tomb[TOMBSTONE] = True
            tombstones.append(tomb)
        if not events:
            raise LakehouseError("none of the supplied keys exist in the live table")
        return self._commit_batch(table, tombstones, events)

    # ------------------------------------------------------------- inspection
    def all_file_records(self) -> list[dict]:
        """Every data file of every table with hash + size, for backup scans."""
        records: list[dict] = []
        for table in self.list_tables():
            meta = self.table_meta(table)
            for rel in meta["current_files"]:
                abs_path = self.data_file_abs(rel)
                records.append(
                    {
                        "table": table,
                        "path": rel,
                        "sha256": sha256_file(abs_path),
                        "size": os.path.getsize(abs_path),
                    }
                )
        return records

    def all_snapshot_records(self) -> list[dict]:
        records: list[dict] = []
        for table in self.list_tables():
            meta = self.table_meta(table)
            for snap in meta["snapshots"]:
                rel = self.snapshot_rel(table, snap["snapshot_id"])
                records.append(
                    {
                        "table": table,
                        "path": rel,
                        "sha256": sha256_file(self.data_file_abs(rel)),
                        "size": os.path.getsize(self.data_file_abs(rel)),
                    }
                )
        return records
