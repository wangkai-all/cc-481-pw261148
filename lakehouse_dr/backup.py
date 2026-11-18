"""Backup engine.

A backup is an immutable snapshot bundle containing four things, matching the
DR requirements:

1. **File versions** - every data/metadata file captured with relative path,
   SHA-256 hash and size. Identical content is stored once in the content
   addressed ``blobs/`` tree; the per-path version history lives in
   ``file-versions.json``.
2. **Metadata snapshot** - a verbatim copy of the lakehouse catalog plus every
   per-table snapshot file, recorded with their own hashes.
3. **CDC position** - the global CDC watermark at backup time, and a mirror of
   the production CDC log so recovery can replay without touching production.
4. **Quality gate results** - the full gate report from backup time.
"""
from __future__ import annotations

import os
import shutil
from dataclasses import dataclass

from . import gates, jsonl
from .config import CATALOG_NAME, CDC_DIR, CDC_LOG
from .hashing import sha256_file
from .lakehouse import Lakehouse
from .state import (
    atomic_write_json,
    ensure_dir,
    read_json,
    short_id,
    utcnow_iso,
)

INDEX_VERSION = 1
VERSIONS_VERSION = 1


@dataclass
class BackupSummary:
    backup_id: str
    created_at: str
    kind: str
    cdc_position: int
    files_captured: int
    new_blobs: int
    gate_status: str
    base_backup_id: str | None
    parent_backup_id: str | None
    note: str

    def to_dict(self) -> dict:
        return dict(self.__dict__)


class BackupError(RuntimeError):
    pass


class BackupStore:
    def __init__(self, ctx):
        self.ctx = ctx
        ensure_dir(ctx.backup_root)
        ensure_dir(ctx.backup_blob_dir)
        ensure_dir(os.path.join(ctx.backup_root, CDC_DIR))
        ensure_dir(os.path.join(ctx.backup_root, "backups"))
        if not os.path.exists(ctx.backup_index):
            atomic_write_json(
                ctx.backup_index,
                {"version": INDEX_VERSION, "backups": []},
            )
        if not os.path.exists(ctx.file_versions_path):
            atomic_write_json(
                ctx.file_versions_path,
                {"version": VERSIONS_VERSION, "files": {}},
            )

    # ------------------------------------------------------------- registries
    def index(self) -> dict:
        return read_json(self.ctx.backup_index)

    def _save_index(self, index: dict) -> None:
        atomic_write_json(self.ctx.backup_index, index)

    def versions(self) -> dict:
        return read_json(self.ctx.file_versions_path)

    def _save_versions(self, versions: dict) -> None:
        atomic_write_json(self.ctx.file_versions_path, versions)

    def list_backups(self) -> list[dict]:
        return list(self.index()["backups"])

    def get_manifest(self, backup_id: str) -> dict:
        return read_json(self.ctx.manifest_path(backup_id))

    def get_backup(self, backup_id: str) -> dict:
        for entry in self.index()["backups"]:
            if entry["backup_id"] == backup_id:
                return entry
        raise BackupError(f"no such backup: {backup_id}")

    def latest_backup(self) -> dict | None:
        backups = self.list_backups()
        return backups[-1] if backups else None

    # ------------------------------------------------------------- blob store
    def _blob_path(self, digest: str) -> str:
        return os.path.join(self.ctx.backup_blob_dir, digest[:2], digest)

    def _store_blob(self, src_abs: str, digest: str) -> bool:
        """Copy a file into the content-addressed store. Returns True if new."""
        dst = self._blob_path(digest)
        if os.path.exists(dst):
            return False
        ensure_dir(os.path.dirname(dst))
        shutil.copy2(src_abs, dst)
        return True

    # --------------------------------------------------------------- cdc sync
    def _sync_cdc_mirror(self, lh: Lakehouse) -> tuple[int, int]:
        """Append production CDC events missing from the mirror.

        Returns (watermark, appended_count). A durable high-water mark is kept
        so truncation of the mirror can later be detected even though no
        backup happened to record the lost positions.
        """
        prod_events = jsonl.read_jsonl(lh.cdc_log_path)
        mirrored = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        have_seqs = {e["seq"] for e in mirrored}
        new_events = [e for e in prod_events if e["seq"] not in have_seqs]
        if new_events:
            jsonl.append_jsonl(self.ctx.backup_cdc_log, new_events)
        watermark = prod_events[-1]["seq"] if prod_events else 0
        self._update_cdc_high_water(watermark)
        return watermark, len(new_events)

    def _cdc_state_path(self) -> str:
        return os.path.join(self.ctx.backup_root, "cdc", "state.json")

    def _update_cdc_high_water(self, watermark: int) -> None:
        path = self._cdc_state_path()
        state = read_json(path, default={"high_water_mark": 0})
        if watermark > state.get("high_water_mark", 0):
            state["high_water_mark"] = watermark
            state["updated_at"] = utcnow_iso()
            atomic_write_json(path, state)

    def cdc_high_water_mark(self) -> int:
        return read_json(self._cdc_state_path(),
                         default={"high_water_mark": 0}).get("high_water_mark", 0)

    # ----------------------------------------------------------------- backup
    def create_backup(self, kind: str = "full", note: str = "") -> BackupSummary:
        if kind not in ("full", "incremental"):
            raise BackupError(f"unknown backup kind: {kind}")
        lh = Lakehouse(self.ctx.lakehouse_root)
        catalog = lh.catalog()
        versions = self.versions()
        index = self.index()
        parent = self.latest_backup()
        base = parent if parent and kind == "incremental" else None

        backup_id = short_id("bk-")
        backup_dir = self.ctx.backup_dir(backup_id)
        ensure_dir(backup_dir)
        ensure_dir(os.path.join(backup_dir, "snapshots"))
        created_at = utcnow_iso()

        # --- 1/4 capture every data file with hash + version history --------
        files: list[dict] = []
        new_blobs = 0
        for rec in lh.all_file_records():
            src = lh.data_file_abs(rec["path"])
            digest = rec["sha256"]
            if self._store_blob(src, digest):
                new_blobs += 1
            history = versions["files"].setdefault(
                rec["path"],
                {"table": rec["table"], "versions": []},
            )
            prev_version = history["versions"][-1]["version"] if history["versions"] else 0
            changed = not history["versions"] or history["versions"][-1]["sha256"] != digest
            if changed:
                history["versions"].append(
                    {
                        "version": prev_version + 1,
                        "sha256": digest,
                        "size": rec["size"],
                        "first_seen_in": backup_id,
                        "first_seen_at": created_at,
                    }
                )
            current = history["versions"][-1]
            files.append(
                {
                    "path": rec["path"],
                    "table": rec["table"],
                    "kind": "data",
                    "sha256": digest,
                    "size": rec["size"],
                    "version": current["version"],
                    "blob": os.path.relpath(self._blob_path(digest), self.ctx.backup_root),
                }
            )

        # --- 2/4 metadata snapshot: catalog + per-table snapshot files ------
        metadata_files: list[dict] = []
        catalog_digest = sha256_file(lh.catalog_path)
        if self._store_blob(lh.catalog_path, catalog_digest):
            new_blobs += 1
        metadata_files.append(
            {
                "path": CATALOG_NAME,
                "kind": "catalog",
                "sha256": catalog_digest,
                "size": os.path.getsize(lh.catalog_path),
                "blob": os.path.relpath(self._blob_path(catalog_digest), self.ctx.backup_root),
            }
        )
        for rec in lh.all_snapshot_records():
            src = lh.data_file_abs(rec["path"])
            if self._store_blob(src, rec["sha256"]):
                new_blobs += 1
            metadata_files.append(
                {
                    "path": rec["path"],
                    "table": rec["table"],
                    "kind": "table-snapshot",
                    "sha256": rec["sha256"],
                    "size": rec["size"],
                    "blob": os.path.relpath(self._blob_path(rec["sha256"]), self.ctx.backup_root),
                }
            )

        # --- 3/4 CDC position + mirror sync --------------------------------
        watermark, cdc_appended = self._sync_cdc_mirror(lh)

        # --- 4/4 quality gates ---------------------------------------------
        gate_report = gates.run_quality_gates(lh)

        manifest = {
            "backup_id": backup_id,
            "kind": kind,
            "created_at": created_at,
            "note": note,
            "lakehouse_root": self.ctx.lakehouse_root,
            "parent_backup_id": parent["backup_id"] if parent else None,
            "base_backup_id": base["backup_id"] if base else None,
            "cdc": {
                "position": watermark,
                "mirror_path": os.path.join(CDC_DIR, CDC_LOG),
                "events_appended": cdc_appended,
            },
            "files": files,
            "metadata_files": metadata_files,
            "catalog_snapshot": catalog,
            "quality_gates": gate_report,
            "stats": {
                "tables": len(catalog.get("tables", {})),
                "data_files": len(files),
                "metadata_files": len(metadata_files),
                "new_blobs": new_blobs,
            },
        }
        atomic_write_json(self.ctx.manifest_path(backup_id), manifest)
        self._save_versions(versions)

        summary = BackupSummary(
            backup_id=backup_id,
            created_at=created_at,
            kind=kind,
            cdc_position=watermark,
            files_captured=len(files) + len(metadata_files),
            new_blobs=new_blobs,
            gate_status=gate_report["status"],
            base_backup_id=manifest["base_backup_id"],
            parent_backup_id=manifest["parent_backup_id"],
            note=note,
        )
        index["backups"].append(
            dict(summary.to_dict(), manifest_path=os.path.relpath(
                self.ctx.manifest_path(backup_id), self.ctx.backup_root))
        )
        self._save_index(index)
        return summary

    # --------------------------------------------------------------- verify
    def verify_backup(self, backup_id: str) -> dict:
        """Re-hash every blob referenced by a backup manifest."""
        manifest = self.get_manifest(backup_id)
        problems: list[dict] = []
        checked = 0
        for entry in manifest["files"] + manifest["metadata_files"]:
            blob_abs = os.path.join(self.ctx.backup_root, entry["blob"])
            checked += 1
            if not os.path.exists(blob_abs):
                problems.append({"path": entry["path"], "reason": "blob missing",
                                 "expected_sha256": entry["sha256"]})
                continue
            actual = sha256_file(blob_abs)
            if actual != entry["sha256"]:
                problems.append(
                    {
                        "path": entry["path"],
                        "reason": "sha256 mismatch",
                        "expected_sha256": entry["sha256"],
                        "actual_sha256": actual,
                    }
                )
        # CDC mirror continuity up to the recorded position
        cdc_problems: list[dict] = []
        events = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        in_range = [e for e in events if e["seq"] <= manifest["cdc"]["position"]]
        seqs = {e["seq"] for e in in_range}
        missing_seqs = [s for s in range(1, manifest["cdc"]["position"] + 1) if s not in seqs]
        for s in missing_seqs[:50]:
            cdc_problems.append({"seq": s, "reason": "missing CDC event in mirror"})

        return {
            "backup_id": backup_id,
            "verified_at": utcnow_iso(),
            "files_checked": checked,
            "ok": not problems and not cdc_problems,
            "problems": problems,
            "cdc_problems": cdc_problems,
            "cdc_position": manifest["cdc"]["position"],
            "cdc_mirrored_events": len(in_range),
        }

    # --------------------------------------------------------- point-in-time
    def backup_at_or_before(self, target_iso: str) -> dict | None:
        """Newest backup whose timestamp <= target (point-in-time selection)."""
        chosen = None
        for entry in self.list_backups():
            if entry["created_at"] <= target_iso:
                chosen = entry
        return chosen

    # ------------------------------------------------------------- tampering
    def tamper_blob(self, backup_id: str, file_path: str) -> dict:
        """Drill helper: corrupt the blob backing one file inside a backup."""
        manifest = self.get_manifest(backup_id)
        for entry in manifest["files"] + manifest["metadata_files"]:
            if entry["path"] == file_path:
                blob_abs = os.path.join(self.ctx.backup_root, entry["blob"])
                if not os.path.exists(blob_abs):
                    raise BackupError(f"blob missing already: {file_path}")
                with open(blob_abs, "r+b") as fh:
                    fh.seek(0)
                    fh.write(b"###DRILL-CORRUPTION###")
                return {"backup_id": backup_id, "path": file_path,
                        "expected_sha256": entry["sha256"],
                        "actual_sha256": sha256_file(blob_abs)}
        raise BackupError(f"file not in backup {backup_id}: {file_path}")

    def truncate_cdc_mirror(self, keep_first_n: int) -> dict:
        """Drill helper: truncate the CDC mirror to simulate a log gap."""
        events = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        kept = events[:keep_first_n]
        jsonl.write_jsonl(self.ctx.backup_cdc_log, kept)
        return {"kept_events": len(kept), "removed": len(events) - len(kept)}

    # ------------------------------------------------------------- analysis
    def chain(self) -> list[dict]:
        """Backup chain with per-link change deltas against the parent."""
        out: list[dict] = []
        prev_files: dict[str, str] = {}
        for entry in self.list_backups():
            manifest = self.get_manifest(entry["backup_id"])
            current = {f["path"]: f["sha256"] for f in manifest["files"]}
            added = sorted(p for p in current if p not in prev_files)
            changed = sorted(p for p in current if p in prev_files and current[p] != prev_files[p])
            removed = sorted(p for p in prev_files if p not in current)
            out.append(
                {
                    "backup_id": entry["backup_id"],
                    "created_at": entry["created_at"],
                    "kind": entry["kind"],
                    "gate_status": entry["gate_status"],
                    "cdc_position": entry["cdc_position"],
                    "added": added,
                    "changed": changed,
                    "removed": removed,
                    "parent_backup_id": entry["parent_backup_id"],
                }
            )
            prev_files = current
        return out
