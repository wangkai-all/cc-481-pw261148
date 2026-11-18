"""Staged point-in-time recovery engine.

Recovery pipeline (each stage is idempotent so an interrupted recovery can be
resumed from the journal):

    1. verify      re-hash every blob the base backup references
    2. metadata    rebuild catalog + per-table snapshot files first
    3. files       restore data files from blobs; corrupt blobs are quarantined
    4. cdc_replay  replay CDC events from the backup position to the target
    5. validate    quality gates + diff against the backup manifest

Safety properties:

* the target directory is created inside ``recovery_area/`` and is rejected if
  it carries the production marker, so a recovery can never overwrite the live
  production snapshot;
* blobs in the immutable ``backup_store/`` are only ever read, never modified;
* corrupted blobs are *copied* into ``quarantine/`` with an impact report.
"""
from __future__ import annotations

import os
import shutil

from . import gates, jsonl
from .backup import BackupStore
from .config import PROD_MARKER, RECOVERY_MARKER
from .hashing import sha256_file
from .lakehouse import Lakehouse, TOMBSTONE
from .state import (
    atomic_write_json,
    ensure_dir,
    is_production_tree,
    read_json,
    short_id,
    utcnow_iso,
)

STAGE_ORDER = ["verify", "metadata", "files", "cdc_replay", "validate"]
STAGE_LABELS = {
    "verify": "备份完整性校验",
    "metadata": "重建元数据快照",
    "files": "恢复数据文件（隔离损坏）",
    "cdc_replay": "按 CDC 位点回放",
    "validate": "质量门禁与差异验证",
}
JOURNAL_VERSION = 1


class RecoveryError(RuntimeError):
    pass


class RecoveryManager:
    def __init__(self, ctx):
        self.ctx = ctx
        self.backups = BackupStore(ctx)
        ensure_dir(ctx.recovery_root)

    # ------------------------------------------------------------- journals
    def list_recoveries(self) -> list[dict]:
        out: list[dict] = []
        if not os.path.isdir(self.ctx.recovery_root):
            return out
        for name in sorted(os.listdir(self.ctx.recovery_root)):
            jpath = self.ctx.journal_path(name)
            if os.path.isfile(jpath):
                out.append(read_json(jpath))
        return out

    def get_journal(self, recovery_id: str) -> dict:
        return read_json(self.ctx.journal_path(recovery_id))

    def _save_journal(self, journal: dict) -> None:
        atomic_write_json(self.ctx.journal_path(journal["recovery_id"]), journal)

    def _target_root(self, recovery_id: str) -> str:
        return os.path.join(self.ctx.recovery_dir(recovery_id), "recovered")

    def _quarantine_dir(self, recovery_id: str) -> str:
        return os.path.join(self.ctx.recovery_dir(recovery_id), "quarantine")

    def _baseline_dir(self, recovery_id: str) -> str:
        return os.path.join(self.ctx.recovery_dir(recovery_id), "baseline")

    # --------------------------------------------------------- safety guard
    def _assert_not_production(self, target_root: str) -> None:
        """Hard guard: refuse to write anywhere that looks like production."""
        probe = target_root
        for _ in range(6):
            if not probe or probe == os.path.dirname(probe):
                break
            if is_production_tree(probe):
                raise RecoveryError(
                    f"REFUSING to recover into production tree {probe!r}: "
                    f"contains {PROD_MARKER}. Recovery must target recovery_area/."
                )
            probe = os.path.dirname(probe)
        prod = os.path.abspath(self.ctx.lakehouse_root)
        if os.path.abspath(target_root) == prod or os.path.abspath(
            target_root
        ).startswith(prod + os.sep):
            raise RecoveryError(
                "REFUSING to recover into the production lakehouse path. "
                "Recovery must target recovery_area/."
            )

    # ------------------------------------------------------------ create run
    def create_recovery(
        self,
        backup_id: str | None = None,
        target_time: str | None = None,
        target_seq: int | None = None,
        note: str = "",
    ) -> dict:
        # Choose base backup: explicit id, or point-in-time selection.
        if backup_id:
            entry = self.backups.get_backup(backup_id)
        elif target_time:
            entry = self.backups.backup_at_or_before(target_time)
            if entry is None:
                raise RecoveryError(f"no backup exists at or before {target_time}")
        else:
            entry = self.backups.latest_backup()
            if entry is None:
                raise RecoveryError("no backups available to recover from")
        manifest = self.backups.get_manifest(entry["backup_id"])

        base_position = manifest["cdc"]["position"]
        if target_seq is None:
            target_seq = base_position
        if target_seq < base_position:
            raise RecoveryError(
                f"target CDC seq {target_seq} precedes backup position "
                f"{base_position}; cannot rewind below the backup point"
            )
        mirror_watermark = self._mirror_watermark()
        if target_seq > mirror_watermark:
            # Not fatal at creation: a truncated/torn CDC mirror is exactly
            # the gap scenario the drill must surface. Stage 4 detects the
            # missing sequences and records them as recovery gaps.
            journal_gap_note = (
                f"target seq {target_seq} exceeds CDC mirror watermark "
                f"{mirror_watermark}; missing events will be reported as gaps"
            )
        else:
            journal_gap_note = None

        recovery_id = short_id("rc-")
        target_root = self._target_root(recovery_id)
        self._assert_not_production(target_root)
        ensure_dir(self.ctx.recovery_dir(recovery_id))
        ensure_dir(self._quarantine_dir(recovery_id))
        ensure_dir(self._baseline_dir(recovery_id))

        journal = {
            "version": JOURNAL_VERSION,
            "recovery_id": recovery_id,
            "created_at": utcnow_iso(),
            "note": note,
            "base_backup_id": entry["backup_id"],
            "base_backup_at": entry["created_at"],
            "base_cdc_position": base_position,
            "target_time": target_time,
            "target_cdc_seq": target_seq,
            "target_root": target_root,
            "cdc_mirror_warning": journal_gap_note,
            "status": "created",
            "current_stage": None,
            "stages": {
                s: {"status": "pending", "started_at": None, "finished_at": None,
                    "detail": None, "error": None}
                for s in STAGE_ORDER
            },
            "quarantine": [],
            "gaps": [],
            "gate_report": None,
            "diff": None,
            "updated_at": utcnow_iso(),
        }
        self._save_journal(journal)
        return journal

    def _mirror_watermark(self) -> int:
        events = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        return max((e["seq"] for e in events), default=0)

    # --------------------------------------------------------------- runner
    def run_recovery(
        self,
        recovery_id: str,
        fail_after_stage: str | None = None,
    ) -> dict:
        """Execute (or resume) a recovery.

        ``fail_after_stage`` is a drill hook: the process crashes after the
        named stage completes, journal already durable, so resume can be
        exercised.
        """
        journal = self.get_journal(recovery_id)
        if journal["status"] == "completed":
            return journal  # repeat drill: completed runs stay immutable
        self._assert_not_production(journal["target_root"])
        journal["status"] = "running"
        self._save_journal(journal)

        for stage in STAGE_ORDER:
            state = journal["stages"][stage]
            if state["status"] == "completed":
                continue  # resume: skip durable, finished stage
            state["status"] = "running"
            state["started_at"] = utcnow_iso()
            journal["current_stage"] = stage
            self._save_journal(journal)
            try:
                detail = getattr(self, f"_stage_{stage}")(journal)
            except Exception as exc:  # noqa: BLE001 - recorded, not swallowed
                state["status"] = "failed"
                state["error"] = f"{type(exc).__name__}: {exc}"
                state["finished_at"] = utcnow_iso()
                journal["status"] = "failed"
                self._save_journal(journal)
                raise
            state["status"] = "completed"
            state["detail"] = detail
            state["finished_at"] = utcnow_iso()
            journal["updated_at"] = utcnow_iso()
            self._save_journal(journal)

            if fail_after_stage == stage:
                journal["status"] = "interrupted"
                self._save_journal(journal)
                return journal

        journal["status"] = "completed"
        journal["current_stage"] = None
        journal["updated_at"] = utcnow_iso()
        self._save_journal(journal)
        return journal

    # ------------------------------------------------------------ stage 1/5
    def _stage_verify(self, journal: dict) -> dict:
        report = self.backups.verify_backup(journal["base_backup_id"])
        if not report["ok"]:
            # Corruption is surfaced as gaps; stage 3 decides quarantine.
            for p in report["problems"]:
                journal["gaps"].append(
                    {
                        "type": "corrupted_backup_file",
                        "path": p["path"],
                        "detail": p["reason"],
                        "expected_sha256": p.get("expected_sha256"),
                        "actual_sha256": p.get("actual_sha256"),
                    }
                )
            for p in report["cdc_problems"]:
                journal["gaps"].append(
                    {"type": "cdc_gap", "seq": p["seq"], "detail": p["reason"]}
                )
        return {
            "files_checked": report["files_checked"],
            "problems": len(report["problems"]),
            "cdc_problems": len(report["cdc_problems"]),
        }

    # ------------------------------------------------------------ stage 2/5
    def _stage_metadata(self, journal: dict) -> dict:
        manifest = self.backups.get_manifest(journal["base_backup_id"])
        target = journal["target_root"]
        ensure_dir(target)
        # mark this tree so the safety guard (and operators) can identify it
        with open(os.path.join(target, RECOVERY_MARKER), "w", encoding="utf-8") as fh:
            fh.write(f"recovery target {journal['recovery_id']}\n")

        restored_meta = 0
        for entry in manifest["metadata_files"]:
            blob_abs = os.path.join(self.ctx.backup_root, entry["blob"])
            dst = os.path.join(target, entry["path"])
            ensure_dir(os.path.dirname(dst))
            if not os.path.exists(blob_abs) or sha256_file(blob_abs) != entry["sha256"]:
                # metadata corruption: quarantine and rebuild from embedded copy
                if os.path.exists(blob_abs):
                    self._quarantine(journal, entry, blob_abs, "metadata restore")
                if entry["kind"] == "catalog":
                    atomic_write_json(dst, manifest["catalog_snapshot"])
                else:
                    table = entry.get("table")
                    snap_id = os.path.basename(entry["path"])[:-5]
                    snap = next(
                        (s for t, m in manifest["catalog_snapshot"]["tables"].items()
                         if t == table for s in m["snapshots"]
                         if s["snapshot_id"] == snap_id),
                        None,
                    )
                    if snap is not None:
                        atomic_write_json(dst, snap)
                journal["gaps"].append(
                    {"type": "metadata_rebuilt_from_manifest", "path": entry["path"],
                     "detail": "blob corrupt/missing; rebuilt from embedded catalog snapshot"}
                )
            else:
                shutil.copy2(blob_abs, dst)
            restored_meta += 1

        # baseline = pristine copy of the metadata-rebuilt tree for replay reset
        baseline = self._baseline_dir(journal["recovery_id"])
        if os.path.exists(baseline):
            shutil.rmtree(baseline)
        shutil.copytree(target, baseline)
        return {"metadata_files_restored": restored_meta}

    # ------------------------------------------------------------ stage 3/5
    def _stage_files(self, journal: dict) -> dict:
        manifest = self.backups.get_manifest(journal["base_backup_id"])
        target = journal["target_root"]
        restored, quarantined = 0, 0
        affected_tables: dict[str, list[str]] = {}

        # reset target tree back to metadata-only baseline for idempotent resume
        baseline = self._baseline_dir(journal["recovery_id"])
        for entry in manifest["files"]:
            blob_abs = os.path.join(self.ctx.backup_root, entry["blob"])
            dst = os.path.join(target, entry["path"])
            ensure_dir(os.path.dirname(dst))
            intact = os.path.exists(blob_abs) and sha256_file(blob_abs) == entry["sha256"]
            if not intact:
                self._quarantine(journal, entry, blob_abs, "data file restore")
                quarantined += 1
                affected_tables.setdefault(entry["table"], []).append(entry["path"])
                journal["gaps"].append(
                    {"type": "file_quarantined", "path": entry["path"],
                     "table": entry["table"],
                     "detail": "backup blob corrupt or missing; file excluded from recovery"}
                )
                continue
            shutil.copy2(blob_abs, dst)
            restored += 1

        # Rewrite catalog: drop quarantined files from current_files + snapshots
        catalog_path = os.path.join(target, "catalog.json")
        catalog = read_json(catalog_path)
        excluded = {q["path"] for q in journal["quarantine"]}
        for table, meta in catalog.get("tables", {}).items():
            meta["current_files"] = [
                f for f in meta.get("current_files", []) if f not in excluded
            ]
            new_snaps = []
            for snap in meta.get("snapshots", []):
                snap = dict(snap)
                snap["files"] = [f for f in snap["files"] if f not in excluded]
                new_snaps.append(snap)
            meta["snapshots"] = new_snaps
            # folded row count over the surviving files
            lh = Lakehouse(target)
            try:
                meta["row_count"] = lh.count_rows(table)
            except (ValueError, KeyError):
                meta["row_count"] = -1
        atomic_write_json(catalog_path, catalog)
        # refresh baseline so stage 4 always replays from this exact state
        shutil.rmtree(baseline)
        shutil.copytree(target, baseline)

        self._write_impact_report(journal, affected_tables)
        return {"files_restored": restored, "files_quarantined": quarantined}

    def _quarantine(self, journal: dict, entry: dict, blob_abs: str, phase: str) -> None:
        qdir = self._quarantine_dir(journal["recovery_id"])
        safe_name = entry["path"].replace(os.sep, "__")
        dst = os.path.join(qdir, safe_name)
        if os.path.exists(blob_abs):
            shutil.copy2(blob_abs, dst)
            actual = sha256_file(blob_abs)
        else:
            dst = dst + ".MISSING"
            with open(dst, "w", encoding="utf-8") as fh:
                fh.write("blob missing in backup store\n")
            actual = None
        journal["quarantine"].append(
            {
                "path": entry["path"],
                "table": entry.get("table"),
                "kind": entry.get("kind"),
                "expected_sha256": entry["sha256"],
                "actual_sha256": actual,
                "quarantined_copy": os.path.relpath(dst, self.ctx.recovery_dir(journal["recovery_id"])),
                "phase": phase,
                "quarantined_at": utcnow_iso(),
            }
        )

    def _write_impact_report(self, journal: dict, affected_tables: dict) -> None:
        manifest = self.backups.get_manifest(journal["base_backup_id"])
        # The corrupt blobs cannot be parsed, so row impact is derived from the
        # catalog's snapshot row counts minus what still folds from survivors.
        rows_lost: dict[str, int] = {}
        lh = Lakehouse(journal["target_root"])
        for table in affected_tables:
            snap_rows = self._catalog_row_counts(manifest).get(table, 0)
            try:
                have = lh.count_rows(table)
            except (ValueError, KeyError):
                have = 0
            rows_lost[table] = max(snap_rows - have, 0)

        # CDC replay events targeting a table with a quarantined source file
        excluded = {q["path"] for q in journal["quarantine"] if q.get("kind") == "data"}
        events = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        replay_events = [
            e for e in events
            if journal["base_cdc_position"] < e["seq"] <= journal["target_cdc_seq"]
        ]
        affected_event_seqs = sorted(
            e["seq"] for e in replay_events if e.get("table") in affected_tables
        )
        impact = {
            "generated_at": utcnow_iso(),
            "quarantined_file_count": len(journal["quarantine"]),
            "affected_tables": sorted(affected_tables.keys()),
            "approx_rows_unavailable": rows_lost,
            "quarantined_paths": sorted(excluded),
            "cdc_events_in_replay_window": len(replay_events),
            "cdc_events_impacted_by_quarantine": affected_event_seqs[:200],
            "residual_risk": (
                "CDC replay for affected tables may partially fail; rows that "
                "cannot be rebuilt remain missing until the file is restored."
            ) if journal["quarantine"] else "none",
        }
        atomic_write_json(
            os.path.join(self.ctx.recovery_dir(journal["recovery_id"]), "impact.json"),
            impact,
        )

    @staticmethod
    def _catalog_row_counts(manifest: dict) -> dict:
        return {
            t: m.get("row_count", 0)
            for t, m in manifest["catalog_snapshot"].get("tables", {}).items()
        }

    # ------------------------------------------------------------ stage 4/5
    def _stage_cdc_replay(self, journal: dict) -> dict:
        target = journal["target_root"]
        # idempotent reset to the post-stage-3 baseline before replaying
        baseline = self._baseline_dir(journal["recovery_id"])
        if os.path.exists(target):
            shutil.rmtree(target)
        shutil.copytree(baseline, target)
        with open(os.path.join(target, RECOVERY_MARKER), "w", encoding="utf-8") as fh:
            fh.write(f"recovery target {journal['recovery_id']}\n")

        events = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        window = [
            e for e in events
            if journal["base_cdc_position"] < e["seq"] <= journal["target_cdc_seq"]
        ]
        # gap detection inside the replay window
        present = {e["seq"] for e in window}
        gap_seqs = [
            s for s in range(journal["base_cdc_position"] + 1,
                             journal["target_cdc_seq"] + 1)
            if s not in present
        ]
        for s in gap_seqs:
            journal["gaps"].append(
                {"type": "cdc_gap", "seq": s,
                 "detail": "event missing in backup CDC mirror; replay skips it"}
            )

        lh = Lakehouse(target)
        applied, skipped = 0, 0
        skip_detail: list[dict] = []
        per_table: dict[str, list[dict]] = {}
        for event in window:
            table = event.get("table")
            if table not in lh.list_tables():
                skipped += 1
                skip_detail.append({"seq": event["seq"], "reason": "table absent"})
                continue
            per_table.setdefault(table, []).append(event)

        for table, table_events in per_table.items():
            meta = lh.table_meta(table)
            pk = meta["primary_key"]
            state = lh._merged_rows(table)  # noqa: SLF001 - controlled rebuild
            new_rows: list[dict] = []
            tomb: list[dict] = []
            for event in table_events:
                key = tuple(event["key"][k] for k in pk)
                if event["op"] in ("insert", "update"):
                    if event.get("after") is None:
                        skipped += 1
                        skip_detail.append({"seq": event["seq"], "reason": "null after"})
                        continue
                    state[key] = dict(event["after"])
                    new_rows.append(dict(event["after"]))
                elif event["op"] == "delete":
                    state[key] = {**event["key"], TOMBSTONE: True}
                    tomb.append({**event["key"], TOMBSTONE: True})
                applied += 1
            batch = new_rows + tomb
            if not batch:
                continue
            # write replay output as one new batch file + CDC events + snapshot.
            # Preserve the original CDC seq values so the recovered watermark
            # matches the target position instead of restarting at 1.
            replay_events = [dict(e) for e in table_events
                             if e["op"] in ("insert", "update", "delete")
                             and not (e["op"] != "delete" and e.get("after") is None)]
            lh._commit_batch(  # noqa: SLF001
                table, batch, replay_events,
                seq_override=table_events[0]["seq"],
            )

        return {
            "events_in_window": len(window),
            "events_applied": applied,
            "events_skipped": skipped,
            "cdc_gaps": gap_seqs,
            "skipped_detail": skip_detail[:50],
            "from_seq": journal["base_cdc_position"],
            "to_seq": journal["target_cdc_seq"],
        }

    # ------------------------------------------------------------ stage 5/5
    def _stage_validate(self, journal: dict) -> dict:
        target = journal["target_root"]
        lh = Lakehouse(target)
        report = gates.run_quality_gates(lh)
        journal["gate_report"] = report
        diff = self.diff_against_backup(journal["recovery_id"])
        journal["diff"] = diff
        return {
            "gate_status": report["status"],
            "failed_gates": report["failed_count"],
            "diff_summary": {k: len(v) for k, v in diff.items() if isinstance(v, list)},
        }

    # -------------------------------------------------------------- analysis
    def diff_against_backup(self, recovery_id: str) -> dict:
        """Diff recovered tree vs the backup manifest (the recovery truth)."""
        journal = self.get_journal(recovery_id)
        manifest = self.backups.get_manifest(journal["base_backup_id"])
        target = journal["target_root"]
        lh = Lakehouse(target)

        expected = {f["path"]: f for f in manifest["files"]}
        actual: dict[str, dict] = {}
        for rec in lh.all_file_records():
            actual[rec["path"]] = rec

        quarantined = {q["path"] for q in journal["quarantine"]}
        missing, corrupted, added, changed = [], [], [], []
        for path, f in expected.items():
            if path in quarantined:
                corrupted.append({"path": path, "table": f["table"],
                                  "reason": "quarantined during recovery"})
                continue
            if path not in actual:
                missing.append({"path": path, "table": f["table"]})
            elif actual[path]["sha256"] != f["sha256"]:
                changed.append({"path": path, "table": f["table"],
                                "expected_sha256": f["sha256"],
                                "actual_sha256": actual[path]["sha256"]})
        for path in actual:
            if path not in expected:
                added.append({"path": path, "table": actual[path]["table"],
                              "reason": "created by CDC replay"})

        row_counts: dict[str, dict] = {}
        for table, meta in manifest["catalog_snapshot"].get("tables", {}).items():
            try:
                actual_count = lh.count_rows(table) if table in lh.list_tables() else 0
            except (ValueError, KeyError):
                actual_count = -1
            row_counts[table] = {
                "at_backup": meta.get("row_count", 0),
                "recovered": actual_count,
                "delta": actual_count - meta.get("row_count", 0),
            }
        return {
            "missing_files": missing,
            "corrupted_files": corrupted,
            "changed_files": changed,
            "added_files": added,
            "row_counts": row_counts,
            "cdc": {
                "base_position": journal["base_cdc_position"],
                "target_seq": journal["target_cdc_seq"],
                "recovered_watermark": lh.cdc_watermark(),
            },
        }

    def gap_analysis(self, recovery_id: str | None = None) -> dict:
        """Aggregate gaps: backup chain, CDC coverage, file versions, recovery."""
        # 1) CDC coverage gaps across the whole mirror. Two gap shapes:
        #    a) holes inside the observed range;
        #    b) a truncated tail - the newest backup recorded a position that
        #       the mirror no longer reaches.
        events = jsonl.read_jsonl(self.ctx.backup_cdc_log)
        seqs = sorted(e["seq"] for e in events)
        cdc_missing: list[int] = []
        if seqs:
            present = set(seqs)
            cdc_missing = [s for s in range(seqs[0], seqs[-1] + 1) if s not in present]
        known_high_water = max(
            [self.backups.cdc_high_water_mark()]
            + [b["cdc_position"] for b in self.backups.list_backups()],
        )
        mirror_watermark = seqs[-1] if seqs else 0
        present_seqs = set(seqs)
        if known_high_water > mirror_watermark:
            cdc_missing.extend(
                s for s in range(mirror_watermark + 1, known_high_water + 1)
                if s not in present_seqs
            )
            cdc_missing = sorted(set(cdc_missing))

        # 2) backup chain time gaps + position jumps
        chain = self.backups.chain()
        chain_gaps: list[dict] = []
        for prev, cur in zip(chain, chain[1:]):
            if cur["cdc_position"] < prev["cdc_position"]:
                chain_gaps.append(
                    {"type": "cdc_position_regression",
                     "from_backup": prev["backup_id"], "to_backup": cur["backup_id"],
                     "detail": f"{prev['cdc_position']} -> {cur['cdc_position']}"})
            if cur["removed"]:
                chain_gaps.append(
                    {"type": "file_disappeared", "backup_id": cur["backup_id"],
                     "paths": cur["removed"]})

        # 3) per-path version gaps (files whose hash changed without a full chain)
        version_gaps: list[dict] = []
        versions = self.backups.versions()["files"]
        for path, info in versions.items():
            vers = info["versions"]
            if len(vers) > 1:
                for a, b in zip(vers, vers[1:]):
                    chain_gap_backups = [
                        c["backup_id"] for c in chain
                        if c["backup_id"] < b["first_seen_in"]
                        and c["backup_id"] > a["first_seen_in"]
                    ]
                    version_gaps.append(
                        {"path": path, "table": info["table"],
                         "from_version": a["version"], "to_version": b["version"],
                         "backups_between": chain_gap_backups})

        # 4) recovery-specific gaps
        recovery_gaps: list[dict] = []
        if recovery_id:
            recovery_gaps = list(self.get_journal(recovery_id).get("gaps", []))

        return {
            "generated_at": utcnow_iso(),
            "cdc_mirror": {
                "watermark": seqs[-1] if seqs else 0,
                "known_high_water_mark": known_high_water,
                "event_count": len(seqs),
                "missing_sequences": cdc_missing,
            },
            "backup_chain_gaps": chain_gaps,
            "file_version_gaps": version_gaps,
            "recovery_gaps": recovery_gaps,
        }
