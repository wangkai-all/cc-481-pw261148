"""End-to-end tests for the DR drill engine.

Run with:  python -m unittest lakehouse_dr.tests.test_dr -v
"""
from __future__ import annotations

import os
import shutil
import tempfile
import unittest

from lakehouse_dr.backup import BackupStore
from lakehouse_dr.config import DRContext, PROD_MARKER
from lakehouse_dr.lakehouse import Lakehouse
from lakehouse_dr.recovery import RecoveryError, RecoveryManager, STAGE_ORDER
from lakehouse_dr.state import read_json


class DRTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.home = tempfile.mkdtemp(prefix="dr-test-")
        self.ctx = DRContext(self.home)
        self.lh = Lakehouse(self.ctx.lakehouse_root)
        self.lh.init()
        self.store = BackupStore(self.ctx)
        self.mgr = RecoveryManager(self.ctx)

    def tearDown(self) -> None:
        shutil.rmtree(self.home, ignore_errors=True)

    # ---------------------------------------------------------- test helpers
    def _seed(self) -> None:
        self.lh.create_table(
            "orders",
            {"columns": {"order_id": "string", "amount": "number", "status": "string"}},
            ["order_id"],
        )
        self.lh.ingest_rows("orders", [
            {"order_id": "O1", "amount": 10, "status": "new"},
            {"order_id": "O2", "amount": 20, "status": "new"},
        ])
        self.lh.ingest_rows("orders", [
            {"order_id": "O2", "amount": 25, "status": "paid"},  # update
            {"order_id": "O3", "amount": 30, "status": "new"},
        ])
        self.lh.delete_rows("orders", [{"order_id": "O1"}])

    def _recovered_lh(self, recovery_id: str) -> Lakehouse:
        journal = self.mgr.get_journal(recovery_id)
        return Lakehouse(journal["target_root"])


class TestLakehouseAndBackup(DRTestBase):
    def test_seed_state_and_cdc_watermark(self) -> None:
        self._seed()
        rows = self.lh.get_rows("orders")
        ids = sorted(r["order_id"] for r in rows)
        self.assertEqual(ids, ["O2", "O3"])  # O1 deleted
        o2 = next(r for r in rows if r["order_id"] == "O2")
        self.assertEqual(o2["amount"], 25)  # update folded correctly
        # 2 inserts + 2 (update+insert) + 1 delete = 5 CDC events
        self.assertEqual(self.lh.cdc_watermark(), 5)

    def test_backup_captures_versions_metadata_cdc_and_gates(self) -> None:
        self._seed()
        summary = self.store.create_backup(note="first")
        self.assertEqual(summary.gate_status, "passed")
        self.assertEqual(summary.cdc_position, 5)
        manifest = self.store.get_manifest(summary.backup_id)
        # 3 data batch files
        self.assertEqual(len(manifest["files"]), 3)
        # catalog + 3 per-table snapshots
        self.assertGreaterEqual(len(manifest["metadata_files"]), 4)
        self.assertTrue(all(f["version"] == 1 for f in manifest["files"]))
        # embedded catalog snapshot present
        self.assertIn("orders", manifest["catalog_snapshot"]["tables"])
        # gate report carried verbatim
        self.assertEqual(manifest["quality_gates"]["failed_count"], 0)

    def test_content_addressed_dedup_and_file_versions(self) -> None:
        self._seed()
        s1 = self.store.create_backup()
        # second backup with no lakehouse changes -> zero new blobs
        s2 = self.store.create_backup()
        self.assertEqual(s2.new_blobs, 0)
        # now change one file via a new ingest -> that path gets version 2
        self.lh.ingest_rows("orders", [{"order_id": "O9", "amount": 90, "status": "new"}])
        s3 = self.store.create_backup()
        versions = self.store.versions()["files"]
        # old data-file paths keep version 1; the brand-new path is v1 too,
        # and the latest backup captured exactly one more data file
        self.assertTrue(all(v["versions"][-1]["version"] >= 1 for v in versions.values()))
        m3 = self.store.get_manifest(s3.backup_id)
        m2 = self.store.get_manifest(s2.backup_id)
        # one new data file; metadata file count can also grow by one snapshot
        self.assertEqual(len(m3["files"]), len(m2["files"]) + 1)
        self.assertGreaterEqual(s3.files_captured, s2.files_captured + 1)

    def test_backup_with_corrupt_lakehouse_file_fails_gates_but_persists(self) -> None:
        self._seed()
        target = self.lh.table_meta("orders")["current_files"][0]
        with open(self.lh.data_file_abs(target), "wb") as fh:
            fh.write(b"{broken json line\n")
        summary = self.store.create_backup(note="damaged prod")
        self.assertEqual(summary.gate_status, "failed")
        # backup still exists and is listed
        self.assertEqual(len(self.store.list_backups()), 1)


class TestHappyPathRecovery(DRTestBase):
    def test_full_recovery_rebuilds_state_and_passes_gates(self) -> None:
        self._seed()
        backup = self.store.create_backup()
        # more activity after the backup
        self.lh.ingest_rows("orders", [{"order_id": "O7", "amount": 70, "status": "new"}])
        self.lh.ingest_rows("orders", [{"order_id": "O8", "amount": 80, "status": "new"}])
        self.store._sync_cdc_mirror(self.lh)
        watermark = self.lh.cdc_watermark()

        journal = self.mgr.create_recovery(target_seq=watermark)
        result = self.mgr.run_recovery(journal["recovery_id"])
        self.assertEqual(result["status"], "completed")
        for stage in STAGE_ORDER:
            self.assertEqual(result["stages"][stage]["status"], "completed", stage)

        rec = self._recovered_lh(journal["recovery_id"])
        self.assertEqual(rec.cdc_watermark(), watermark)
        ids = sorted(r["order_id"] for r in rec.get_rows("orders"))
        self.assertEqual(ids, ["O2", "O3", "O7", "O8"])  # O1 still deleted
        # validate stage attached gate report + diff
        self.assertEqual(result["gate_report"]["status"], "passed")
        diff = result["diff"]
        self.assertEqual(diff["missing_files"], [])
        self.assertEqual(diff["corrupted_files"], [])
        self.assertEqual(diff["row_counts"]["orders"]["recovered"], 4)
        # CDC replay added new batch files beyond the backup
        self.assertTrue(len(diff["added_files"]) >= 1)

    def test_point_in_time_target_seq(self) -> None:
        self._seed()  # watermark 5
        self.store.create_backup()
        self.lh.ingest_rows("orders", [{"order_id": "O7", "amount": 70, "status": "new"}])  # seq 6
        self.lh.ingest_rows("orders", [{"order_id": "O8", "amount": 80, "status": "new"}])  # seq 7
        self.store._sync_cdc_mirror(self.lh)

        journal = self.mgr.create_recovery(target_seq=6)
        self.mgr.run_recovery(journal["recovery_id"])
        rec = self._recovered_lh(journal["recovery_id"])
        ids = sorted(r["order_id"] for r in rec.get_rows("orders"))
        self.assertEqual(ids, ["O2", "O3", "O7"])  # O8 not yet applied

    def test_point_in_time_by_target_time_picks_backup(self) -> None:
        self._seed()
        first = self.store.create_backup(note="early")
        self.lh.ingest_rows("orders", [{"order_id": "O7", "amount": 70, "status": "new"}])
        self.store.create_backup(note="later")
        # ask for a timestamp between the two backups -> must resolve to first
        chosen = self.store.backup_at_or_before(first.created_at)
        self.assertIsNotNone(chosen)
        self.assertEqual(chosen["backup_id"], first.backup_id)

    def test_cannot_rewind_below_backup_position(self) -> None:
        self._seed()
        self.store.create_backup()
        with self.assertRaises(RecoveryError):
            self.mgr.create_recovery(target_seq=1)


class TestCorruptionAndQuarantine(DRTestBase):
    def test_corrupt_catalog_blob_is_rebuilt_from_embedded_snapshot(self) -> None:
        self._seed()
        backup = self.store.create_backup()
        # corrupt the catalog blob itself
        self.store.tamper_blob(backup.backup_id, "catalog.json")
        journal = self.mgr.create_recovery()
        result = self.mgr.run_recovery(journal["recovery_id"])
        self.assertEqual(result["status"], "completed")
        # metadata stage quarantined + rebuilt from the embedded catalog
        self.assertTrue(any(q["path"] == "catalog.json" for q in result["quarantine"]))
        self.assertTrue(
            any(g["type"] == "metadata_rebuilt_from_manifest" for g in result["gaps"]))
        rec = self._recovered_lh(journal["recovery_id"])
        self.assertEqual(rec.list_tables(), ["orders"])
        self.assertEqual(rec.count_rows("orders"), 2)
        # recovery still ends with passing gates
        self.assertEqual(result["gate_report"]["status"], "passed")

    def test_corrupt_blob_is_quarantined_with_impact_report(self) -> None:
        self._seed()
        backup = self.store.create_backup()
        manifest = self.store.get_manifest(backup.backup_id)
        victim = manifest["files"][0]["path"]
        self.store.tamper_blob(backup.backup_id, victim)

        # verify catches it first
        report = self.store.verify_backup(backup.backup_id)
        self.assertFalse(report["ok"])
        self.assertTrue(any(p["path"] == victim for p in report["problems"]))

        journal = self.mgr.create_recovery()
        result = self.mgr.run_recovery(journal["recovery_id"])
        # pipeline still completes; damage is quarantined and reported
        self.assertEqual(result["status"], "completed")
        quarantined = result["quarantine"]
        self.assertEqual(len(quarantined), 1)
        self.assertEqual(quarantined[0]["path"], victim)
        q_copy = os.path.join(self.ctx.recovery_dir(journal["recovery_id"]),
                              quarantined[0]["quarantined_copy"])
        self.assertTrue(os.path.exists(q_copy))
        # the recovered tree simply does not contain the bad file
        rec_root = result["target_root"]
        self.assertFalse(os.path.exists(os.path.join(rec_root, victim)))
        # gap recorded + impact report written
        self.assertTrue(any(g["type"] == "file_quarantined" for g in result["gaps"]))
        impact = read_json(os.path.join(self.ctx.recovery_dir(journal["recovery_id"]),
                                        "impact.json"))
        self.assertIn("orders", impact["affected_tables"])
        # immutable backup blob itself was never deleted
        self.assertTrue(os.path.exists(
            os.path.join(self.ctx.backup_root, manifest["files"][0]["blob"])))

    def test_cdc_gap_in_replay_window_is_reported(self) -> None:
        self._seed()
        self.store.create_backup()
        self.lh.ingest_rows("orders", [{"order_id": "O7", "amount": 70, "status": "new"}])
        self.lh.ingest_rows("orders", [{"order_id": "O8", "amount": 80, "status": "new"}])
        watermark = self.lh.cdc_watermark()
        self.store._sync_cdc_mirror(self.lh)
        # chop the mirror back to the backup position -> replay window all missing
        self.store.truncate_cdc_mirror(keep_first_n=5)

        journal = self.mgr.create_recovery(target_seq=watermark)
        result = self.mgr.run_recovery(journal["recovery_id"])
        cdc_gaps = [g for g in result["gaps"] if g["type"] == "cdc_gap"]
        self.assertEqual({g["seq"] for g in cdc_gaps}, {6, 7})
        # global gap analysis also sees it
        gaps = self.mgr.gap_analysis(journal["recovery_id"])
        self.assertIn(6, gaps["cdc_mirror"]["missing_sequences"])


class TestInterruptionAndRepeatDrills(DRTestBase):
    def test_interrupted_recovery_resumes_from_journal(self) -> None:
        self._seed()
        backup = self.store.create_backup()
        self.lh.ingest_rows("orders", [{"order_id": "O7", "amount": 70, "status": "new"}])
        self.store._sync_cdc_mirror(self.lh)

        journal = self.mgr.create_recovery(target_seq=self.lh.cdc_watermark())
        rid = journal["recovery_id"]
        interrupted = self.mgr.run_recovery(rid, fail_after_stage="metadata")
        self.assertEqual(interrupted["status"], "interrupted")
        self.assertEqual(interrupted["stages"]["metadata"]["status"], "completed")
        self.assertEqual(interrupted["stages"]["files"]["status"], "pending")

        # resume: completed stages are skipped, rest run to the end
        resumed = self.mgr.run_recovery(rid)
        self.assertEqual(resumed["status"], "completed")
        rec = self._recovered_lh(rid)
        self.assertIn("O7", [r["order_id"] for r in rec.get_rows("orders")])

    def test_repeated_runs_are_idempotent_and_isolated(self) -> None:
        self._seed()
        self.store.create_backup()
        r1 = self.mgr.create_recovery(note="drill-1")
        self.mgr.run_recovery(r1["recovery_id"])
        r2 = self.mgr.create_recovery(note="drill-2")
        self.mgr.run_recovery(r2["recovery_id"])
        # separate directories, separate journals
        self.assertNotEqual(r1["recovery_id"], r2["recovery_id"])
        self.assertTrue(os.path.isdir(self.ctx.recovery_dir(r1["recovery_id"])))
        self.assertTrue(os.path.isdir(self.ctx.recovery_dir(r2["recovery_id"])))
        # re-running a completed recovery is a no-op, keeps it immutable
        again = self.mgr.run_recovery(r1["recovery_id"])
        self.assertEqual(again["status"], "completed")
        self.assertEqual(len(self.mgr.list_recoveries()), 2)

    def test_recovery_never_overwrites_production(self) -> None:
        self._seed()
        self.store.create_backup()
        journal = self.mgr.create_recovery()
        # forge a production marker inside the target path -> guard must refuse
        target = journal["target_root"]
        os.makedirs(target, exist_ok=True)
        with open(os.path.join(target, PROD_MARKER), "w") as fh:
            fh.write("fake prod\n")
        with self.assertRaises(RecoveryError):
            self.mgr.run_recovery(journal["recovery_id"])

        # direct path-alias attempt is refused at creation time too
        ctx2 = DRContext(self.home)
        from lakehouse_dr.recovery import RecoveryManager as RM
        mgr2 = RM(ctx2)
        # create normal recovery then tamper journal target to the prod root
        j2 = mgr2.create_recovery()
        j2["target_root"] = self.ctx.lakehouse_root
        self.mgr._save_journal(j2)
        with self.assertRaises(RecoveryError):
            mgr2.run_recovery(j2["recovery_id"])
        # production catalog untouched
        self.assertTrue(os.path.exists(self.ctx.lakehouse_catalog))


class TestChainAndGaps(DRTestBase):
    def test_chain_reports_added_changed_removed(self) -> None:
        self._seed()
        b1 = self.store.create_backup()
        self.lh.ingest_rows("orders", [{"order_id": "O7", "amount": 70, "status": "new"}])
        b2 = self.store.create_backup()
        chain = self.store.chain()
        self.assertEqual(len(chain), 2)
        self.assertEqual(chain[0]["backup_id"], b1.backup_id)
        self.assertEqual(len(chain[1]["added"]), 1)
        # CDC positions move forward monotonically
        self.assertGreater(chain[1]["cdc_position"], chain[0]["cdc_position"])

    def test_gap_analysis_clean_when_healthy(self) -> None:
        self._seed()
        self.store.create_backup()
        gaps = self.mgr.gap_analysis()
        self.assertEqual(gaps["cdc_mirror"]["missing_sequences"], [])
        self.assertEqual(gaps["backup_chain_gaps"], [])


if __name__ == "__main__":
    unittest.main()
