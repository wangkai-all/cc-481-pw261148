"""HTTP API tests using the stdlib WSGI-ish handler via a real socket.

A ThreadingHTTPServer is bound to an ephemeral port in a background thread;
requests go through urllib so the full routing/serialization stack is
exercised.
"""
from __future__ import annotations

import json
import shutil
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from lakehouse_dr.backup import BackupStore
from lakehouse_dr.config import DRContext
from lakehouse_dr.lakehouse import Lakehouse
from lakehouse_dr.web import _make_handler, DRServer


class APITestBase(unittest.TestCase):
    def setUp(self) -> None:
        self.home = tempfile.mkdtemp(prefix="dr-api-")
        self.ctx = DRContext(self.home)
        lh = Lakehouse(self.ctx.lakehouse_root)
        lh.init()
        lh.create_table(
            "orders",
            {"columns": {"order_id": "string", "amount": "number"}},
            ["order_id"],
        )
        lh.ingest_rows("orders", [
            {"order_id": "O1", "amount": 10}, {"order_id": "O2", "amount": 20}
        ])
        self.store = BackupStore(self.ctx)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(DRServer(self.ctx)))
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.home, ignore_errors=True)

    def call(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_overview_backups_and_static(self) -> None:
        status, overview = self.call("GET", "/api/overview")
        self.assertEqual(status, 200)
        self.assertEqual(overview["lakehouse"]["tables"], ["orders"])

        status, created = self.call("POST", "/api/backups", {"kind": "full", "note": "api"})
        self.assertEqual(status, 201)
        self.assertEqual(created["gate_status"], "passed")
        bid = created["backup_id"]

        status, listing = self.call("GET", "/api/backups")
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["backups"]), 1)

        status, manifest = self.call("GET", f"/api/backups/{bid}")
        self.assertEqual(status, 200)
        self.assertTrue(manifest["files"])

        status, verify = self.call("GET", f"/api/backups/{bid}/verify")
        self.assertEqual(status, 200)
        self.assertTrue(verify["ok"])

        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/", timeout=5) as resp:
            self.assertEqual(resp.status, 200)
            self.assertIn("text/html", resp.headers["Content-Type"])

    def test_recovery_lifecycle_through_http(self) -> None:
        _, created = self.call("POST", "/api/backups", {"kind": "full"})
        bid = created["backup_id"]

        status, journal = self.call("POST", "/api/recoveries", {"backup_id": bid})
        self.assertEqual(status, 201)
        rid = journal["recovery_id"]
        self.assertEqual(journal["status"], "created")

        # interrupt after verify, then resume
        status, interrupted = self.call(
            "POST", f"/api/recoveries/{rid}/run", {"fail_after_stage": "verify"})
        self.assertEqual(status, 200)
        self.assertEqual(interrupted["status"], "interrupted")

        status, completed = self.call("POST", f"/api/recoveries/{rid}/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(completed["status"], "completed")
        for stage in completed["stage_order"]:
            self.assertEqual(completed["stages"][stage]["status"], "completed")

        status, diff = self.call("GET", f"/api/recoveries/{rid}/diff")
        self.assertEqual(status, 200)
        self.assertIn("row_counts", diff)

        status, gaps = self.call("GET", f"/api/gaps?recovery_id={rid}")
        self.assertEqual(status, 200)
        self.assertIn("cdc_mirror", gaps)

    def test_corruption_drill_through_http(self) -> None:
        _, created = self.call("POST", "/api/backups", {"kind": "full"})
        bid = created["backup_id"]
        _, manifest = self.call("GET", f"/api/backups/{bid}")
        victim = manifest["files"][0]["path"]

        status, tampered = self.call(
            "POST", "/api/drills/corrupt-file",
            {"backup_id": bid, "path": victim})
        self.assertEqual(status, 201)
        self.assertNotEqual(tampered["expected_sha256"], tampered["actual_sha256"])

        _, verify = self.call("GET", f"/api/backups/{bid}/verify")
        self.assertFalse(verify["ok"])

        _, journal = self.call("POST", "/api/recoveries", {"backup_id": bid})
        _, result = self.call("POST", f"/api/recoveries/{journal['recovery_id']}/run", {})
        self.assertEqual(len(result["quarantine"]), 1)
        self.assertEqual(result["quarantine"][0]["path"], victim)

    def test_unknown_route_and_bad_body(self) -> None:
        status, payload = self.call("GET", "/api/nope")
        self.assertEqual(status, 404)
        self.assertIn("error", payload)

        req = urllib.request.Request(
            f"http://127.0.0.1:{self.port}/api/backups",
            data=b"not-json", method="POST",
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=5)
            self.fail("expected HTTP error")
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 400)


if __name__ == "__main__":
    unittest.main()
