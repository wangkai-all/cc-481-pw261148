"""Stdlib-only HTTP server: JSON API + the single-page web console.

API (all JSON, POST bodies are JSON objects):

GET  /api/overview
GET  /api/backups
GET  /api/backups/<id>
GET  /api/backups/<id>/verify
GET  /api/file-versions
GET  /api/chain
POST /api/backups            {kind, note}
GET  /api/recoveries
GET  /api/recoveries/<id>
POST /api/recoveries         {backup_id?, target_time?, target_seq?, note?}
POST /api/recoveries/<id>/run            {fail_after_stage?}
GET  /api/recoveries/<id>/diff
GET  /api/gaps              ?recovery_id=
GET  /api/lakehouse
POST /api/drills/corrupt-file   {backup_id, path}
POST /api/drills/truncate-cdc   {keep_events}
GET  /                       static console
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .backup import BackupStore
from .config import DRContext
from .lakehouse import Lakehouse
from .recovery import STAGE_LABELS, STAGE_ORDER, RecoveryManager, RecoveryError
from .state import ensure_dir, read_json, utcnow_iso

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

_MIME = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".svg": "image/svg+xml",
}


class DRServer:
    def __init__(self, ctx: DRContext):
        self.ctx = ctx
        self.store = BackupStore(ctx)
        self.recovery = RecoveryManager(ctx)
        self._lock = threading.Lock()  # serialize mutating drill operations


def _make_handler(server: DRServer):
    class Handler(BaseHTTPRequestHandler):
        server_version = "LakehouseDR/1.0"

        def log_message(self, fmt, *args):  # quieter console
            pass

        # ------------------------------------------------------------ helpers
        def _send(self, payload, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _error(self, message: str, status: int = 400) -> None:
            self._send({"error": message, "type": "error"}, status)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON body: {exc}") from exc
            if not isinstance(data, dict):
                raise ValueError("JSON body must be an object")
            return data

        def _static(self, rel: str) -> None:
            rel = rel.lstrip("/") or "index.html"
            safe = os.path.normpath(os.path.join(STATIC_DIR, rel))
            if not safe.startswith(STATIC_DIR + os.sep) and safe != STATIC_DIR:
                return self._error("forbidden", 403)
            if not os.path.isfile(safe):
                return self._error("not found", 404)
            with open(safe, "rb") as fh:
                body = fh.read()
            ext = os.path.splitext(safe)[1]
            self.send_response(200)
            self.send_header("Content-Type", _MIME.get(ext, "application/octet-stream"))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        # --------------------------------------------------------------- GET
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path, query = parsed.path, parse_qs(parsed.query)
            try:
                if path.startswith("/api/"):
                    self._route_get(path, query)
                else:
                    self._static(path)
            except RecoveryError as exc:
                self._error(str(exc), 409)
            except FileNotFoundError as exc:
                self._error(f"not found: {exc}", 404)
            except Exception as exc:  # noqa: BLE001
                self._error(f"{type(exc).__name__}: {exc}", 500)

        def _route_get(self, path: str, query: parse_qs) -> None:
            s, r, ctx = server.store, server.recovery, server.ctx
            if path == "/api/overview":
                backups = s.list_backups()
                journals = r.list_recoveries()
                lh = Lakehouse(ctx.lakehouse_root)
                return self._send({
                    "generated_at": utcnow_iso(),
                    "lakehouse": {
                        "root": ctx.lakehouse_root,
                        "tables": lh.list_tables(),
                        "cdc_watermark": lh.cdc_watermark(),
                    },
                    "backups": {
                        "count": len(backups),
                        "latest": backups[-1] if backups else None,
                        "gate_failed": [b["backup_id"] for b in backups
                                        if b["gate_status"] == "failed"],
                    },
                    "recoveries": {
                        "total": len(journals),
                        "completed": sum(1 for j in journals if j["status"] == "completed"),
                        "interrupted": sum(1 for j in journals
                                           if j["status"] in ("interrupted", "failed", "running")),
                    },
                })
            if path == "/api/backups":
                return self._send({"backups": s.list_backups()})
            if path == "/api/file-versions":
                return self._send(s.versions())
            if path == "/api/chain":
                return self._send({"chain": s.chain()})
            if path == "/api/recoveries":
                return self._send({"recoveries": r.list_recoveries()})
            if path == "/api/gaps":
                rid = query.get("recovery_id", [None])[0]
                return self._send(r.gap_analysis(rid))
            if path == "/api/lakehouse":
                return self._send(self._lakehouse_payload())

            parts = [p for p in path.split("/") if p]
            # /api/backups/<id>[/verify]
            if len(parts) >= 3 and parts[1] == "backups":
                bid = parts[2]
                if len(parts) == 3:
                    return self._send(s.get_manifest(bid))
                if len(parts) == 4 and parts[3] == "verify":
                    return self._send(s.verify_backup(bid))
            # /api/recoveries/<id>[/diff]
            if len(parts) >= 3 and parts[1] == "recoveries":
                rid = parts[2]
                journal = r.get_journal(rid)
                if len(parts) == 3:
                    return self._send(self._recovery_payload(journal))
                if len(parts) == 4 and parts[3] == "diff":
                    return self._send(r.diff_against_backup(rid))
            return self._error(f"unknown route: {path}", 404)

        def _lakehouse_payload(self) -> dict:
            lh = Lakehouse(server.ctx.lakehouse_root)
            tables = {}
            for name in lh.list_tables():
                meta = lh.table_meta(name)
                tables[name] = {
                    "primary_key": meta["primary_key"],
                    "schema": meta["schema"],
                    "row_count": meta["row_count"],
                    "current_files": meta["current_files"],
                    "snapshots": meta["snapshots"],
                    "sample_rows": lh.get_rows(name)[:10],
                }
            return {
                "root": server.ctx.lakehouse_root,
                "cdc_watermark": lh.cdc_watermark(),
                "tables": tables,
            }

        def _recovery_payload(self, journal: dict) -> dict:
            rid = journal["recovery_id"]
            impact_path = os.path.join(server.ctx.recovery_dir(rid), "impact.json")
            impact = read_json(impact_path) if os.path.exists(impact_path) else None
            return {
                **journal,
                "stage_order": STAGE_ORDER,
                "stage_labels": STAGE_LABELS,
                "impact_report": impact,
            }

        # -------------------------------------------------------------- POST
        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path
            try:
                body = self._body()
            except ValueError as exc:
                return self._error(str(exc))
            try:
                with server._lock:
                    self._route_post(path, body)
            except RecoveryError as exc:
                self._error(str(exc), 409)
            except FileNotFoundError as exc:
                self._error(f"not found: {exc}", 404)
            except Exception as exc:  # noqa: BLE001
                self._error(f"{type(exc).__name__}: {exc}", 500)

        def _route_post(self, path: str, body: dict) -> None:
            s, r = server.store, server.recovery
            parts = [p for p in path.split("/") if p]

            if path == "/api/backups":
                summary = s.create_backup(
                    kind=body.get("kind", "full"),
                    note=body.get("note", ""),
                )
                return self._send(summary.to_dict(), 201)

            if path == "/api/recoveries":
                journal = r.create_recovery(
                    backup_id=body.get("backup_id"),
                    target_time=body.get("target_time"),
                    target_seq=body.get("target_seq"),
                    note=body.get("note", ""),
                )
                return self._send(journal, 201)

            if path == "/api/drills/corrupt-file":
                return self._send(
                    s.tamper_blob(body["backup_id"], body["path"]), 201)
            if path == "/api/drills/truncate-cdc":
                return self._send(
                    s.truncate_cdc_mirror(int(body.get("keep_events", 0))), 201)

            # /api/recoveries/<id>/run
            if len(parts) == 4 and parts[1] == "recoveries" and parts[3] == "run":
                journal = r.run_recovery(
                    parts[2],
                    fail_after_stage=body.get("fail_after_stage"),
                )
                return self._send(self._recovery_payload(journal))

            return self._error(f"unknown route: {path}", 404)

    return Handler


def serve(ctx: DRContext, host: str = "127.0.0.1", port: int = 8000) -> None:
    ensure_dir(ctx.home)
    httpd = ThreadingHTTPServer((host, port), _make_handler(DRServer(ctx)))
    print(f"lakehouse-dr console: http://{host}:{port}  (home={ctx.home})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nshutting down")
    finally:
        httpd.server_close()
