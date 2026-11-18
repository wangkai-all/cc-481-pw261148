#!/usr/bin/env python3
"""Command line interface for the local lakehouse DR drill tool.

Run ``python -m lakehouse_dr.cli --help`` (or the ``dr-cli`` console script)
for the full command list. Everything lives under one --home directory so
multiple independent drill environments can coexist.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

from .backup import BackupStore
from .config import DRContext
from .gates import run_quality_gates
from .lakehouse import Lakehouse
from .recovery import STAGE_LABELS, RecoveryManager
from .web import serve


def _ctx(home: str) -> DRContext:
    return DRContext(os.path.abspath(home))


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


# --------------------------------------------------------------- environment
def cmd_init(args) -> None:
    ctx = _ctx(args.home)
    lh = Lakehouse(ctx.lakehouse_root)
    lh.init()
    BackupStore(ctx)
    RecoveryManager(ctx)
    _print({"status": "initialized", "home": ctx.home,
            "lakehouse": ctx.lakehouse_root, "backup_store": ctx.backup_root,
            "recovery_area": ctx.recovery_root})


def cmd_seed(args) -> None:
    ctx = _ctx(args.home)
    lh = Lakehouse(ctx.lakehouse_root)
    lh.init()
    lh.create_table(
        "orders",
        {"columns": {"order_id": "string", "customer": "string",
                     "amount": "number", "status": "string"}},
        ["order_id"],
    )
    lh.create_table(
        "customers",
        {"columns": {"cust_id": "string", "name": "string", "tier": "string"}},
        ["cust_id"],
    )
    result = []
    result.append(lh.ingest_rows("orders", [
        {"order_id": f"O{1000+i}", "customer": f"cust-{i % 5}",
         "amount": 100 + i * 7, "status": "new"} for i in range(6)
    ]))
    result.append(lh.ingest_rows("customers", [
        {"cust_id": f"cust-{i}", "name": f"Customer {i}", "tier": "gold" if i < 2 else "silver"}
        for i in range(5)
    ]))
    result.append(lh.ingest_rows("orders", [
        {"order_id": "O1002", "customer": "cust-2", "amount": 250, "status": "paid"},
        {"order_id": "O1009", "customer": "cust-1", "amount": 99, "status": "new"},
    ]))
    result.append(lh.delete_rows("orders", [{"order_id": "O1004"}]))
    _print({"status": "seeded", "batches": result,
            "cdc_watermark": lh.cdc_watermark(),
            "tables": {t: lh.count_rows(t) for t in lh.list_tables()}})


# ------------------------------------------------------------------ lakehouse
def cmd_lakehouse(args) -> None:
    ctx = _ctx(args.home)
    lh = Lakehouse(ctx.lakehouse_root)
    payload = {
        "cdc_watermark": lh.cdc_watermark(),
        "tables": {
            t: {"row_count": lh.count_rows(t),
                "files": lh.table_meta(t)["current_files"],
                "rows": lh.get_rows(t) if args.rows else None}
            for t in lh.list_tables()
        },
    }
    _print(payload)


def cmd_gates(args) -> None:
    lh = Lakehouse(_ctx(args.home).lakehouse_root)
    _print(run_quality_gates(lh))


# -------------------------------------------------------------------- backup
def cmd_backup_create(args) -> None:
    store = BackupStore(_ctx(args.home))
    _print(store.create_backup(kind=args.kind, note=args.note).to_dict())


def cmd_backup_list(args) -> None:
    store = BackupStore(_ctx(args.home))
    _print({"backups": store.list_backups()})


def cmd_backup_show(args) -> None:
    _print(BackupStore(_ctx(args.home)).get_manifest(args.backup_id))


def cmd_backup_verify(args) -> None:
    _print(BackupStore(_ctx(args.home)).verify_backup(args.backup_id))


def cmd_versions(args) -> None:
    _print(BackupStore(_ctx(args.home)).versions())


def cmd_chain(args) -> None:
    _print({"chain": BackupStore(_ctx(args.home)).chain()})


# ------------------------------------------------------------------ recovery
def cmd_recovery_create(args) -> None:
    mgr = RecoveryManager(_ctx(args.home))
    _print(mgr.create_recovery(
        backup_id=args.backup_id, target_time=args.target_time,
        target_seq=args.target_seq, note=args.note))


def cmd_recovery_run(args) -> None:
    mgr = RecoveryManager(_ctx(args.home))
    journal = mgr.run_recovery(args.recovery_id, fail_after_stage=args.fail_after)
    _print(_journal_view(mgr, journal))


def cmd_recovery_list(args) -> None:
    mgr = RecoveryManager(_ctx(args.home))
    rows = []
    for j in mgr.list_recoveries():
        rows.append({
            "recovery_id": j["recovery_id"], "status": j["status"],
            "base_backup_id": j["base_backup_id"],
            "base_cdc_position": j["base_cdc_position"],
            "target_cdc_seq": j["target_cdc_seq"],
            "current_stage": j["current_stage"],
            "quarantined": len(j["quarantine"]),
            "created_at": j["created_at"],
        })
    _print({"recoveries": rows})


def cmd_recovery_show(args) -> None:
    mgr = RecoveryManager(_ctx(args.home))
    _print(_journal_view(mgr, mgr.get_journal(args.recovery_id)))


def cmd_recovery_diff(args) -> None:
    mgr = RecoveryManager(_ctx(args.home))
    _print(mgr.diff_against_backup(args.recovery_id))


def cmd_gaps(args) -> None:
    mgr = RecoveryManager(_ctx(args.home))
    _print(mgr.gap_analysis(args.recovery_id))


def _journal_view(mgr: RecoveryManager, journal: dict) -> dict:
    rid = journal["recovery_id"]
    impact_path = os.path.join(mgr.ctx.recovery_dir(rid), "impact.json")
    impact = None
    if os.path.exists(impact_path):
        with open(impact_path, encoding="utf-8") as fh:
            impact = json.load(fh)
    return {**journal, "stage_labels": STAGE_LABELS, "impact_report": impact}


# --------------------------------------------------------------------- drills
def cmd_drill_corrupt(args) -> None:
    store = BackupStore(_ctx(args.home))
    _print(store.tamper_blob(args.backup_id, args.path))


def cmd_drill_truncate_cdc(args) -> None:
    store = BackupStore(_ctx(args.home))
    _print(store.truncate_cdc_mirror(args.keep_events))


def cmd_serve(args) -> None:
    serve(_ctx(args.home), host=args.host, port=args.port)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dr-cli",
                                description="Local lakehouse disaster-recovery drill tool")
    p.add_argument("--home", default=os.environ.get("DR_HOME", "./dr-home"),
                   help="environment directory (env DR_HOME, default ./dr-home)")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("init", help="initialize lakehouse + backup store + recovery area")
    sp.set_defaults(func=cmd_init)

    sp = sub.add_parser("seed", help="create demo tables and write batches + CDC events")
    sp.set_defaults(func=cmd_seed)

    sp = sub.add_parser("lakehouse", help="inspect the production lakehouse")
    sp.add_argument("--rows", action="store_true", help="include live rows")
    sp.set_defaults(func=cmd_lakehouse)

    sp = sub.add_parser("gates", help="run quality gates against the lakehouse")
    sp.set_defaults(func=cmd_gates)

    sp = sub.add_parser("backup", help="take a backup")
    sp.add_argument("--kind", choices=["full", "incremental"], default="full")
    sp.add_argument("--note", default="")
    sp.set_defaults(func=cmd_backup_create)

    sp = sub.add_parser("backups", help="list backups")
    sp.set_defaults(func=cmd_backup_list)

    sp = sub.add_parser("backup-show", help="show one backup manifest")
    sp.add_argument("backup_id")
    sp.set_defaults(func=cmd_backup_show)

    sp = sub.add_parser("backup-verify", help="re-hash blobs of one backup")
    sp.add_argument("backup_id")
    sp.set_defaults(func=cmd_backup_verify)

    sp = sub.add_parser("versions", help="show per-path file version registry")
    sp.set_defaults(func=cmd_versions)

    sp = sub.add_parser("chain", help="backup chain with change deltas")
    sp.set_defaults(func=cmd_chain)

    sp = sub.add_parser("recovery-create", help="create a point-in-time recovery run")
    sp.add_argument("--backup-id")
    sp.add_argument("--target-time", help="ISO timestamp; newest backup at/before it")
    sp.add_argument("--target-seq", type=int, help="CDC seq to replay up to")
    sp.add_argument("--note", default="")
    sp.set_defaults(func=cmd_recovery_create)

    sp = sub.add_parser("recovery-run", help="run or resume a recovery")
    sp.add_argument("recovery_id")
    sp.add_argument("--fail-after", choices=list(STAGE_LABELS),
                    help="drill: interrupt after this stage (for resume testing)")
    sp.set_defaults(func=cmd_recovery_run)

    sp = sub.add_parser("recoveries", help="list recovery runs")
    sp.set_defaults(func=cmd_recovery_list)

    sp = sub.add_parser("recovery-show", help="show one recovery journal")
    sp.add_argument("recovery_id")
    sp.set_defaults(func=cmd_recovery_show)

    sp = sub.add_parser("recovery-diff", help="diff recovered tree vs backup truth")
    sp.add_argument("recovery_id")
    sp.set_defaults(func=cmd_recovery_diff)

    sp = sub.add_parser("gaps", help="gap analysis (CDC, chain, versions, recovery)")
    sp.add_argument("--recovery-id")
    sp.set_defaults(func=cmd_gaps)

    sp = sub.add_parser("drill-corrupt", help="corrupt one backup blob (drill)")
    sp.add_argument("backup_id")
    sp.add_argument("path")
    sp.set_defaults(func=cmd_drill_corrupt)

    sp = sub.add_parser("drill-truncate-cdc", help="truncate CDC mirror (drill)")
    sp.add_argument("--keep-events", type=int, required=True)
    sp.set_defaults(func=cmd_drill_truncate_cdc)

    sp = sub.add_parser("serve", help="start the web console")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8000)
    sp.set_defaults(func=cmd_serve)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except Exception as exc:  # noqa: BLE001 - clean CLI error surface
        print(f"error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
