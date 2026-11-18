#!/usr/bin/env python3
"""End-to-end disaster-recovery drill demonstration.

Scenarios exercised, in order:

  1. build a small lakehouse (2 tables, several batches, updates, a delete)
  2. take full + incremental backups; show file versions and CDC positions
  3. more production activity, then a point-in-time recovery with CDC replay
  4. inject corruption into a backup blob -> quarantine + impact report
  5. interrupt a recovery mid-pipeline and resume it from the journal
  6. repeat the drill into an isolated directory (never touching production)
  7. truncate the CDC mirror and show gap analysis + diff

Usage:  python3 scripts/demo.py [home_dir]
"""
from __future__ import annotations

import json
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lakehouse_dr.backup import BackupStore  # noqa: E402
from lakehouse_dr.config import DRContext  # noqa: E402
from lakehouse_dr.lakehouse import Lakehouse  # noqa: E402
from lakehouse_dr.recovery import STAGE_LABELS, RecoveryManager  # noqa: E402
from lakehouse_dr.state import is_production_tree  # noqa: E402


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def show(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def main(home: str) -> None:
    if os.path.exists(home):
        shutil.rmtree(home)
    ctx = DRContext(home)
    lh = Lakehouse(ctx.lakehouse_root)
    lh.init()
    store = BackupStore(ctx)
    mgr = RecoveryManager(ctx)

    # ------------------------------------------------------------- 1. data
    banner("1. 构建模拟湖仓（orders / customers，多批次 + 更新 + 删除）")
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
    lh.ingest_rows("orders", [
        {"order_id": f"O{1000+i}", "customer": f"cust-{i % 5}",
         "amount": 100 + i * 7, "status": "new"} for i in range(6)
    ])
    lh.ingest_rows("customers", [
        {"cust_id": f"cust-{i}", "name": f"Customer {i}",
         "tier": "gold" if i < 2 else "silver"} for i in range(5)
    ])
    lh.ingest_rows("orders", [
        {"order_id": "O1002", "customer": "cust-2", "amount": 250, "status": "paid"},
        {"order_id": "O1009", "customer": "cust-1", "amount": 99, "status": "new"},
    ])
    lh.delete_rows("orders", [{"order_id": "O1004"}])
    print(f"生产 CDC 水位: seq={lh.cdc_watermark()}")
    print("orders 行数:", lh.count_rows("orders"), "| customers 行数:", lh.count_rows("customers"))

    # ---------------------------------------------------------- 2. backups
    banner("2. 全量 + 增量备份（文件版本 / 元数据快照 / CDC 位点 / 质量门禁）")
    b1 = store.create_backup(kind="full", note="基线全量")
    print("全量备份:", b1.to_dict())
    lh.ingest_rows("orders", [
        {"order_id": "O1010", "customer": "cust-3", "amount": 175, "status": "new"}
    ])
    b2 = store.create_backup(kind="incremental", note="增量-新订单")
    print("增量备份:", b2.to_dict())
    print("\n文件版本注册表（节选）:")
    for path, info in list(store.versions()["files"].items())[:4]:
        print(f"  {path}: " + ", ".join(
            f"v{v['version']}={v['sha256'][:10]}…({v['first_seen_in']})"
            for v in info["versions"]))

    # --------------------------------------------------- 3. PITR + CDC replay
    banner("3. 时间点恢复：选 b1 基线，回放 CDC 到当前水位")
    lh.ingest_rows("orders", [
        {"order_id": "O1011", "customer": "cust-0", "amount": 60, "status": "new"},
        {"order_id": "O1012", "customer": "cust-4", "amount": 88, "status": "new"},
    ])
    store._sync_cdc_mirror(lh)
    target_seq = lh.cdc_watermark()
    j1 = mgr.create_recovery(backup_id=b1.backup_id, target_seq=target_seq,
                             note="PITR 全量+CDC 回放")
    r1 = mgr.run_recovery(j1["recovery_id"])
    print("恢复状态:", r1["status"])
    for s, st in r1["stages"].items():
        print(f"  {STAGE_LABELS[s]:<28} {st['status']:<10} {st['detail']}")
    rec1 = Lakehouse(r1["target_root"])
    print("恢复后 orders 行数:", rec1.count_rows("orders"),
          "| CDC 水位:", rec1.cdc_watermark())
    print("生产树未被触碰:", is_production_tree(ctx.lakehouse_root),
          "| 恢复目录带隔离标记:", os.path.basename(r1["target_root"]))

    # ------------------------------------------------- 4. corruption drill
    banner("4. 损坏文件演练：篡改备份 blob，恢复时隔离并输出影响报告")
    victim = store.get_manifest(b2.backup_id)["files"][0]["path"]
    store.tamper_blob(b2.backup_id, victim)
    verify = store.verify_backup(b2.backup_id)
    print("备份校验 ok =", verify["ok"], "| 问题:",
          [p["path"] for p in verify["problems"]])
    j2 = mgr.create_recovery(backup_id=b2.backup_id, note="损坏隔离演练")
    r2 = mgr.run_recovery(j2["recovery_id"])
    print("隔离文件:")
    show(r2["quarantine"])
    impact_path = os.path.join(ctx.recovery_dir(j2["recovery_id"]), "impact.json")
    with open(impact_path, encoding="utf-8") as fh:
        print("影响报告:")
        show(json.load(fh))

    # -------------------------------------------- 5. interruption + resume
    banner("5. 中断恢复演练：在元数据阶段后中断，再从 journal 续跑")
    j3 = mgr.create_recovery(backup_id=b1.backup_id, target_seq=target_seq,
                             note="中断-续跑演练")
    interrupted = mgr.run_recovery(j3["recovery_id"], fail_after_stage="metadata")
    print("中断后状态:", interrupted["status"],
          "| 已完成阶段:", [s for s, st in interrupted["stages"].items()
                          if st["status"] == "completed"])
    resumed = mgr.run_recovery(j3["recovery_id"])
    print("续跑后状态:", resumed["status"],
          "| 各阶段:", {s: st["status"] for s, st in resumed["stages"].items()})

    # ------------------------------------------------- 6. repeat isolation
    banner("6. 重复演练：每次恢复都是独立目录，生产快照绝不被覆盖")
    j4 = mgr.create_recovery(backup_id=b1.backup_id, note="重复演练 A")
    mgr.run_recovery(j4["recovery_id"])
    j5 = mgr.create_recovery(backup_id=b1.backup_id, note="重复演练 B")
    mgr.run_recovery(j5["recovery_id"])
    print("恢复演练总数:", len(mgr.list_recoveries()))
    print("生产 catalog 仍在:", os.path.exists(ctx.lakehouse_catalog))
    try:
        forged = mgr.create_recovery(note="恶意目标")
        os.makedirs(forged["target_root"], exist_ok=True)
        with open(os.path.join(forged["target_root"], ".lakehouse-prod-marker"), "w") as fh:
            fh.write("fake\n")
        mgr.run_recovery(forged["recovery_id"])
        print("ERROR: 生产保护未生效!")
    except Exception as exc:  # noqa: BLE001
        print("生产保护生效，拒绝恢复:", type(exc).__name__, str(exc)[:80])

    # -------------------------------------------------------- 7. CDC gaps
    banner("7. CDC 缺口 + 差异分析")
    store.truncate_cdc_mirror(keep_first_n=b1.cdc_position)
    gaps = mgr.gap_analysis(j1["recovery_id"])
    show({
        "cdc_mirror": gaps["cdc_mirror"],
        "recovery_gap_count": len(gaps["recovery_gaps"]),
        "file_version_gap_count": len(gaps["file_version_gaps"]),
    })
    diff = mgr.diff_against_backup(j1["recovery_id"])
    show({
        "missing": len(diff["missing_files"]),
        "corrupted": len(diff["corrupted_files"]),
        "added_by_cdc": len(diff["added_files"]),
        "row_counts": diff["row_counts"],
        "cdc": diff["cdc"],
    })
    banner("演练全部完成")
    print(f"环境目录: {home}")
    print(f"启动页面: python3 -m lakehouse_dr.cli --home {home} serve")


if __name__ == "__main__":
    home_arg = sys.argv[1] if len(sys.argv) > 1 else "./dr-home-demo"
    main(os.path.abspath(home_arg))
