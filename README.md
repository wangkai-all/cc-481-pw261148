# 本地湖仓灾备演练工具（Lakehouse DR Drill）

一个零第三方依赖（纯 Python 标准库）的本地湖仓灾备演练平台：记录**文件版本、元数据快照、CDC 位点和质量门禁结果**，支持按时间点恢复——**先重建元数据，再按 CDC 位点回放**；遇到损坏文件自动**隔离并报告影响**；恢复**绝不覆盖现有生产快照**。页面展示备份清单、恢复阶段、缺口与差异，并支持中断恢复与重复演练。

## 能力与需求对照

| 需求 | 实现 |
| --- | --- |
| 记录文件版本 | 内容寻址 blob 存储（相同内容只存一份）+ 按路径的版本注册表 `file-versions.json`（版本号、哈希、首次出现的备份） |
| 元数据快照 | 每次备份保存 catalog 全量副本 + 每个表的 snapshot 文件，均带 SHA-256 |
| CDC 位点 | 全局有序 CDC 日志（seq 1..N），备份记录 `position`，并在备份侧维护镜像与持久化高水位 |
| 质量门禁结果 | 5 道门禁：JSONL 可解析、文件可读哈希、CDC 连续性、Schema 一致性、行数对账；结果写入清单，失败也保留备份 |
| 选择时间点恢复 | 按备份 ID、目标时间（`backup_at_or_before`）或目标 CDC seq 创建恢复 |
| 先重建元数据再回放 | 固定 5 阶段流水线：① 校验 → ② 元数据重建 → ③ 文件恢复 → ④ CDC 回放 → ⑤ 门禁与差异验证 |
| 损坏文件隔离 + 影响 | 损坏 blob 复制到 `quarantine/`，从恢复 catalog 摘除，输出 `impact.json`（受影响表、不可用行数、受影响 CDC 事件） |
| 不能覆盖生产快照 | 生产树带 `.lakehouse-prod-marker`；恢复目标固定在 `recovery_area/` 并带独立标记；路径与标记双重硬性拒绝（有测试覆盖） |
| 备份清单页面 | 备份表（ID/时间/类型/位点/门禁/父备份）+ 清单详情（版本、元数据、门禁明细） |
| 恢复阶段页面 | 5 阶段时间线（状态/耗时/详情/错误）+ 隔离区 + 影响报告 + 恢复级缺口 |
| 缺口页面 | CDC 镜像空洞与截断尾部、备份链缺口、文件版本演进、恢复级缺口 |
| 差异页面 | 丢失/损坏/变化/回放新增文件、行数对账、CDC 位点对比 |
| 中断恢复 | 每阶段幂等，journal 持久化；`fail_after_stage` 模拟中断，续跑跳过已完成阶段 |
| 重复演练 | 每次恢复独立目录、独立 journal；已完成的恢复不可变，重跑为 no-op |

## 目录结构

```
dr-home/
├── lakehouse/                 # 模拟生产湖仓
│   ├── .lakehouse-prod-marker
│   ├── catalog.json           # 表 schema、文件清单、快照、行数
│   ├── cdc/cdc-log.jsonl      # 全局有序变更事件
│   └── tables/<t>/data/*.jsonl, snapshots/*.json
├── backup_store/              # 不可变备份区（恢复只读，永不修改）
│   ├── index.json             # 备份清单
│   ├── file-versions.json     # 路径 -> 版本历史
│   ├── blobs/<sha前2位>/<sha> # 内容寻址
│   ├── cdc/cdc-log.jsonl      # CDC 镜像 + state.json 高水位
│   └── backups/<bk>/manifest.json
└── recovery_area/<rc>/        # 每次演练一个目录
    ├── journal.json           # 阶段状态、缺口、隔离清单（断点续跑依据）
    ├── baseline/              # 元数据+文件恢复后的基线（回放可幂等重置）
    ├── recovered/             # 恢复目标（带 .recovery-target-marker）
    ├── quarantine/            # 损坏文件隔离副本
    └── impact.json            # 影响分析报告
```

## 快速开始

```bash
# 1. 初始化环境并造数（2 张表、多批次、更新、删除，产生 CDC）
python3 -m lakehouse_dr.cli --home ./dr-home init
python3 -m lakehouse_dr.cli --home ./dr-home seed

# 2. 全量 / 增量备份
python3 -m lakehouse_dr.cli --home ./dr-home backup --kind full --note "基线"
python3 -m lakehouse_dr.cli --home ./dr-home backup --kind incremental --note "增量"
python3 -m lakehouse_dr.cli --home ./dr-home backups
python3 -m lakehouse_dr.cli --home ./dr-home backup-verify <bk-id>
python3 -m lakehouse_dr.cli --home ./dr-home versions   # 文件版本注册表
python3 -m lakehouse_dr.cli --home ./dr-home chain      # 备份链与每链变化

# 3. 时间点恢复（目标 seq 可选，默认备份位点）
python3 -m lakehouse_dr.cli --home ./dr-home recovery-create --target-seq 12
python3 -m lakehouse_dr.cli --home ./dr-home recovery-run <rc-id>
python3 -m lakehouse_dr.cli --home ./dr-home recovery-show <rc-id>
python3 -m lakehouse_dr.cli --home ./dr-home recovery-diff <rc-id>

# 4. 演练：注入损坏 / 中断续跑 / CDC 截断
python3 -m lakehouse_dr.cli --home ./dr-home drill-corrupt <bk-id> tables/orders/data/00001.jsonl
python3 -m lakehouse_dr.cli --home ./dr-home recovery-run <rc-id> --fail-after metadata
python3 -m lakehouse_dr.cli --home ./dr-home recovery-run <rc-id>          # 再次执行即续跑
python3 -m lakehouse_dr.cli --home ./dr-home drill-truncate-cdc --keep-events 5
python3 -m lakehouse_dr.cli --home ./dr-home gaps --recovery-id <rc-id>

# 5. 一键端到端演示（7 个场景）
python3 scripts/demo.py ./dr-home-demo

# 6. Web 控制台
python3 -m lakehouse_dr.cli --home ./dr-home serve --port 8000
# 浏览器打开 http://127.0.0.1:8000
```

安装为命令行工具（可选）：`pip install -e .` 后可直接使用 `dr-cli`。

## Web 控制台

五个页签：

- **备份清单**：创建全量/增量备份、查看清单详情（文件版本、元数据快照、CDC 位点、门禁明细）、校验、一键注入损坏；
- **恢复演练**：选备份/时间点/目标 seq 创建恢复，阶段流水线实时刷新，支持运行/续跑、中断模拟、隔离区与影响报告；
- **缺口分析**：CDC 覆盖（空洞 + 截断尾部）、备份链缺口、文件版本演进、恢复级缺口，可绑定某次恢复；
- **差异对比**：恢复树相对备份真相的丢失/损坏/变化/回放新增文件与行数对账；
- **生产湖仓**：表结构、文件、快照、样例行与 CDC 水位。

页面每 3 秒自动刷新（可关闭），所有操作都经过同一套后端引擎与安全校验。

## HTTP API（摘要）

```
GET  /api/overview | /api/backups | /api/backups/<id> | /api/backups/<id>/verify
GET  /api/file-versions | /api/chain | /api/recoveries | /api/recoveries/<id>
GET  /api/recoveries/<id>/diff | /api/gaps?recovery_id= | /api/lakehouse
POST /api/backups                       {kind, note}
POST /api/recoveries                    {backup_id?, target_time?, target_seq?, note?}
POST /api/recoveries/<id>/run           {fail_after_stage?}
POST /api/drills/corrupt-file           {backup_id, path}
POST /api/drills/truncate-cdc           {keep_events}
```

## 测试

```bash
python3 -m unittest lakehouse_dr.tests.test_dr -v
```

15 个用例覆盖：湖仓折叠视图与 CDC 水位、备份四要素与去重、损坏湖仓下门禁失败但备份保留、完整恢复与门禁、时间点（seq 与时间）、损坏 blob 隔离与影响报告、CDC 缺口、中断续跑、重复演练隔离、生产覆盖保护、备份链与缺口分析。

## 设计说明与边界

- 模拟湖仓用 JSONL 追加批次 + 主键折叠（last-writer-wins），删除用墓碑行；接口刻意贴近真实湖仓（catalog、snapshot、CDC log、数据文件），便于替换为 Iceberg/Hudi 等真实实现。
- 备份 blob 内容寻址、不可变；演练篡改只改 blob 副本所在存储，恢复侧永远只读备份区。
- CDC 回放保留原始 seq（重写事件号会让水位失真），并在回放前把目标树重置到文件恢复基线，保证阶段重试幂等。
- 质量门禁失败不会阻断备份——灾备场景下"带病"快照也必须可留存、可追溯。
