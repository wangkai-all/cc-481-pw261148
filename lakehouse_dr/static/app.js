/* Lakehouse DR drill console — vanilla JS, no build step. */
"use strict";

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => [...document.querySelectorAll(sel)];
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const badge = (status) => `<span class="badge ${esc(status)}">${esc(status)}</span>`;

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const data = await res.json();
  if (!res.ok || data.type === "error") throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}

let toastTimer = null;
function toast(msg, isErr = false) {
  const el = $("#toast");
  el.textContent = msg;
  el.className = `toast ${isErr ? "err" : ""}`;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.add("hidden"), 4000);
  el.classList.remove("hidden");
}

/* ------------------------------------------------------------- tabs */
$$(".tab").forEach((t) => t.addEventListener("click", () => {
  $$(".tab").forEach((x) => x.classList.remove("active"));
  $$(".panel").forEach((x) => x.classList.remove("active"));
  t.classList.add("active");
  $(`#tab-${t.dataset.tab}`).classList.add("active");
  refreshAll();
}));

/* ------------------------------------------------------------- overview */
async function loadOverview() {
  const o = await api("/api/overview");
  const rc = o.recoveries;
  $("#overview").innerHTML = `
    <div class="stat ok"><b>${o.lakehouse.tables.length}</b><span>湖仓表</span></div>
    <div class="stat"><b>${o.lakehouse.cdc_watermark}</b><span>生产 CDC 位点</span></div>
    <div class="stat ${o.backups.gate_failed.length ? "warn" : "ok"}">
      <b>${o.backups.count}</b><span>备份${o.backups.gate_failed.length ? `（${o.backups.gate_failed.length} 门禁失败）` : ""}</span></div>
    <div class="stat ${rc.interrupted ? "warn" : "ok"}">
      <b>${rc.completed}/${rc.total}</b><span>完成/总恢复演练</span></div>`;
  return o;
}

/* ------------------------------------------------------------- backups */
async function loadBackups() {
  const { backups } = await api("/api/backups");
  $("#backup-rows").innerHTML = backups.map((b) => `
    <tr data-id="${esc(b.backup_id)}">
      <td class="mono">${esc(b.backup_id)}</td>
      <td class="mono">${esc(b.created_at)}</td>
      <td><span class="badge ${esc(b.kind)}">${esc(b.kind)}</span></td>
      <td class="mono">seq=${b.cdc_position}</td>
      <td>${b.files_captured}</td>
      <td>${b.new_blobs}</td>
      <td>${badge(b.gate_status)}</td>
      <td class="mono">${esc(b.parent_backup_id ?? "—")}</td>
      <td>
        <button class="btn small" data-act="show">清单</button>
        <button class="btn small" data-act="verify">校验</button>
        <button class="btn small danger" data-act="corrupt">注入损坏</button>
      </td>
    </tr>`).join("") || `<tr><td colspan="9" class="empty">尚无备份，先创建一个</td></tr>`;

  $("#backup-rows").querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.closest("tr").dataset.id;
      const act = btn.dataset.act;
      try {
        if (act === "show") await showBackup(id);
        if (act === "verify") {
          const r = await api(`/api/backups/${id}/verify`);
          toast(r.ok ? `校验通过：${r.files_checked} 个文件`
                     : `校验发现 ${r.problems.length + r.cdc_problems.length} 个问题`, !r.ok);
          $("#backup-detail").classList.remove("hidden");
          $("#backup-detail").innerHTML = `<h3>备份校验报告 · ${esc(id)}</h3><pre class="code">${esc(JSON.stringify(r, null, 2))}</pre>`;
        }
        if (act === "corrupt") {
          const manifest = await api(`/api/backups/${id}`);
          const target = manifest.files[0];
          if (!target) return toast("该备份没有数据文件可注入", true);
          const r = await api("/api/drills/corrupt-file", {
            method: "POST", body: { backup_id: id, path: target.path } });
          toast(`已注入损坏（演练）：${r.path}，期望 ${r.expected_sha256.slice(0, 10)}… 实际 ${r.actual_sha256.slice(0, 10)}…`, true);
        }
      } catch (err) { toast(err.message, true); }
    });
  });
  // backup picker on recovery tab
  $("#rc-backup").innerHTML =
    `<option value="">自动选择最新备份</option>` +
    backups.map((b) => `<option value="${esc(b.backup_id)}">${esc(b.backup_id)} (seq=${b.cdc_position})</option>`).join("");
  return backups;
}

async function showBackup(id) {
  const m = await api(`/api/backups/${id}`);
  const gatesRows = m.quality_gates.results.map((g) => `
    <tr><td>${esc(g.name)}</td><td>${badge(g.status)}</td><td>${esc(g.detail)}</td></tr>`).join("");
  $("#backup-detail").classList.remove("hidden");
  $("#backup-detail").innerHTML = `
    <h3>备份清单 · <span class="mono">${esc(m.backup_id)}</span>
      <button class="btn small" style="float:right" onclick="document.getElementById('backup-detail').classList.add('hidden')">关闭</button></h3>
    <div class="cards">
      <div class="card"><h4>基本信息</h4>
        <div class="kv"><span>类型 / 时间</span><span>${esc(m.kind)} · ${esc(m.created_at)}</span></div>
        <div class="kv"><span>父备份</span><span class="mono">${esc(m.parent_backup_id ?? "—")}</span></div>
        <div class="kv"><span>备注</span><span>${esc(m.note || "—")}</span></div></div>
      <div class="card"><h4>CDC 位点</h4>
        <div class="kv"><span>position</span><span class="mono">seq=${m.cdc.position}</span></div>
        <div class="kv"><span>本次新增事件</span><span>${m.cdc.events_appended}</span></div>
        <div class="kv"><span>镜像文件</span><span class="mono">${esc(m.cdc.mirror_path)}</span></div></div>
      <div class="card"><h4>统计</h4>
        <div class="kv"><span>数据文件</span><span>${m.stats.data_files}</span></div>
        <div class="kv"><span>元数据文件</span><span>${m.stats.metadata_files}</span></div>
        <div class="kv"><span>新增内容 Blob</span><span>${m.stats.new_blobs}</span></div></div>
    </div>
    <h4>质量门禁</h4>
    <table class="grid"><thead><tr><th>门禁</th><th>结果</th><th>详情</th></tr></thead><tbody>${gatesRows}</tbody></table>
    <h4>文件版本（${m.files.length} 个数据文件）</h4>
    <pre class="code">${esc(m.files.map((f) => `${f.version}\t${f.sha256.slice(0, 12)}…\t${f.path}`).join("\n"))}</pre>
    <h4>元数据快照（${m.metadata_files.length} 个）</h4>
    <pre class="code">${esc(m.metadata_files.map((f) => `${f.kind}\t${f.path}`).join("\n"))}</pre>`;
}

$("#btn-backup-full").addEventListener("click", () => makeBackup("full"));
$("#btn-backup-inc").addEventListener("click", () => makeBackup("incremental"));
async function makeBackup(kind) {
  try {
    const r = await api("/api/backups", { method: "POST",
      body: { kind, note: $("#backup-note").value } });
    toast(`备份 ${r.backup_id} 完成，门禁=${r.gate_status}，CDC 位点=${r.cdc_position}`);
    refreshAll();
  } catch (err) { toast(err.message, true); }
}

/* ------------------------------------------------------------- recoveries */
async function loadRecoveries() {
  const { recoveries } = await api("/api/recoveries");
  $("#recovery-rows").innerHTML = recoveries.map((j) => `
    <tr data-id="${esc(j.recovery_id)}">
      <td class="mono">${esc(j.recovery_id)}</td>
      <td>${badge(j.status)}</td>
      <td class="mono">${esc(j.base_backup_id)}</td>
      <td class="mono">${j.base_cdc_position} → ${j.target_cdc_seq}</td>
      <td>${esc(j.current_stage ? STAGE_LABELS[j.current_stage] || j.current_stage : "—")}</td>
      <td>${j.quarantine.length ? `<span class="badge failed">${j.quarantine.length}</span>` : "0"}</td>
      <td class="mono">${esc(j.created_at)}</td>
      <td>
        <button class="btn small primary" data-act="run">运行/续跑</button>
        <button class="btn small" data-act="show">阶段</button>
        <button class="btn small warn" data-act="interrupt">中断模拟</button>
      </td>
    </tr>`).join("") || `<tr><td colspan="8" class="empty">尚无恢复演练</td></tr>`;

  $("#recovery-rows").querySelectorAll("button").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.stopPropagation();
      const id = btn.closest("tr").dataset.id;
      const act = btn.dataset.act;
      try {
        if (act === "run") {
          const r = await api(`/api/recoveries/${id}/run`, { method: "POST", body: {} });
          toast(r.status === "completed" ? `恢复 ${id} 已完成全部 5 个阶段`
                                         : `恢复 ${id} 状态=${r.status}`, r.status === "failed");
          await refreshAll();
          await showRecovery(id);
        }
        if (act === "show") await showRecovery(id);
        if (act === "interrupt") {
          const j = await api(`/api/recoveries/${id}`);
          const done = STAGE_ORDER.filter((s) => j.stages[s].status === "completed");
          const failAfter = done[done.length - 1] || "verify";
          const r = await api(`/api/recoveries/${id}/run`, {
            method: "POST", body: { fail_after_stage: failAfter } });
          toast(`已模拟中断（在 ${STAGE_LABELS[failAfter]} 之后），可再次“运行/续跑”`, true);
          await refreshAll();
          await showRecovery(id);
        }
      } catch (err) { toast(err.message, true); }
    });
  });

  const sel = $("#diff-recovery");
  const gapSel = $("#gap-recovery");
  const cur = sel.value;
  sel.innerHTML = recoveries.map((j) =>
    `<option value="${esc(j.recovery_id)}">${esc(j.recovery_id)} (${esc(j.status)})</option>`).join("");
  if (cur) sel.value = cur;
  const gapCur = gapSel.value;
  gapSel.innerHTML = `<option value="">全局视角（不绑定恢复）</option>` +
    recoveries.map((j) => `<option value="${esc(j.recovery_id)}">${esc(j.recovery_id)}</option>`).join("");
  if (gapCur) gapSel.value = gapCur;
  return recoveries;
}

const STAGE_LABELS = {
  verify: "① 备份完整性校验", metadata: "② 重建元数据快照",
  files: "③ 恢复数据文件（隔离损坏）", cdc_replay: "④ 按 CDC 位点回放",
  validate: "⑤ 质量门禁与差异验证",
};

async function showRecovery(id) {
  const j = await api(`/api/recoveries/${id}`);
  const pipeline = STAGE_ORDER.map((s) => {
    const st = j.stages[s];
    return `<div class="stage ${esc(st.status)}">
      <div class="name">${STAGE_LABELS[s]} ${badge(st.status)}</div>
      <div class="when">${esc(st.started_at || "未开始")} → ${esc(st.finished_at || "—")}</div>
      <div class="msg">${esc(st.error || (st.detail ? JSON.stringify(st.detail) : ""))}</div>
    </div>`;
  }).join("");

  const qRows = j.quarantine.map((q) => `
    <tr><td class="mono">${esc(q.path)}</td><td>${esc(q.table ?? "—")}</td>
    <td class="mono">${esc(q.expected_sha256.slice(0, 12))}…</td>
    <td class="mono">${esc((q.actual_sha256 || "MISSING").slice(0, 12))}…</td>
    <td>${esc(q.phase)}</td><td class="mono">${esc(q.quarantined_copy)}</td></tr>`).join("");

  const gapRows = j.gaps.map((g) => `<li>${esc(JSON.stringify(g))}</li>`).join("");
  const impact = j.impact_report;
  $("#recovery-detail").classList.remove("hidden");
  $("#recovery-detail").innerHTML = `
    <h3>恢复演练 · <span class="mono">${esc(j.recovery_id)}</span>
      ${badge(j.status)}
      <button class="btn small" style="float:right" onclick="document.getElementById('recovery-detail').classList.add('hidden')">关闭</button></h3>
    <p class="hint">目标目录 <span class="mono">${esc(j.target_root)}</span> ·
      基线备份 <span class="mono">${esc(j.base_backup_id)}</span> ·
      CDC 回放区间 seq ${j.base_cdc_position} → ${j.target_cdc_seq} ·
      备注 ${esc(j.note || "—")}</p>
    <div class="pipeline">${pipeline}</div>
    <h4>隔离区与影响报告</h4>
    ${qRows ? `<table class="grid"><thead><tr><th>文件</th><th>表</th><th>期望哈希</th><th>实际哈希</th><th>阶段</th><th>隔离副本</th></tr></thead><tbody>${qRows}</tbody></table>`
             : `<p class="empty">无隔离文件</p>`}
    ${impact ? `<pre class="code">${esc(JSON.stringify(impact, null, 2))}</pre>` : ""}
    <h4>本恢复记录的缺口（${j.gaps.length}）</h4>
    ${gapRows ? `<ul class="mono" style="font-size:12px">${gapRows}</ul>` : `<p class="empty">无缺口</p>`}
    ${j.gate_report ? `<h4>恢复后质量门禁 ${badge(j.gate_report.status)}</h4>
      <table class="grid"><tbody>${j.gate_report.results.map((g) =>
        `<tr><td>${esc(g.name)}</td><td>${badge(g.status)}</td><td>${esc(g.detail)}</td></tr>`).join("")}</tbody></table>` : ""}`;
}

$("#btn-rc-create").addEventListener("click", async () => {
  const body = {
    backup_id: $("#rc-backup").value || null,
    target_time: $("#rc-target-time").value || null,
    target_seq: $("#rc-target-seq").value ? Number($("#rc-target-seq").value) : null,
    note: $("#rc-note").value,
  };
  try {
    const j = await api("/api/recoveries", { method: "POST", body });
    toast(`恢复演练 ${j.recovery_id} 已创建（尚未运行）`);
    await refreshAll();
    await showRecovery(j.recovery_id);
  } catch (err) { toast(err.message, true); }
});

/* ------------------------------------------------------------- gaps */
async function loadGaps() {
  const rid = $("#gap-recovery").value || "";
  const g = await api(`/api/gaps${rid ? `?recovery_id=${encodeURIComponent(rid)}` : ""}`);
  const seqPills = g.cdc_mirror.missing_sequences.map((s) =>
    `<span class="pill bad">seq ${s}</span>`).join("");
  $("#gap-view").innerHTML = `
    <div class="gap-block card">
      <h4>CDC 覆盖（镜像水位 seq=${g.cdc_mirror.watermark}，事件 ${g.cdc_mirror.event_count} 个）</h4>
      ${seqPills || `<span class="pill">序列连续，无缺口</span>`}
    </div>
    <div class="gap-block card">
      <h4>备份链缺口（${g.backup_chain_gaps.length}）</h4>
      ${g.backup_chain_gaps.map((x) => `<span class="pill warn">${esc(x.type)} ${esc(JSON.stringify(x).slice(0, 120))}</span>`).join("") || `<span class="pill">无</span>`}
    </div>
    <div class="gap-block card">
      <h4>文件版本演进（${g.file_version_gaps.length} 个路径发生过版本变化）</h4>
      ${g.file_version_gaps.map((x) => `<span class="pill">${esc(x.table)} v${x.from_version}→v${x.to_version} ${esc(x.path)}</span>`).join("") || `<span class="pill">无版本变化</span>`}
    </div>
    <div class="gap-block card">
      <h4>恢复缺口（${g.recovery_gaps.length}）</h4>
      ${g.recovery_gaps.map((x) => `<span class="pill ${x.type.includes("gap") || x.type.includes("quarantin") ? "bad" : "warn"}">${esc(x.type)} ${esc(JSON.stringify(x).slice(0, 140))}</span>`).join("") || `<span class="pill">${rid ? "该恢复无缺口" : "未绑定恢复演练"}</span>`}
    </div>`;
}
$("#gap-recovery").addEventListener("change", loadGaps);

/* ------------------------------------------------------------- diff */
async function loadDiff() {
  const rid = $("#diff-recovery").value;
  if (!rid) { $("#diff-view").innerHTML = `<p class="empty">选择一次恢复演练查看差异</p>`; return; }
  const d = await api(`/api/recoveries/${rid}/diff`);
  const col = (title, items, cls) => `
    <div class="diff-col"><h4>${title} <span class="count-${cls}">(${items.length})</span></h4>
      ${items.length ? `<ul>${items.map((x) => `<li>${esc(JSON.stringify(x))}</li>`).join("")}</ul>`
                     : `<p class="empty">无</p>`}</div>`;
  const rows = Object.entries(d.row_counts).map(([t, c]) => `
    <tr><td>${esc(t)}</td><td>${c.at_backup}</td><td>${c.recovered}</td>
    <td class="${c.delta ? "count-bad" : "count-ok"}">${c.delta >= 0 ? "+" : ""}${c.delta}</td></tr>`).join("");
  $("#diff-view").innerHTML = `
    <div class="diff-grid">
      ${col("丢失文件 missing", d.missing_files, "bad")}
      ${col("损坏/隔离 corrupted", d.corrupted_files, "bad")}
      ${col("内容变化 changed", d.changed_files, "bad")}
      ${col("回放新增 added（CDC 产生的新批次）", d.added_files, "ok")}
    </div>
    <h4 style="margin-top:18px">行数对账</h4>
    <table class="grid"><thead><tr><th>表</th><th>备份时行数</th><th>恢复后行数</th><th>差异</th></tr></thead>
      <tbody>${rows}</tbody></table>
    <h4>CDC 位点</h4><pre class="code">${esc(JSON.stringify(d.cdc, null, 2))}</pre>`;
}
$("#diff-recovery").addEventListener("change", loadDiff);

/* ------------------------------------------------------------- lakehouse */
async function loadLakehouse() {
  const l = await api("/api/lakehouse");
  $("#lakehouse-view").innerHTML = `
    <p class="hint">生产湖仓 <span class="mono">${esc(l.root)}</span> · CDC 水位 <span class="mono">seq=${l.cdc_watermark}</span></p>
    <div class="cards">${Object.entries(l.tables).map(([name, t]) => `
      <div class="card"><h4>${esc(name)}（${t.row_count} 行）</h4>
        <div class="kv"><span>主键</span><span class="mono">${esc(t.primary_key.join(","))}</span></div>
        <div class="kv"><span>数据文件</span><span>${t.current_files.length}</span></div>
        <div class="kv"><span>快照数</span><span>${t.snapshots.length}</span></div>
        <div class="kv"><span>样例行</span><span class="mono">${esc(JSON.stringify(t.sample_rows[0] || {}).slice(0, 90))}</span></div>
      </div>`).join("")}</div>`;
}

/* ------------------------------------------------------------- refresh */
async function refreshAll() {
  try {
    await loadOverview();
    await loadBackups();
    const recs = await loadRecoveries();
    await loadGaps();
    if ($("#diff-recovery").value) await loadDiff();
    if ($(".panel.active").id === "tab-lakehouse") await loadLakehouse();
    // keep an open recovery detail live (polling shows stage progress)
    const open = $("#recovery-detail:not(.hidden)");
    if (open && recs.length) {
      const title = open.querySelector("h3 .mono")?.textContent;
      if (title) await showRecovery(title);
    }
  } catch (err) {
    if (!err.message.includes("Failed to fetch")) toast(err.message, true);
  }
}
$("#refresh-btn").addEventListener("click", refreshAll);
setInterval(() => { if ($("#auto-refresh").checked) refreshAll(); }, 3000);
refreshAll();
