/* 分片 KV 演练台前端：轮询 /api/state 渲染，所有操作走 JSON API。 */
'use strict';

const $ = (id) => document.getElementById(id);
const SHARD_IDS = ['shard-1', 'shard-2', 'shard-3'];

let lastState = null;
let lastTrace = null;
let hiddenEventsUntilId = 0;

function escapeHtml(v) {
  return String(v ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function fmtTime(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  const p = (n, l = 2) => String(n).padStart(l, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}
function fmtAgo(ts) {
  if (!ts) return '–';
  const ms = Date.now() - ts;
  if (ms < 1000) return '刚刚';
  if (ms < 60000) return `${Math.round(ms / 1000)}s 前`;
  return `${Math.round(ms / 60000)}m 前`;
}

async function api(path, options = {}) {
  const res = await fetch(path, {
    method: options.method || 'GET',
    headers: options.body !== undefined ? { 'content-type': 'application/json' } : undefined,
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || data.ok === false) {
    throw new Error(data.error || `请求失败 HTTP ${res.status}`);
  }
  return data;
}

// 读取允许 404(found:false)，不抛异常
async function rawRead(key) {
  const res = await fetch(`/api/kv/${encodeURIComponent(key)}`);
  return res.json().catch(() => ({ ok: false, error: `HTTP ${res.status}` }));
}

let toastTimer = null;
function toast(message, type = 'info', ms = 2600) {
  const stack = $('toast-stack');
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = message;
  stack.appendChild(el);
  setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity .3s'; setTimeout(() => el.remove(), 320); }, ms);
}

/* ---------------- 轨迹渲染 ---------------- */
const TRACE_ICON = {
  'cache-hit': '🎯', 'cache-miss': '❓', rejected: '🚫', 'link-error': '🔌',
  confirmed: '✅', 'confirmed-fallback': '🛟', 'write-ok': '📝', 'read-ok': '📖', backoff: '⏳',
};
function renderTrace(trace, containerId = 'trace-log') {
  const box = $(containerId);
  if (!box) return;
  if (!trace || !trace.length) {
    box.innerHTML = '<div class="muted tiny">暂无轨迹。</div>';
    return;
  }
  box.innerHTML = trace.map((t) => `
    <div class="trace-item k-${escapeHtml(t.kind)}">
      <span class="t-ico">${TRACE_ICON[t.kind] || '•'}</span>
      <span class="trace-time">${fmtTime(t.at)}</span>
      <span>${escapeHtml(t.message || t.kind)}${t.code ? ` <span class="pill bad">${escapeHtml(t.code)}</span>` : ''}</span>
    </div>`).join('');
}

/* ---------------- 结果卡片 ---------------- */
function renderWriteResult(r) {
  $('write-result').innerHTML = `
    <div class="result-head">
      <span class="pill ok">写入成功</span>
      <span class="result-title mono">${escapeHtml(r.key)}</span>
      <span class="pill info">物理属主 ${escapeHtml(r.owner)}</span>
      <span class="pill gray">尝试 ${r.attempts} 次</span>
      ${r.neededReconfirm ? '<span class="pill warn">触发归属重新确认</span>' : ''}
    </div>
    <div class="kv-value">${escapeHtml(r.value ?? '(空值)')}</div>
    <div class="meta-line">
      <span>oplog seq <b>${r.seq ?? '–'}</b></span>
      <span>路由 epoch <b>${r.cacheEpoch ?? '–'}</b></span>
      <span>耗时 <b>${r.durationMs}ms</b></span>
      <span>时间 <b>${fmtTime(Date.now())}</b></span>
    </div>`;
  if (r.trace) { lastTrace = r.trace; renderTrace(r.trace); }
}

function renderReadResult(r, key) {
  if (r.found) {
    $('read-result').innerHTML = `
      <div class="result-head">
        <span class="pill ok">读取成功</span>
        <span class="result-title mono">${escapeHtml(r.key || key)}</span>
        <span class="pill info">由 ${escapeHtml(r.owner)} 提供</span>
        <span class="pill gray">尝试 ${r.attempts} 次</span>
      </div>
      <div class="kv-value big" data-testid="read-value">${escapeHtml(r.value)}</div>
      <div class="meta-line"><span>版本 version <b>${r.version ?? '–'}</b></span><span>耗时 <b>${r.durationMs}ms</b></span></div>`;
  } else {
    $('read-result').innerHTML = `
      <div class="result-head"><span class="pill warn">键不存在</span><span class="result-title mono">${escapeHtml(r.key || key)}</span></div>
      <div class="muted tiny">代理当前路由 ${escapeHtml(r.owner || '?')}，尝试 ${r.attempts ?? 1} 次。</div>`;
  }
  if (r.trace) { lastTrace = r.trace; renderTrace(r.trace); }
}

function physRow(p, id, owner, role) {
  if (!p.has) return `<div class="phys-row"><span class="pid">${id}</span><span class="pval muted">— 无物理副本 —</span></div>`;
  let pill = '<span class="pill gray">副本</span>';
  if (id === owner) pill = '<span class="pill ok">权威属主</span>';
  else if (role === 'moved-away' || role === 'moved-away-gced') pill = '<span class="pill bad">旧属主遗留</span>';
  else if (role === 'migration-target') pill = '<span class="pill info">搬迁目标</span>';
  return `<div class="phys-row ${id === owner ? 'owner' : ''}">
    <span class="pid">${id}</span>
    <span class="pval">${escapeHtml(p.value)}</span>
    ${pill}
  </div>`;
}

function renderVerify(v) {
  const c = v.checks || {};
  const overall = v.consistent ? '<span class="pill ok">读写一致 ✓</span>' : '<span class="pill bad">不一致 ✗</span>';
  const conv = v.replicasConverged ? '<span class="pill ok">副本已追平</span>' : '<span class="pill warn">副本延迟中</span>';
  const roleById = {};
  [...(v.replicaCopies || []), ...(v.staleSourceCopies || [])].forEach((c) => { roleById[c.shard] = c.role; });
  $('verify-result').innerHTML = `
    <div class="result-head">${overall}${conv}<span class="result-title mono">${escapeHtml(v.key)}</span><span class="pill info">属主 ${escapeHtml(v.ownership.owner)}@${v.ownership.epoch}</span></div>
    <div class="check-grid">
      <div class="check-item ${c.proxyMatchesOwner ? 'yes' : 'no'}">代理读 = 属主数据<br>${c.proxyMatchesOwner ? '✓' : '✗'}</div>
      <div class="check-item ${c.ownerMatchesTruth ? 'yes' : 'no'}">属主数据 = 写入真值<br>${c.ownerMatchesTruth ? '✓' : '✗'}</div>
      <div class="check-item ${c.replicasConverged ? 'yes' : 'no'}">活跃副本已追平<br>${c.replicasConverged ? '✓' : '…'}</div>
    </div>
    <div class="physical-grid">${SHARD_IDS.map((id) => physRow(v.physical[id] || {}, id, v.ownerPhysical, roleById[id])).join('')}</div>
    <p class="hint" style="margin-bottom:0">${escapeHtml(v.note)}</p>`;
  if (v.proxyRead?.trace) { lastTrace = v.proxyRead.trace; renderTrace(v.proxyRead.trace); }
}

function renderOwnership(o, viaTrace) {
  $('ownership-result').innerHTML = `
    <div class="result-head">
      <span class="pill info">${escapeHtml(o.rangeLabel || o.rangeId)}</span>
      <span class="result-title mono">${escapeHtml(o.key)}</span>
      ${o.migrationPhase ? `<span class="pill gray">搬迁阶段 ${escapeHtml(o.migrationPhase)}</span>` : ''}
    </div>
    <div class="meta-line" style="margin-top:0">
      <span>权威属主 <b style="color:var(--emerald)">${escapeHtml(o.owner)}</b></span>
      <span>范围 epoch / fence <b>${o.epoch ?? o.rangeEpoch}</b></span>
      <span>全局 configEpoch <b>${o.configEpoch}</b></span>
      <span>确认方 <b>${escapeHtml(o.authority || o.via || '–')}</b></span>
    </div>`;
  if (viaTrace) renderTrace(viaTrace);
}

/* ---------------- 主状态渲染 ---------------- */
const PHASE_TEXT = {
  idle: '待启动', snapshot: '全量复制中', streaming: '增量追平 / 监听中',
  committing: '提交切换中', committed: '搬迁完成', failed: '搬迁失败',
};

function renderState(s) {
  lastState = s;
  const qk = document.querySelector('.quick-keys');
  if (qk && !qk.dataset.ready && Array.isArray(s.quickKeys) && s.quickKeys.length) {
    qk.dataset.ready = '1';
    s.quickKeys.forEach((k) => {
      const b = document.createElement('button');
      b.type = 'button'; b.className = 'qkey'; b.textContent = k;
      b.addEventListener('click', () => { $('input-key').value = k; $('ownership-key').value = k; $('input-value').focus(); });
      qk.appendChild(b);
    });
  }
  $('epoch-value').textContent = s.configEpoch;
  $('truth-value').textContent = s.truthKeys;

  const m = s.migration || {};
  const phaseEl = $('migration-phase');
  const paused = !!m.paused;
  const disconnected = m.linkState === 'disconnected';
  phaseEl.textContent = paused ? `已暂停 · ${PHASE_TEXT[m.phase] || m.phase}` : (PHASE_TEXT[m.phase] || m.phase || '–');
  phaseEl.className = `phase-badge phase-${m.phase || 'idle'}`;
  if (paused) phaseEl.classList.add('is-paused');
  if (disconnected) phaseEl.classList.add('is-disconnected');
  if (['snapshot', 'streaming', 'committing'].includes(m.phase) && !paused && !disconnected) phaseEl.classList.add('is-running');

  const srcLink = $('mig-source-link');
  if (m.sourceId) {
    srcLink.textContent = disconnected ? `搬迁↔${m.sourceId} 断开` : `搬迁↔${m.sourceId} 连通`;
    srcLink.className = `link-badge ${disconnected ? 'bad' : 'ok'}`;
  }

  // 按钮状态
  const pauseBtn = $('btn-pause');
  const startBtn = $('btn-start');
  const active = !!m.active;
  pauseBtn.disabled = !active || m.phase === 'committing';
  pauseBtn.querySelector('.btn-label').textContent = paused ? '继续搬迁' : '暂停搬迁';
  pauseBtn.querySelector('.ico-pause').textContent = paused ? '▶' : '⏸';
  startBtn.hidden = !(m.phase === 'idle' || m.phase === 'failed');
  $('btn-cutover').disabled = m.phase !== 'streaming';
  $('btn-cutover').title = m.phase === 'snapshot' ? '全量复制完成后才能切换' : '';
  $('btn-gc').disabled = m.phase !== 'committed';

  // 范围 / 进度
  const rangeText = m.rangeId
    ? `${m.rangeId}  [${m.start || '-∞'}, ${m.end || '+∞'})   ${m.sourceId} → ${m.targetId}`
    : '尚未发起搬迁（默认 range-b: key:0400 ~ key:0800, shard-2 → shard-3）';
  $('mig-range-text').textContent = rangeText;
  const pct = m.snapshot?.percent || 0;
  $('mig-progress-text').textContent = `${m.snapshot?.copied || 0} / ${m.snapshot?.total || 0} 行 · ${pct}%`;
  const bar = $('mig-progress-bar-inner');
  bar.style.width = `${m.phase === 'committed' ? 100 : pct}%`;
  bar.classList.toggle('full', m.phase === 'committed');

  $('stat-rows').textContent = m.stats?.rowsCopied || 0;
  $('stat-ops').textContent = m.stats?.opsReplayed || 0;
  $('stat-lag').textContent = m.stream?.lag || 0;
  $('stat-lag').style.color = (m.stream?.lag || 0) > 0 ? 'var(--amber)' : 'var(--emerald)';
  $('stat-retries').textContent = m.stats?.retries || 0;
  $('stat-episodes').textContent = m.stats?.disconnectEpisodes || 0;
  $('stat-fence').textContent = m.fence ? `${m.fence} / ${m.configEpoch}` : `– / ${s.configEpoch}`;

  // stepper
  const order = ['snapshot', 'streaming', 'committing', 'committed'];
  let idx = order.indexOf(m.phase);
  if (m.phase === 'idle' || m.phase === 'failed') idx = -1;
  document.querySelectorAll('#stepper .step').forEach((stepEl, i) => {
    const stepName = stepEl.dataset.step;
    const stepIdx = order.indexOf(stepName);
    stepEl.classList.toggle('done', m.phase === 'committed' ? true : stepIdx < idx);
    stepEl.classList.toggle('active', stepIdx === idx && m.phase !== 'committed');
  });
  document.querySelectorAll('#stepper .step-line').forEach((lineEl, i) => {
    lineEl.classList.toggle('done', m.phase === 'committed' ? true : i < idx);
  });

  renderCache(s.proxy?.cache || []);
  renderShards(s.shards || []);
  renderLinks(s.links || {});
  renderTraffic(s.traffic || {});
  syncSwitch('auto-refresh-cache', !!s.proxy?.aggressiveInvalidation);
  renderEvents(s.events || []);
  if (s.audit) renderAudit(s.audit);
}

function renderCache(rows) {
  const tb = $('cache-tbody');
  if (!rows.length) {
    tb.innerHTML = '<tr><td colspan="5" class="muted center">缓存为空（下一次请求将触发归属确认）</td></tr>';
    return;
  }
  tb.innerHTML = rows.map((c) => `
    <tr>
      <td><b>${escapeHtml(c.rangeLabel)}</b><div class="mono muted" style="font-size:10.5px">${escapeHtml(c.rangeId)}</div></td>
      <td class="mono">${escapeHtml(c.cachedOwner)}<br><span class="muted">epoch ${c.cachedEpoch} · ${fmtAgo(c.confirmedAt)}</span></td>
      <td class="mono">${escapeHtml(c.authoritativeOwner)}<br><span class="muted">epoch ${c.authoritativeEpoch}</span></td>
      <td>${c.stale ? '<span class="stale-yes">● 已过期</span>' : '<span class="stale-no">● 新鲜</span>'}</td>
      <td class="muted" style="font-size:11px">${escapeHtml(c.via || '–')}</td>
    </tr>`).join('');
}

const ROLE_TEXT = {
  owner: '属主', 'source-owner': '源属主', 'migration-target': '搬迁目标',
  'moved-away': '已移走·旧副本', 'moved-away-gced': '已移走·已GC',
};
function renderShards(shards) {
  for (const sh of shards) {
    const metrics = $('metrics-' + sh.id);
    if (metrics) {
      metrics.innerHTML = `
        <div class="metric"><span class="m-label">物理键</span><span class="m-value">${sh.keyCount}</span></div>
        <div class="metric"><span class="m-label">范围内</span><span class="m-value" style="color:var(--primary)">${sh.inMigrationRange}</span></div>
        <div class="metric"><span class="m-label">读 / 写</span><span class="m-value" style="font-size:13px">${sh.stats.reads}/${sh.stats.writes}</span></div>
        <div class="metric"><span class="m-label">复制行</span><span class="m-value" style="color:var(--blue);font-size:14px">${sh.stats.replicated}</span></div>`;
    }
    const chips = $('owner-' + sh.id);
    if (chips) {
      chips.innerHTML = (sh.ownership || []).map((o) =>
        `<span class="own-chip role-${escapeHtml(o.role)}">${escapeHtml(o.rangeId.replace('range-', '范围'))} · ${ROLE_TEXT[o.role] || o.role}${o.epoch ? ` @${o.epoch}` : ''}</span>`).join('');
    }
  }
}

async function refreshEntries() {
  await Promise.all(SHARD_IDS.map(async (id) => {
    try {
      const data = await api(`/api/shards/${id}/entries?scope=migration-range&limit=90`);
      const tb = $('entries-' + id);
      if (!tb) return;
      if (!data.entries.length) {
        tb.innerHTML = '<tr><td colspan="3" class="muted center">搬迁范围内暂无物理键</td></tr>';
        return;
      }
      tb.innerHTML = data.entries.map((e) => `
        <tr title="version=${e.version} 更新于 ${fmtTime(e.updatedAt)}">
          <td class="mono">${escapeHtml(e.key)}</td>
          <td class="mono" style="max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${escapeHtml(e.value)}</td>
          <td>${e.replicated ? '<span class="tag-rep">复制</span>' : '<span class="tag-local">本地</span>'}</td>
        </tr>`).join('');
    } catch { /* 轮询静默 */ }
  }));
}

function renderLinks(links) {
  const map = { 'proxy->migrator': 'proxy-migrator', 'migrator->source': 'migrator-source' };
  for (const [link, suffix] of Object.entries(map)) {
    const l = links[link];
    if (!l) continue;
    const stateEl = $('state-' + suffix);
    const cb = $('fault-' + suffix);
    if (stateEl) {
      stateEl.textContent = l.down ? 'DISCONNECTED' : 'CONNECTED';
      stateEl.className = `link-state${l.down ? ' down' : ''}`;
    }
    if (cb && document.activeElement !== cb) cb.checked = !!l.down;
  }
}

function syncSwitch(id, val) {
  const el = $(id);
  if (el && document.activeElement !== el) el.checked = !!val;
}

function renderTraffic(t) {
  syncSwitch('traffic-toggle', t.enabled);
  const el = $('traffic-stats');
  if (el) {
    el.textContent = t.enabled || t.writes > 0
      ? `enabled=${t.enabled} · 写入 ${t.writes} · 成功 ${t.ok} · 失败 ${t.errors}${t.lastKey ? ` · 最近 ${t.lastKey}` : ''}${t.lastError ? ` · 错误: ${t.lastError}` : ''}`
      : '未开启';
    el.style.color = t.errors ? 'var(--red)' : 'var(--ink-2)';
  }
}

function renderAudit(a) {
  $('audit-result').innerHTML = a.consistent
    ? `<div class="result-head"><span class="pill ok">巡检通过 ✓</span><span>已核对 ${a.checked} 个已确认键，0 不一致（${fmtTime(a.at)}）</span></div>`
    : `<div class="result-head"><span class="pill bad">${a.mismatchCount} 个不一致</span><span>核对 ${a.checked} 键</span></div>
       <div class="mono tiny">${(a.mismatches || []).slice(0, 8).map((x) => `${escapeHtml(x.key)}: 期望 ${escapeHtml(x.expected)} / 实际 ${escapeHtml(x.actual)} @${escapeHtml(x.owner)}`).join('<br>')}</div>`;
}

function renderEvents(events) {
  const box = $('event-log');
  const visible = events.filter((e) => e.id > hiddenEventsUntilId);
  const atBottom = box.scrollHeight - box.scrollTop - box.clientHeight < 60;
  box.innerHTML = visible.map((e) => `
    <div class="log-line level-${escapeHtml(e.level)}">
      <span class="log-time">${fmtTime(e.ts)}</span>
      <span class="log-cat ${escapeHtml(e.category)}">${escapeHtml(e.category)}</span>
      <span class="log-msg">${escapeHtml(e.message)}</span>
    </div>`).join('');
  if ($('log-autoscroll').checked && atBottom) box.scrollTop = box.scrollHeight;
}

/* ---------------- 操作绑定 ---------------- */
function currentKey() { return $('input-key').value.trim(); }

async function doWrite() {
  const key = currentKey();
  if (!key) return toast('请先填写 Key', 'warn');
  const value = $('input-value').value;
  const btn = $('btn-write');
  btn.disabled = true;
  try {
    const r = await api('/api/kv', { method: 'POST', body: { key, value } });
    renderWriteResult(r);
    toast(`写入成功：${r.owner}（尝试 ${r.attempts} 次）`, 'success');
    // 立即实读 + 核对，形成“写入 → 读结果 → 分片核对”闭环
    const rr = await rawRead(key);
    renderReadResult(rr, key);
    const v = await api(`/api/verify?key=${encodeURIComponent(key)}`);
    renderVerify(v);
    // 自动读 / 核对会刷新轨迹面板；写路径的归属确认轨迹最关键，恢复展示它
    if (r.trace) { lastTrace = r.trace; renderTrace(r.trace); }
    refreshState(); refreshEntries();
  } catch (err) {
    toast(err.message, 'error', 4000);
    $('write-result').innerHTML = `<span class="pill bad">写入失败</span> ${escapeHtml(err.message)}`;
  } finally {
    btn.disabled = false;
  }
}

async function doRead() {
  const key = currentKey();
  if (!key) return toast('请先填写 Key', 'warn');
  try {
    const r = await rawRead(key);
    renderReadResult(r, key);
    if (r.trace) { lastTrace = r.trace; renderTrace(r.trace); }
    refreshState();
  } catch (err) { toast(err.message, 'error'); }
}

async function doVerify() {
  const key = currentKey();
  if (!key) return toast('请先填写 Key', 'warn');
  try {
    const v = await api(`/api/verify?key=${encodeURIComponent(key)}`);
    renderVerify(v);
    refreshState();
  } catch (err) { toast(err.message, 'error'); }
}

async function doDelete() {
  const key = currentKey();
  if (!key) return toast('请先填写 Key', 'warn');
  try {
    await api(`/api/kv/${encodeURIComponent(key)}`, { method: 'DELETE' });
    toast(`已删除 ${key}`, 'success');
    refreshState();
  } catch (err) { toast(err.message, 'error'); }
}

function bind() {
  $('btn-write').addEventListener('click', doWrite);
  $('btn-read').addEventListener('click', doRead);
  $('btn-verify').addEventListener('click', doVerify);
  $('btn-delete').addEventListener('click', doDelete);
  $('input-value').addEventListener('keydown', (e) => { if (e.key === 'Enter') doWrite(); });
  $('input-key').addEventListener('keydown', (e) => { if (e.key === 'Enter') doRead(); });

  // 快键由 renderState 在首次拿到 quickKeys 后渲染

  $('btn-pause').addEventListener('click', async () => {
    const paused = lastState?.migration?.paused;
    try {
      await api(paused ? '/api/migration/resume' : '/api/migration/pause', { method: 'POST' });
      toast(paused ? '搬迁已继续' : '正在暂停并等待检查点确认…', paused ? 'success' : 'warn');
      setTimeout(refreshState, 250);
    } catch (err) { toast(err.message, 'error'); }
  });

  $('btn-start').addEventListener('click', async () => {
    try { await api('/api/migration/start', { method: 'POST', body: {} }); toast('搬迁已启动', 'success'); refreshState(); }
    catch (err) { toast(err.message, 'error'); }
  });

  $('btn-cutover').addEventListener('click', async () => {
    try {
      await api('/api/migration/cutover', { method: 'POST' });
      toast('已请求完成切换：追平增量后提交 epoch', 'success');
      setTimeout(refreshState, 400);
    } catch (err) { toast(err.message, 'error'); }
  });

  $('btn-gc').addEventListener('click', async () => {
    try {
      const r = await api('/api/migration/gc', { method: 'POST' });
      toast(`已清理旧源副本 ${r.deleted} 行`, 'success');
      refreshState(); refreshEntries();
    } catch (err) { toast(err.message, 'error'); }
  });

  $('btn-reset').addEventListener('click', async () => {
    try {
      await api('/api/demo/reset', { method: 'POST', body: {} });
      hiddenEventsUntilId = 0;
      toast('演示环境已重置，搬迁将自动开始', 'success');
      setTimeout(() => { refreshState(); refreshEntries(); }, 300);
    } catch (err) { toast(err.message, 'error'); }
  });

  // 归属查询 / 确认
  $('btn-ownership').addEventListener('click', async () => {
    const key = $('ownership-key').value.trim();
    if (!key) return toast('请填写键', 'warn');
    try {
      const r = await api(`/api/ownership?key=${encodeURIComponent(key)}`);
      renderOwnership(r.ownership);
    } catch (err) { toast(err.message, 'error'); }
  });
  $('btn-proxy-confirm').addEventListener('click', async () => {
    const key = $('ownership-key').value.trim();
    if (!key) return toast('请填写键', 'warn');
    try {
      const r = await api('/api/proxy/confirm', { method: 'POST', body: { key } });
      renderOwnership({ ...r.entry, authority: r.entry.via, key }, r.trace);
      toast(`代理缓存已刷新：${r.entry.owner}@${r.entry.epoch}`, 'success');
      refreshState();
    } catch (err) { toast(err.message, 'error'); }
  });

  // 缓存
  $('btn-inject-stale').addEventListener('click', async () => {
    try { await api('/api/proxy/stale', { method: 'POST', body: {} }); toast('已注入 range-b 旧路由 shard-2@1', 'warn'); refreshState(); }
    catch (err) { toast(err.message, 'error'); }
  });
  $('btn-clear-cache').addEventListener('click', async () => {
    try { await api('/api/proxy/clear', { method: 'POST' }); toast('代理缓存已清空', 'success'); refreshState(); }
    catch (err) { toast(err.message, 'error'); }
  });
  $('auto-refresh-cache').addEventListener('change', async (e) => {
    try { await api('/api/proxy/aggressive', { method: 'POST', body: { enabled: e.target.checked } }); refreshState(); }
    catch (err) { toast(err.message, 'error'); e.target.checked = !e.target.checked; }
  });

  // 故障开关
  const linkMap = { 'fault-proxy-migrator': 'proxy->migrator', 'fault-migrator-source': 'migrator->source' };
  for (const [id, link] of Object.entries(linkMap)) {
    $(id).addEventListener('change', async (e) => {
      const down = e.target.checked;
      try {
        await api('/api/faults', { method: 'POST', body: { link, down } });
        toast(down ? `已切断 ${link}` : `已恢复 ${link}`, down ? 'warn' : 'success');
        refreshState();
      } catch (err) { toast(err.message, 'error'); e.target.checked = !down; }
    });
  }
  document.querySelectorAll('[data-blip]').forEach((b) => b.addEventListener('click', async () => {
    const link = b.dataset.blip;
    try { await api('/api/faults/blip', { method: 'POST', body: { link, ms: 2500 } }); toast(`${link} 瞬断 2.5s`, 'warn'); refreshState(); }
    catch (err) { toast(err.message, 'error'); }
  }));

  // 流量
  $('traffic-toggle').addEventListener('change', async (e) => {
    try { await api('/api/traffic', { method: 'POST', body: { enabled: e.target.checked } }); refreshState(); }
    catch (err) { toast(err.message, 'error'); e.target.checked = !e.target.checked; }
  });

  // 巡检
  $('btn-audit').addEventListener('click', async () => {
    const btn = $('btn-audit'); btn.disabled = true; btn.textContent = '巡检中…';
    try {
      const a = await api('/api/audit', { method: 'POST' });
      renderAudit(a);
      toast(a.consistent ? `巡检通过：${a.checked} 键全部一致` : `发现 ${a.mismatchCount} 个不一致`, a.consistent ? 'success' : 'error', 4000);
      refreshState();
    } catch (err) { toast(err.message, 'error'); }
    finally { btn.disabled = false; btn.textContent = '全量一致性巡检（真值 vs 权威属主物理数据）'; }
  });

  // 日志清屏：隐藏当前已有事件
  $('btn-clear-view').addEventListener('click', () => {
    hiddenEventsUntilId = lastState?.events?.reduce((m, e) => Math.max(m, e.id), 0) || 0;
    $('event-log').innerHTML = '';
  });

  // 同步归属输入框
  $('input-key').addEventListener('input', () => { $('ownership-key').value = $('input-key').value; });
}

async function refreshState() {
  try {
    const s = await api('/api/state');
    renderState(s);
  } catch (err) {
    const ph = $('migration-phase');
    if (ph) { ph.textContent = '服务连接失败，重试中…'; ph.className = 'phase-badge phase-failed'; }
  }
}

window.addEventListener('DOMContentLoaded', () => {
  bind();
  refreshState();
  refreshEntries();
  setInterval(refreshState, 1000);
  setInterval(refreshEntries, 2200);
});
