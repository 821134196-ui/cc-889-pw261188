// 端到端冒烟 / 不变量测试：自启服务（独立端口 + 加速搬迁），跑完退出。
// 用法：npm test  或  node test/smoke.mjs
import { spawn } from 'node:child_process';
import assert from 'node:assert/strict';

const PORT = 4399;
const BASE = `http://127.0.0.1:${PORT}`;

const child = spawn(process.execPath, ['server.js'], {
  cwd: new URL('..', import.meta.url).pathname,
  env: { ...process.env, PORT: String(PORT), MIGRATION_DELAY_SCALE: '0.05' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
let serverLogs = '';
child.stdout.on('data', (d) => (serverLogs += d));
child.stderr.on('data', (d) => (serverLogs += d));

async function api(path, opts = {}) {
  const res = await fetch(BASE + path, {
    ...opts,
    headers: { 'content-type': 'application/json', ...(opts.headers || {}) },
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, json };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, { timeout = 15000, label = 'condition', interval = 120 } = {}) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    last = await fn();
    if (last) return last;
    await sleep(interval);
  }
  throw new Error(`等待超时: ${label}\n${serverLogs.slice(-1200)}`);
}

let passed = 0;
function check(name, fn) {
  fn();
  passed++;
  console.log(`  ✓ ${name}`);
}

try {
  // 0. 健康
  await waitFor(async () => {
    try {
      const r = await fetch(BASE + '/api/health');
      return r.ok;
    } catch {
      return false;
    }
  }, { label: 'server health' });
  console.log('— 服务已启动');

  // 1. 初始状态
  let s = (await api('/api/state')).json;
  check('初始有 3 个分片', () => assert.equal(s.shards.length, 3));
  check('播种 1082 个真值键', () => assert.equal(s.truthKeys, 1082));
  check('初始 configEpoch=1', () => assert.equal(s.configEpoch, 1));

  // 2. 重置为不自动搬迁，精细控制时序
  await api('/api/demo/reset', { method: 'POST', body: { autostart: false } });
  let st = (await api('/api/state')).json;
  check('重置后搬迁 idle', () => assert.equal(st.migration.phase, 'idle'));

  // 3. 启动搬迁并立刻切断 搬迁进程->源分片
  await api('/api/migration/start', { method: 'POST', body: {} });
  await api('/api/faults', { method: 'POST', body: { link: 'migrator->source', down: true } });
  await sleep(900);
  st = (await api('/api/state')).json;
  check('源链路断开时状态=disconnected', () => assert.equal(st.migration.linkState, 'disconnected'));
  check('断开期间发生过重试', () => assert.ok(st.migration.stats.retries > 0));
  const copiedWhileDown = st.migration.snapshot.copied;
  await sleep(500);
  st = (await api('/api/state')).json;
  check('断开期间复制位点不前进', () => assert.equal(st.migration.snapshot.copied, copiedWhileDown));

  // 4. 恢复链路，等待全量完成并进入 streaming
  await api('/api/faults', { method: 'POST', body: { link: 'migrator->source', down: false } });
  await waitFor(async () => {
    const m = (await api('/api/state')).json.migration;
    return m.phase === 'streaming' ? m : null;
  }, { label: '进入 streaming' });
  st = (await api('/api/state')).json;
  check('恢复后断点续传完成全量', () => assert.equal(st.migration.snapshot.percent, 100));
  check('源链路状态恢复 connected', () => assert.equal(st.migration.linkState, 'connected'));

  // 5. 暂停搬迁，实写并核对
  const paused = (await api('/api/migration/pause', { method: 'POST' })).json.migration;
  check('暂停被 worker 确认', () => assert.equal(paused.paused, true));
  const w = (await api('/api/kv', { method: 'POST', body: { key: 'key:0501', value: '暂停期间实写-Hello' } })).json;
  check('暂停期写入成功且属源分片 shard-2', () => {
    assert.equal(w.ok, true);
    assert.equal(w.owner, 'shard-2');
  });
  const r = (await api('/api/kv/key:0501')).json;
  check('代理读回相同值', () => assert.equal(r.value, '暂停期间实写-Hello'));
  const v = (await api('/api/verify?key=key:0501')).json;
  check('核对：读写与真值一致', () => assert.equal(v.consistent, true));
  check('暂停期目标副本允许有延迟', () => assert.equal(v.checks.ownerMatchesTruth, true));

  // 6. 恢复 + 切断 代理->搬迁进程 + 切换
  await api('/api/migration/resume', { method: 'POST' });
  await waitFor(async () => {
    const m = (await api('/api/state')).json.migration;
    return m.stream.lag === 0 ? m : null;
  }, { label: '增量追平' });

  await api('/api/faults', { method: 'POST', body: { link: 'proxy->migrator', down: true } });
  await api('/api/migration/cutover', { method: 'POST' });
  await waitFor(async () => {
    const x = (await api('/api/state')).json;
    return x.migration.phase === 'committed' && x.configEpoch === 2 ? x : null;
  }, { label: '切换提交' });
  console.log('  — 已提交 epoch=2（proxy->migrator 仍断开，代理缓存仍旧）');

  // 7. 旧缓存 + pm 断连下写入：必须 NOT_OWNER -> 回退配置中心确认 -> 重试成功
  const w2 = (await api('/api/kv', { method: 'POST', body: { key: 'key:0666', value: '旧缓存+断连下写入' } })).json;
  check('旧缓存写入最终成功', () => assert.equal(w2.ok, true));
  check('新写入物理归属 shard-3', () => assert.equal(w2.owner, 'shard-3'));
  check('至少尝试 2 次（旧路由被拒后重试）', () => assert.ok(w2.attempts >= 2));
  check('trace 含 NOT_OWNER 拒绝', () => assert.ok(w2.trace.some((t) => t.code === 'NOT_OWNER')));
  check('trace 含搬迁进程链路错误', () => assert.ok(w2.trace.some((t) => t.kind === 'link-error')));
  check('trace 含配置中心回退确认', () => assert.ok(w2.trace.some((t) => t.kind === 'confirmed-fallback')));

  const r2 = (await api('/api/kv?key=key:0666')).json;
  check('新属主读回值一致', () => assert.equal(r2.value, '旧缓存+断连下写入'));
  const v2 = (await api('/api/verify?key=key:0666')).json;
  check('切换后核对一致且副本收敛', () => {
    assert.equal(v2.consistent, true);
    assert.equal(v2.replicasConverged, true);
  });

  // 8. 恢复 pm 链路；再写一键，走搬迁进程确认
  await api('/api/faults', { method: 'POST', body: { link: 'proxy->migrator', down: false } });
  await sleep(100);
  const w3 = (await api('/api/kv', { method: 'POST', body: { key: 'key:0777', value: '链路恢复后写入' } })).json;
  check('链路恢复后写入成功归属 shard-3', () => {
    assert.equal(w3.ok, true);
    assert.equal(w3.owner, 'shard-3');
  });

  // 9. fence 回归：全局 epoch=2 后，未搬迁范围 range-a 仍可正常写；
  //    清空代理缓存后重新确认必须拿到该范围自己的 epoch=1 而不是全局 2。
  await api('/api/proxy/clear', { method: 'POST' });
  const wa = (await api('/api/kv', { method: 'POST', body: { key: 'key:0011', value: '范围A-fence回归' } })).json;
  check('range-a 缓存未命中确认后写入 shard-1 成功', () => {
    assert.equal(wa.ok, true);
    assert.equal(wa.owner, 'shard-1');
  });
  check('range-a 首次即成功（fence 与范围 epoch 匹配）', () => assert.equal(wa.attempts, 1));
  const ra = (await api('/api/kv/key:0011')).json;
  check('range-a 读回一致', () => assert.equal(ra.value, '范围A-fence回归'));

  // range-c 同理
  const wc = (await api('/api/kv', { method: 'POST', body: { key: 'key:0900', value: '范围C写入' } })).json;
  check('range-c 写入 shard-3 成功', () => assert.equal(wc.owner, 'shard-3'));

  // 10. 全量巡检
  const audit = (await api('/api/audit', { method: 'POST' })).json;
  check(`巡检全部一致（checked=${audit.checked}）`, () => {
    assert.equal(audit.consistent, true);
    assert.equal(audit.mismatchCount, 0);
  });

  // 11. 物理分片状态可查
  const entries = (await api('/api/shards/shard-3/entries?scope=migration-range&limit=5')).json;
  check('可查询 shard-3 搬迁范围物理数据', () => assert.equal(entries.ok, true));
  check('shard-3 已有物理行', () => assert.ok(entries.entries.length > 0));

  // 12. 重置
  const reset = (await api('/api/demo/reset', { method: 'POST', body: {} })).json;
  check('重置成功 epoch 回到 1', () => assert.equal(reset.state.configEpoch, 1));
  await waitFor(async () => {
    const m = (await api('/api/state')).json.migration;
    return m.phase === 'snapshot' || m.phase === 'streaming' ? true : null;
  }, { label: '重置后自动开始搬迁' });

  console.log(`\n全部 ${passed} 项断言通过 ✅`);
} catch (err) {
  console.error('\n冒烟测试失败 ❌:', err.message);
  console.error(serverLogs.slice(-2000));
  process.exitCode = 1;
} finally {
  child.kill('SIGTERM');
  setTimeout(() => process.exit(process.exitCode || 0), 300);
}
