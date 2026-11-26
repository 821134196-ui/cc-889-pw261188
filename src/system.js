// System：装配所有角色，播种数据，提供页面 / API 所需的状态快照、核对与巡检。

import { EventBus } from './events.js';
import { LinkManager } from './links.js';
import { ShardNode } from './store.js';
import { TopologyDirector } from './topology.js';
import { Proxy } from './proxy.js';
import { MigrationWorker } from './migrator.js';

const QUICK_KEYS = ['key:0400', 'key:0450', 'key:0501', 'key:0618', 'key:0666', 'key:0799'];

export class System {
  constructor() {
    this.bus = new EventBus();
    this.links = new LinkManager(this.bus);
    this.director = new TopologyDirector(this.bus);
    this.shards = new Map();
    this.proxy = new Proxy({ director: this.director, shards: this.shards, links: this.links, bus: this.bus });
    this.migrator = new MigrationWorker({ director: this.director, shards: this.shards, links: this.links, bus: this.bus });
    this.proxy.setMigrator(this.migrator);
    this.truth = new Map(); // 已确认写入的逻辑真值（用于一致性巡检）
    this.traffic = { enabled: false, intervalMs: 850, writes: 0, ok: 0, errors: 0, lastAt: null, lastKey: null, lastError: null };
    this._trafficIndex = 0;
    this.auditResult = null;
    this._initShards();
    this.seed();
    this.warmProxyCache();
    this._startBackgroundTasks();
    // 立即自启搬迁：start() 的同步前缀会立刻把状态置为 active/snapshot，
    // 因此端口开始服务时就不存在“尚未自启”的空窗。
    this.migrator.start({}).catch((err) => {
      this.bus.log('system', `自动启动搬迁失败：${err.message}`, null, 'error');
    });
    this.bus.log('system', '系统就绪：3 个分片 + 代理 + 拓扑主管 + 搬迁进程，已播种 1082 个键');
  }

  _initShards() {
    this.shards.clear();
    const defs = [
      ['shard-1', '北京分片'],
      ['shard-2', '上海分片（搬迁源）'],
      ['shard-3', '广州分片（搬迁目标）'],
    ];
    for (const [id, name] of defs) {
      const s = new ShardNode(id, name, this.bus);
      s.setDirector(this.director);
      this.shards.set(id, s);
    }
    // 初始归属表（与 director 一致，epoch=1）
    this.shards.get('shard-1').setOwnership('range-a', { role: 'owner', epoch: 1, ownerId: 'shard-1' });
    this.shards.get('shard-2').setOwnership('range-b', { role: 'owner', epoch: 1, ownerId: 'shard-2' });
    this.shards.get('shard-3').setOwnership('range-c', { role: 'owner', epoch: 1, ownerId: 'shard-3' });
  }

  seed() {
    this.truth.clear();
    // key:0001 .. key:1080，四位补零保证字典序 == 数字序
    for (let i = 1; i <= 1080; i++) {
      const key = `key:${String(i).padStart(4, '0')}`;
      const value = `初始值-${i}`;
      const range = this.director.resolve(key);
      this.shards.get(range.owner).seedPut(key, value);
      this.truth.set(key, value);
    }
    // 几个可读性更强的演示键（落在 range-c）
    const extras = [
      ['user:alice', '{"city":"beijing","level":3}'],
      ['user:bob', '{"city":"shanghai","level":5}'],
    ];
    for (const [k, v] of extras) {
      const range = this.director.resolve(k);
      this.shards.get(range.owner).seedPut(k, v);
      this.truth.set(k, v);
    }
  }

  warmProxyCache() {
    // 模拟代理启动后缓存了全部路由，且之后依赖 NOT_OWNER 被动失效。
    for (const r of this.director.ranges) {
      this.proxy.cache.set(r.id, {
        owner: r.owner,
        epoch: r.epoch,
        fence: r.epoch,
        confirmedAt: Date.now(),
        via: 'startup-warm',
        reason: null,
      });
    }
  }

  scheduleAutostart() {
    // 保留方法名兼容旧调用；现在立即启动以消除空窗。
    if (this._autostartTimer) clearTimeout(this._autostartTimer);
    this.migrator.start({}).catch((err) => {
      this.bus.log('system', `自动启动搬迁失败：${err.message}`, null, 'error');
    });
  }

  _startBackgroundTasks() {
    // 代理 → 搬迁进程 心跳（经 LinkManager 统计链路状态）
    setInterval(() => {
      this.links.call('proxy->migrator', () => this.migrator.ping()).catch(() => {});
    }, 2000).unref?.();

    // 可选：代理主动周期刷新路由缓存（默认关闭，以复现“旧缓存”场景）
    setInterval(() => {
      if (!this.proxy.aggressiveInvalidation) return;
      for (const r of this.director.ranges) {
        this.proxy.cache.set(r.id, {
          owner: r.owner,
          epoch: r.epoch,
          fence: r.epoch,
          confirmedAt: Date.now(),
          via: 'auto-refresh',
          reason: null,
        });
      }
    }, 1000).unref?.();

    // 持续写入流量发生器（默认关闭）
    setInterval(async () => {
      if (!this.traffic.enabled) return;
      const keys = ['key:0421', 'key:0501', 'key:0577', 'key:0618', 'key:0700', 'key:0733', 'key:0790', 'key:0488'];
      const key = keys[this._trafficIndex++ % keys.length];
      const n = ++this.traffic.writes;
      const value = `traffic#${n}@${new Date().toISOString().slice(11, 19)}`;
      this.traffic.lastKey = key;
      this.traffic.lastAt = Date.now();
      try {
        const res = await this.proxy.write(key, value);
        this.traffic.ok++;
        this.traffic.lastError = null;
        this.truth.set(key, value);
        if (res.neededReconfirm) this.traffic.reconfirmed = (this.traffic.reconfirmed || 0) + 1;
      } catch (err) {
        this.traffic.errors++;
        this.traffic.lastError = err.message;
      }
    }, this.traffic.intervalMs).unref?.();
  }

  setTraffic(enabled, intervalMs) {
    this.traffic.enabled = !!enabled;
    if (intervalMs && Number.isFinite(Number(intervalMs))) {
      this.traffic.intervalMs = Math.max(120, Number(intervalMs));
    }
    this.bus.log('traffic', enabled ? `持续写入流量已开启（每 ${this.traffic.intervalMs}ms 写一个搬迁范围内的键）` : '持续写入流量已停止');
    return this.traffic;
  }

  async writeKV(key, value) {
    const res = await this.proxy.write(key, value);
    if (res.ok) this.truth.set(key, value);
    return res;
  }

  async deleteKV(key) {
    const res = await this.proxy.write(key, null, { op: 'delete' });
    if (res.ok) this.truth.delete(key);
    return res;
  }

  async readKV(key) {
    return this.proxy.read(key);
  }

  // 核对单个键：代理读结果 + 各分片物理副本 + 逻辑真值。
  async verifyKey(key) {
    const own = this.director.ownershipOf(key);
    let proxyRead = null;
    let proxyError = null;
    try {
      proxyRead = await this.proxy.read(key);
    } catch (err) {
      proxyError = err.message;
    }
    const physical = {};
    for (const [id, shard] of this.shards) {
      const row = shard.getLocal(key);
      physical[id] = row
        ? { has: true, value: row.value, version: row.version, replicated: !!row.replicated, updatedAt: row.updatedAt }
        : { has: false };
    }
    const ownerRow = this.shards.get(own.owner)?.getLocal(key) || null;
    const truthHas = this.truth.has(key);
    const truthValue = this.truth.get(key) ?? null;

    const proxyMatchesOwner = proxyRead
      ? proxyRead.found === !!ownerRow && (proxyRead.value ?? null) === (ownerRow?.value ?? null)
      : false;
    const ownerMatchesTruth = truthHas ? ownerRow?.value === truthValue : !ownerRow;

    // 搬迁副本追赶情况（信息性：暂停 / 断连期间允许暂时未追平，不影响读写一致性）
    const ownerValue = ownerRow?.value ?? null;
    const ownerShard = this.shards.get(own.owner);
    const rangeId = own.rangeId;
    const roleOf = (id) => this.shards.get(id)?.ownership.get(rangeId)?.role || null;
    const replicaCopies = [];
    const staleSourceCopies = [];
    for (const [id, p] of Object.entries(physical)) {
      if (id === own.owner || !p.has) continue;
      const item = { shard: id, role: roleOf(id), value: p.value, version: p.version, inSync: p.value === ownerValue };
      if (item.role === 'moved-away') staleSourceCopies.push(item); // 旧属主遗留副本，等待 GC
      else replicaCopies.push(item); // 活跃搬迁副本（如 migration-target）
    }
    const replicasConverged = replicaCopies.every((c) => c.inSync);
    const divergentCopies = replicaCopies.filter((c) => !c.inSync);

    // 一致性的硬标准：代理读 == 权威属主物理数据 == 已确认写入真值。
    const consistent = !proxyError && proxyMatchesOwner && ownerMatchesTruth;
    const note = !consistent
      ? '检测到读写不一致，请查看 checks 与各分片物理副本'
      : !replicasConverged
        ? `读写一致；搬迁目标副本尚有 ${divergentCopies.length} 份旧值（暂停 / 断连 / 追赶中属预期），恢复搬迁后将自动收敛`
        : staleSourceCopies.length
          ? '读写一致、目标副本已追平；旧源分片保留切换前副本（moved-away，不参与读写），可执行 GC 清理'
          : '代理读结果、权威属主物理数据、已确认写入真值一致；各物理副本均已追平';

    const result = {
      key,
      consistent,
      replicasConverged,
      at: Date.now(),
      ownership: own,
      proxyRead: proxyRead ? { found: proxyRead.found, value: proxyRead.value, owner: proxyRead.owner, attempts: proxyRead.attempts, trace: proxyRead.trace } : null,
      proxyError,
      ownerPhysical: own.owner,
      physical,
      replicaCopies,
      staleSourceCopies,
      truth: { has: truthHas, value: truthValue },
      checks: { proxyMatchesOwner, ownerMatchesTruth, replicasConverged },
      note,
    };
    this.bus.log(
      consistent ? 'verify' : 'verify-warn',
      `核对 ${key}：${consistent ? '读写一致 ✓' : '不一致 ✗'}；副本${replicasConverged ? '已追平' : '有延迟'}（属主=${own.owner}，代理尝试=${proxyRead?.attempts ?? '-'}）`,
      result,
      consistent ? 'success' : 'error',
    );
    return result;
  }

  // 全量巡检：逐键比对逻辑真值与权威属主的物理数据。
  audit() {
    const mismatches = [];
    let checked = 0;
    for (const [key, value] of this.truth) {
      checked++;
      const own = this.director.ownershipOf(key);
      const row = this.shards.get(own.owner)?.getLocal(key) || null;
      if (!row || row.value !== value) {
        mismatches.push({ key, expected: value, actual: row ? row.value : null, owner: own.owner });
        if (mismatches.length >= 50) break;
      }
    }
    this.auditResult = { at: Date.now(), checked, mismatchCount: mismatches.length, mismatches, consistent: mismatches.length === 0 };
    this.bus.log('system', `一致性巡检完成：核对 ${checked} 个已确认键，不一致 ${mismatches.length} 个`, null, mismatches.length ? 'error' : 'success');
    return this.auditResult;
  }

  // 清理旧源分片上已移交范围的遗留物理副本。
  gcSource() {
    const m = this.migrator.state;
    if (!m.sourceId || !m.rangeId) return { deleted: 0 };
    const source = this.shards.get(m.sourceId);
    const deleted = source.gcRange(m.start ?? '', m.end ?? null);
    source.setOwnership(m.rangeId, { role: 'moved-away-gced', epoch: m.fence || source.ownership.get(m.rangeId)?.epoch || 2, gcedAt: Date.now() });
    this.bus.log('migration', `已 GC 旧源分片 ${m.sourceId} 上搬迁范围的遗留副本：删除 ${deleted} 行（权威数据在 ${m.targetId}）`, { deleted }, 'success');
    return { deleted, sourceId: m.sourceId, rangeId: m.rangeId };
  }

  shardEntries(id, query = {}) {
    const shard = this.shards.get(id);
    if (!shard) return null;
    const mig = this.migrator.state;
    let start = query.start;
    let end = query.end;
    if (query.scope === 'migration-range' || (!start && !end && query.scope !== 'all')) {
      start = mig.start ?? 'key:0400';
      end = mig.end ?? 'key:0800';
    }
    const limit = Math.min(500, Number(query.limit) || 120);
    return { scope: { start: start ?? '', end: end ?? null }, entries: shard.entriesInRange(start ?? '', end ?? null, limit) };
  }

  // 完整状态快照（页面每秒轮询）
  state() {
    const mig = this.migrator.snapshot();
    const rangeStart = mig.start || 'key:0400';
    const rangeEnd = mig.end || 'key:0800';
    const shards = [...this.shards.values()].map((s) => {
      const base = s.snapshotStats();
      let inMigrationRange = 0;
      for (const k of s.data.keys()) if (k >= rangeStart && (rangeEnd == null || k < rangeEnd)) inMigrationRange++;
      return { ...base, inMigrationRange };
    });
    return {
      ok: true,
      now: Date.now(),
      service: 'sharded-kv-live-migration',
      configEpoch: this.director.configEpoch,
      topology: this.director.snapshot(),
      shards,
      migration: mig,
      proxy: {
        cache: this.proxy.cacheSnapshot(),
        aggressiveInvalidation: this.proxy.aggressiveInvalidation,
        recent: this.proxy.recent,
      },
      links: this.links.snapshot(),
      traffic: this.traffic,
      audit: this.auditResult,
      truthKeys: this.truth.size,
      quickKeys: QUICK_KEYS,
      // 页面只需要事件元数据与文案；体积较大的 detail 不放进轮询快照
      events: this.bus.tail(150).map(({ detail, ...evt }) => evt),
    };
  }

  reset({ autostart = true } = {}) {
    if (this._autostartTimer) clearTimeout(this._autostartTimer);
    this.traffic.enabled = false;
    this.traffic.writes = 0;
    this.traffic.ok = 0;
    this.traffic.errors = 0;
    this.traffic.lastError = null;
    this.traffic.reconfirmed = 0;
    this.auditResult = null;
    this.proxy.cache.clear();
    this.proxy.recent = [];
    this.proxy.aggressiveInvalidation = false;
    this.migrator.reset(); // gen++ 作废旧 worker 循环
    this.links.reset();
    this.director.reset();
    this._initShards(); // 在同一个 this.shards Map 上重建
    this.bus.reset();
    this.seed();
    this.warmProxyCache();
    this.bus.log('system', '演示环境已重置并重新播种');
    if (autostart !== false) this.scheduleAutostart(500);
    return this.state();
  }
}
