// 搬迁进程（Migration Worker / Coordinator）逻辑角色。
//
// 状态机：idle → snapshot(先全量复制) → streaming(追增量) → committing → committed
//   - 任意运行阶段可 pause / resume（检查点：扫描 lastKey + oplog seq）
//   - migrator->source 链路可被故障注入切断：快速失败、保留游标、恢复后续传
//   - 提交瞬间对源分片加短暂写栅栏，追平最后增量，再由拓扑主管原子推进 epoch
//   - 同时充当“搬迁权威”，通过 proxy->migrator 链路回答归属确认；
//     该链路断开时，代理会回退到拓扑主管。

import { LinkDownError } from './links.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const DELAY_SCALE = Number(process.env.MIGRATION_DELAY_SCALE || process.env.DELAY_SCALE || 1);

function freshState(opts = {}) {
  return {
    id: opts.id || null,
    active: false,
    phase: 'idle', // idle | snapshot | streaming | committing | committed | failed
    paused: false,
    pausedAt: null,
    cutoverRequested: false,
    rangeId: opts.rangeId || null,
    start: opts.start ?? null,
    end: opts.end ?? null,
    sourceId: opts.sourceId || null,
    targetId: opts.targetId || null,
    linkState: 'connected',
    sourceLinkDownEpisode: false,
    snapshot: { total: 0, copied: 0, percent: 0, lastKey: null, batches: 0, sourceSeqAtStart: 0, finishedAt: null },
    stream: { cursorSeq: 0, opsReplayed: 0, lag: 0, lastAppliedAt: null },
    stats: { rowsCopied: 0, opsReplayed: 0, retries: 0, disconnectEpisodes: 0 },
    startedAt: null,
    updatedAt: null,
    finishedAt: null,
    committedAt: null,
    fence: null,
    configEpoch: null,
    error: null,
  };
}

export class MigrationWorker {
  constructor({ director, shards, links, bus }) {
    this.director = director;
    this.shards = shards;
    this.links = links;
    this.bus = bus;
    this.gen = 0; // 每次 reset / 新任务递增，作废旧循环
    this.state = freshState();
    this.batchSize = 12;
    this.batchDelayMs = Math.max(0, Math.round(110 * DELAY_SCALE));
    this.streamIdleMs = Math.max(20, Math.round(280 * DELAY_SCALE));
  }

  sourceIdFor(rangeId) {
    return this.state.rangeId === rangeId ? this.state.sourceId : null;
  }

  ping() {
    return { ok: true, role: 'migration-worker', at: Date.now(), migrationId: this.state.id, phase: this.state.phase };
  }

  // 代理经 proxy->migrator 链路调用的归属确认。
  confirmOwnership(key) {
    const own = this.director.ownershipOf(key);
    return {
      ...own,
      via: 'migration-process',
      migrationId: this.state.id,
      workerPhase: this.state.phase,
      workerPaused: this.state.paused,
    };
  }

  _alive(gen) {
    return gen === this.gen;
  }

  _touch() {
    this.state.updatedAt = Date.now();
    this.director.markMigration(this._view());
  }

  _view() {
    const s = this.state;
    return {
      id: s.id,
      phase: s.phase,
      paused: s.paused,
      rangeId: s.rangeId,
      sourceId: s.sourceId,
      targetId: s.targetId,
      percent: s.snapshot.percent,
      lag: s.stream.lag,
      active: s.active,
    };
  }

  reset() {
    this.gen++;
    this.state = freshState();
    this.director.markMigration(null);
  }

  async start(opts = {}) {
    if (this.state.active && this.state.phase !== 'committed' && this.state.phase !== 'failed') {
      const err = new Error('已有搬迁任务在运行，请先完成或重置');
      err.code = 'MIGRATION_BUSY';
      throw err;
    }
    const rangeId = opts.rangeId || 'range-b';
    const range = this.director.ranges.find((r) => r.id === rangeId) || this.director.resolve(opts.start || 'key:0400');
    const sourceId = opts.sourceId || range.owner;
    const targetId = opts.targetId || 'shard-3';
    if (sourceId === targetId) throw new Error('源分片与目标分片不能相同');
    const source = this.shards.get(sourceId);
    const target = this.shards.get(targetId);
    if (!source || !target) throw new Error('源或目标分片不存在');

    this.gen++;
    const gen = this.gen;
    this.state = freshState({
      id: `mig-${Date.now().toString(36)}`,
      rangeId: range.id,
      start: opts.start ?? range.start,
      end: opts.end ?? range.end,
      sourceId,
      targetId,
    });
    const s = this.state;
    s.active = true;
    s.phase = 'snapshot';
    s.startedAt = Date.now();
    this._touch();

    // 搬迁期间：源仍是写属主（增量进 oplog），目标标记为搬迁目标副本。
    source.setOwnership(range.id, { role: 'source-owner', epoch: this.director.configEpoch, ownerId: sourceId, targetId });
    target.setOwnership(range.id, { role: 'migration-target', epoch: this.director.configEpoch, ownerId: sourceId, targetId });

    this.bus.log('migration', `搬迁任务 ${s.id} 启动：${range.label} [${s.start || '-∞'}, ${s.end || '+∞'}) ${sourceId} → ${targetId}，先全量复制再追增量`, { ...this._view() });

    // 不阻塞 HTTP 请求，后台执行。
    this._run(gen).catch((err) => {
      if (err?.message === 'CANCELLED') return;
      s.phase = 'failed';
      s.active = false;
      s.error = err?.message || String(err);
      this.bus.log('migration', `搬迁失败：${s.error}`, null, 'error');
    });

    return this.snapshot();
  }

  // 暂停：请求在下一个检查点生效，并等待 worker 确认，方便 API / 自动化断言。
  async pause(timeoutMs = 2500) {
    const s = this.state;
    if (!s.active || s.phase === 'committed') return this.snapshot();
    s.paused = true;
    s.pausedAt = null;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      if (s.pausedAt != null || ['committing', 'committed', 'failed'].includes(s.phase)) break;
      await sleep(40);
    }
    this.bus.log('migration', s.pausedAt != null ? '搬迁已暂停（检查点已确认）；期间写入仍由源分片受理并记录增量' : '暂停请求已记录，等待当前批次结束…', { phase: s.phase });
    return this.snapshot();
  }

  resume() {
    const s = this.state;
    if (!s.active) return this.snapshot();
    s.paused = false;
    s.pausedAt = null;
    this.bus.log('migration', '搬迁继续：从扫描断点 / oplog 游标续跑', {
      lastKey: s.snapshot.lastKey,
      cursorSeq: s.stream.cursorSeq,
    });
    return this.snapshot();
  }

  // 用户点击“完成切换”：即使处于暂停也放行，先追平再提交。
  requestCutover() {
    const s = this.state;
    if (!s.active) throw new Error('当前没有进行中的搬迁');
    if (s.phase === 'committed') return this.snapshot();
    if (s.phase === 'snapshot') {
      const err = new Error('全量复制尚未完成，暂不能切换');
      err.code = 'SNAPSHOT_NOT_DONE';
      throw err;
    }
    s.cutoverRequested = true;
    s.paused = false;
    s.pausedAt = null;
    this.bus.log('migration', '收到完成切换请求：将追平剩余增量并原子提交拓扑');
    return this.snapshot();
  }

  async _gate(gen) {
    const s = this.state;
    while (s.paused && !s.cutoverRequested) {
      if (!this._alive(gen)) throw new Error('CANCELLED');
      s.pausedAt = s.pausedAt || Date.now();
      await sleep(120);
    }
    if (!this._alive(gen)) throw new Error('CANCELLED');
  }

  // 经可能被切断的 migrator->source 链路调用源分片；断连时保留游标并重试。
  async _sourceRpc(gen, label, fn) {
    const s = this.state;
    for (;;) {
      if (!this._alive(gen)) throw new Error('CANCELLED');
      try {
        const out = await this.links.call('migrator->source', fn);
        if (s.sourceLinkDownEpisode) {
          s.sourceLinkDownEpisode = false;
          s.linkState = 'connected';
          this.bus.log('migration', `与源分片连接恢复（${label}），从已有游标继续，不重复不丢失`, {
            lastKey: s.snapshot.lastKey,
            cursorSeq: s.stream.cursorSeq,
          }, 'success');
        }
        return out;
      } catch (err) {
        if (err instanceof LinkDownError || err.code === 'LINK_DOWN') {
          s.linkState = 'disconnected';
          if (!s.sourceLinkDownEpisode) {
            s.sourceLinkDownEpisode = true;
            s.stats.disconnectEpisodes++;
            this.bus.log('migration', `搬迁进程与源分片失去连接（${label}）；已保留检查点 lastKey=${s.snapshot.lastKey} / seq=${s.stream.cursorSeq}，将自动重试`, null, 'warn');
          }
          s.stats.retries++;
          await sleep(350);
          continue;
        }
        throw err;
      }
    }
  }

  async _run(gen) {
    const s = this.state;
    const source = this.shards.get(s.sourceId);
    const target = this.shards.get(s.targetId);

    // ---------- 阶段 1：全量复制 ----------
    s.phase = 'snapshot';
    const total = source.countRange(s.start, s.end);
    s.snapshot.total = total;
    s.snapshot.sourceSeqAtStart = source.seq;
    this._touch();
    this.bus.log('migration', `全量复制开始：范围内 ${total} 个键，快照位点 seq=${source.seq}`);

    let afterKey = null;
    let guard = 0;
    while (guard++ < 100000) {
      await this._gate(gen);
      if (!this._alive(gen)) throw new Error('CANCELLED');

      const batch = await this._sourceRpc(gen, 'scan', () =>
        source.scanRange(s.start, s.end, { limit: this.batchSize, afterKey }),
      );
      if (!batch.rows.length) break;

      for (const row of batch.rows) target.applySnapshotRow(row);
      afterKey = batch.lastKey;
      s.snapshot.lastKey = batch.lastKey;
      s.snapshot.batches++;
      s.snapshot.copied += batch.rows.length;
      s.stats.rowsCopied = s.snapshot.copied;
      s.snapshot.percent = s.snapshot.total ? Math.min(100, Math.round((s.snapshot.copied / s.snapshot.total) * 100)) : 100;
      this._touch();

      if (batch.exhausted) break;
      if (this.batchDelayMs > 0) await sleep(this.batchDelayMs);
    }

    s.snapshot.percent = 100;
    s.snapshot.finishedAt = Date.now();
    // 增量从快照开始时记录的 seq 之后拉取（与扫描有重叠也可幂等重放）
    s.stream.cursorSeq = s.snapshot.sourceSeqAtStart;
    this.bus.log('migration', `全量复制完成：复制 ${s.snapshot.copied} 行；开始追增量，oplog 游标 seq>${s.stream.cursorSeq}`);

    // ---------- 阶段 2：追增量（常驻 tail，直到请求切换）----------
    s.phase = 'streaming';
    this._touch();

    let idleRounds = 0;
    while (this._alive(gen)) {
      await this._gate(gen);
      if (!this._alive(gen)) throw new Error('CANCELLED');

      const ops = await this._sourceRpc(gen, 'oplog', () => source.oplogAfter(s.stream.cursorSeq, { limit: 200 }));
      if (ops.length) {
        for (const op of ops) {
          target.applyOplog(source.id, op);
          s.stream.cursorSeq = op.seq;
          s.stream.opsReplayed++;
          s.stats.opsReplayed++;
        }
        s.stream.lastAppliedAt = Date.now();
        s.stream.lag = Math.max(0, source.seq - s.stream.cursorSeq);
        this._touch();
        idleRounds = 0;
        continue; // 有增量就立刻继续排空
      }

      s.stream.lag = Math.max(0, source.seq - s.stream.cursorSeq);
      this._touch();

      if (s.cutoverRequested) {
        await this._commit(gen);
        return;
      }

      // 可被暂停 / 取消快速打断的 idle 等待
      const waitSlices = Math.max(1, Math.round(this.streamIdleMs / 50));
      for (let i = 0; i < waitSlices; i++) {
        await sleep(50);
        if (!this._alive(gen)) throw new Error('CANCELLED');
        if (s.cutoverRequested) break;
      }
      idleRounds++;
    }
    throw new Error('CANCELLED');
  }

  async _commit(gen) {
    if (!this._alive(gen)) throw new Error('CANCELLED');
    const s = this.state;
    const source = this.shards.get(s.sourceId);
    const target = this.shards.get(s.targetId);
    s.phase = 'committing';
    this._touch();
    this.bus.log('migration', '进入提交流程：对源分片施加短暂写栅栏，排空最后增量…');

    source.writeBlocked = true;
    source.blockReason = 'commit-barrier';
    try {
      // 栅栏内排空 oplog（读 oplog 不受写栅栏影响；链路故障仍可重试）
      for (let round = 0; round < 60; round++) {
        if (!this._alive(gen)) throw new Error('CANCELLED');
        const ops = await this._sourceRpc(gen, 'final-drain', () =>
          source.oplogAfter(s.stream.cursorSeq, { limit: 500 }),
        );
        for (const op of ops) {
          target.applyOplog(source.id, op);
          s.stream.cursorSeq = op.seq;
          s.stream.opsReplayed++;
          s.stats.opsReplayed++;
        }
        s.stream.lag = Math.max(0, source.seq - s.stream.cursorSeq);
        if (s.stream.lag === 0) break;
        await sleep(30);
      }

      // 原子推进权威拓扑 epoch（范围 epoch 作为 fence，全局 configEpoch 作为版本）
      const res = this.director.commit(s.rangeId, s.targetId);
      s.fence = res.range.epoch;
      s.configEpoch = res.configEpoch;

      source.setOwnership(s.rangeId, {
        role: 'moved-away',
        epoch: res.range.epoch,
        ownerId: s.targetId,
        redirect: s.targetId,
      });
      target.setOwnership(s.rangeId, {
        role: 'owner',
        epoch: res.range.epoch,
        ownerId: s.targetId,
        fence: res.range.epoch,
      });

      s.phase = 'committed';
      s.active = false;
      s.cutoverRequested = false;
      s.paused = false;
      s.pausedAt = null;
      s.committedAt = Date.now();
      s.finishedAt = s.committedAt;
      s.stream.lag = 0;
      this._touch();
      this.director.markMigration({ ...this._view(), phase: 'committed' });
      this.bus.log('migration', `搬迁完成并已提交：${s.rangeId} 新属主=${s.targetId}，fence/configEpoch=${res.configEpoch}。仍缓存旧路由的代理写入将被旧属主拒绝并触发归属确认`, null, 'success');
    } finally {
      source.writeBlocked = false;
      source.blockReason = null;
    }
  }

  snapshot() {
    return JSON.parse(JSON.stringify(this.state));
  }
}
