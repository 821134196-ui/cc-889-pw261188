// 分片节点：真实保存 KV 数据，维护单调 op-log 与本地归属 / 栅栏（fence）表。
// 节点在处理代理请求时会独立校验归属——即使代理缓存了旧路由，
// 旧属主也不会接受已搬迁范围的新写入，而是返回 NOT_OWNER 与最新归属。

let KEY_SEQ = 0;

export class ShardNode {
  constructor(id, name, bus) {
    this.id = id;
    this.name = name;
    this.bus = bus;
    this.data = new Map(); // key -> { value, version, updatedAt }
    this.oplog = []; // { seq, op:'put'|'delete', key, value, ts }
    this.seq = 0;
    this.stats = { reads: 0, writes: 0, replicated: 0, rejected: 0 };
    // rangeId -> { role:'owner'|'source'|'target'|'moved-away', epoch, ownerId, fence }
    this.ownership = new Map();
    // 作为搬迁目标时，记录各源分片已回放的 op-log 位点（幂等）
    this.replicaCursor = new Map();
    this.writeBlocked = false; // 切换提交前的短暂写栅栏（barrier）
    this.blockReason = null;
    this.director = null; // 由 system 注入
  }

  setDirector(d) {
    this.director = d;
  }

  setOwnership(rangeId, info) {
    this.ownership.set(rangeId, { ...(this.ownership.get(rangeId) || {}), ...info });
  }

  // ---- 本地读写 / 种子 ----
  seedPut(key, value) {
    const now = Date.now();
    this.data.set(key, { value: String(value), version: 0, updatedAt: now, seeded: true });
  }

  localPut(key, value) {
    this.seq += 1;
    const op = { seq: this.seq, op: 'put', key, value: String(value), ts: Date.now() };
    this.oplog.push(op);
    this.data.set(key, { value: op.value, version: op.seq, updatedAt: op.ts });
    this.stats.writes++;
    return op;
  }

  localDelete(key) {
    this.seq += 1;
    const op = { seq: this.seq, op: 'delete', key, value: null, ts: Date.now() };
    this.oplog.push(op);
    this.data.delete(key);
    this.stats.writes++;
    return op;
  }

  getLocal(key) {
    return this.data.get(key) || null;
  }

  countRange(start = '', end = null) {
    let n = 0;
    for (const k of this.data.keys()) {
      if (k >= start && (end == null || k < end)) n++;
    }
    return n;
  }

  // 按 key 字典序分页扫描（全量复制用），afterKey 用于断点续扫。
  scanRange(start = '', end = null, { limit = 20, afterKey = null } = {}) {
    const keys = [...this.data.keys()]
      .filter((k) => k >= start && (end == null || k < end))
      .sort();
    const rows = [];
    for (const k of keys) {
      if (afterKey != null && k <= afterKey) continue;
      const row = this.data.get(k);
      rows.push({ key: k, value: row.value, version: row.version, updatedAt: row.updatedAt });
      if (rows.length >= limit) break;
    }
    return { rows, lastKey: rows.length ? rows[rows.length - 1].key : afterKey, exhausted: rows.length < limit };
  }

  oplogAfter(afterSeq = 0, { limit = 500 } = {}) {
    return this.oplog.filter((o) => o.seq > afterSeq).slice(0, limit);
  }

  // ---- 搬迁目标：全量行 / 增量 op 应用（幂等）----
  applySnapshotRow(row) {
    const existing = this.data.get(row.key);
    if (existing && existing.version > row.version) return false;
    this.data.set(row.key, {
      value: row.value,
      version: row.version,
      updatedAt: row.updatedAt || Date.now(),
      replicated: true,
    });
    this.stats.replicated++;
    return true;
  }

  applyOplog(sourceId, op) {
    const cursor = this.replicaCursor.get(sourceId) || 0;
    if (op.seq <= cursor) return { applied: false, reason: 'duplicate' };
    if (op.op === 'put') {
      const existing = this.data.get(op.key);
      if (!existing || existing.version <= op.seq) {
        this.data.set(op.key, { value: op.value, version: op.seq, updatedAt: op.ts, replicated: true });
      }
    } else if (op.op === 'delete') {
      this.data.delete(op.key);
    }
    this.replicaCursor.set(sourceId, op.seq);
    this.stats.replicated++;
    return { applied: true };
  }

  // ---- 数据面 RPC：代理转发来的写 / 读，节点独立做归属与栅栏校验 ----
  _checkOwnership(key, fence) {
    if (!this.director) return { ok: true };
    const range = this.director.resolve(key);
    const current = this.director.ownershipOf(key);
    if (current.owner !== this.id) {
      this.stats.rejected++;
      return {
        ok: false,
        code: 'NOT_OWNER',
        message: `${key} 当前归属 ${current.owner}（epoch=${current.epoch}），${this.id} 已不是属主`,
        owner: current.owner,
        epoch: current.epoch,
        configEpoch: current.configEpoch,
        fence: current.fence,
        rangeId: range.id,
      };
    }
    const local = this.ownership.get(range.id);
    if (fence != null && local?.epoch != null && fence !== local.epoch) {
      this.stats.rejected++;
      return {
        ok: false,
        code: 'FENCE_MISMATCH',
        message: `栅栏令牌过期：请求 fence=${fence}，节点当前 epoch=${local.epoch}`,
        owner: this.id,
        epoch: local.epoch,
        fence: local.epoch,
        rangeId: range.id,
      };
    }
    return { ok: true, range, epoch: current.epoch };
  }

  handleWriteRPC(req) {
    const { key, value, op = 'put', fence = null } = req;
    const own = this._checkOwnership(key, fence);
    if (!own.ok) return own;
    if (this.writeBlocked) {
      return {
        ok: false,
        retryable: true,
        code: 'COMMIT_BARRIER',
        message: `分片 ${this.id} 正处于切换提交栅栏，请按最新路由重试`,
        owner: this.id,
      };
    }
    const recorded = op === 'delete' ? this.localDelete(key) : this.localPut(key, value);
    return {
      ok: true,
      owner: this.id,
      shardName: this.name,
      seq: recorded.seq,
      epoch: own.epoch,
      appliedAt: recorded.ts,
    };
  }

  handleReadRPC(req) {
    const { key, fence = null } = req;
    const own = this._checkOwnership(key, fence);
    if (!own.ok) return own;
    this.stats.reads++;
    const row = this.getLocal(key);
    return {
      ok: true,
      owner: this.id,
      shardName: this.name,
      found: !!row,
      value: row ? row.value : null,
      version: row ? row.version : null,
      epoch: own.epoch,
    };
  }

  // 切换完成后清理旧属主上的搬迁范围物理副本（GC）。oplog 保留用于审计。
  gcRange(start = '', end = null) {
    let deleted = 0;
    for (const k of [...this.data.keys()]) {
      if (k >= start && (end == null || k < end)) {
        this.data.delete(k);
        deleted++;
      }
    }
    return deleted;
  }

  entriesInRange(start = '', end = null, limit = 100) {
    return [...this.data.entries()]
      .filter(([k]) => k >= start && (end == null || k < end))
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .slice(0, limit)
      .map(([key, row]) => ({
        key,
        value: row.value,
        version: row.version,
        updatedAt: row.updatedAt,
        replicated: !!row.replicated,
      }));
  }

  snapshotStats() {
    return {
      id: this.id,
      name: this.name,
      keyCount: this.data.size,
      oplogSeq: this.seq,
      stats: { ...this.stats },
      writeBlocked: this.writeBlocked,
      ownership: [...this.ownership.entries()].map(([rangeId, v]) => ({ rangeId, ...v })),
      replicaCursor: [...this.replicaCursor.entries()].map(([sourceId, seq]) => ({ sourceId, seq })),
    };
  }
}

export { KEY_SEQ };
