// 无状态代理（带本地路由缓存）。
// 关键点：
//  1) 缓存可能保留旧路由；写入被旧属主以 NOT_OWNER / FENCE_MISMATCH 拒绝后，
//     代理必须重新“确认新的写入归属”，刷新缓存并用新 fence 重试，对客户端成功。
//  2) 归属确认优先询问搬迁进程（经过可能中断的 proxy->migrator 链路）；
//     链路断开时有限重试，随后回退到权威拓扑主管，因此断连不影响写入正确性。

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class Proxy {
  constructor({ director, shards, links, bus }) {
    this.director = director;
    this.shards = shards; // Map id -> ShardNode
    this.links = links;
    this.bus = bus;
    this.migrator = null;
    this.cache = new Map(); // rangeId -> { owner, epoch, confirmedAt, via, reason }
    this.aggressiveInvalidation = false;
    this.recent = []; // 最近数据面操作（含 trace）
    this.maxRecent = 20;
  }

  setMigrator(m) {
    this.migrator = m;
  }

  _pushRecent(rec) {
    this.recent.unshift(rec);
    if (this.recent.length > this.maxRecent) this.recent.length = this.maxRecent;
  }

  // 权威归属确认：先搬迁进程，失败 / 断连重试后回退拓扑主管。
  async confirmOwnership(key, reason, trace = []) {
    const range = this.director.resolve(key);
    let migrationAnswer = null;
    let linkError = null;

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        migrationAnswer = await this.links.call('proxy->migrator', () =>
          this.migrator.confirmOwnership(key),
        );
        break;
      } catch (err) {
        linkError = err;
        trace.push({
          kind: 'link-error',
          at: Date.now(),
          message: `向搬迁进程确认归属失败（第 ${attempt} 次）：${err.message}`,
        });
        if (attempt < 2) await sleep(180);
      }
    }

    let confirmation;
    if (migrationAnswer && migrationAnswer.owner) {
      confirmation = {
        owner: migrationAnswer.owner,
        epoch: migrationAnswer.epoch,
        fence: migrationAnswer.fence ?? migrationAnswer.epoch,
        via: 'migration-process',
        migrationPhase: migrationAnswer.migrationPhase,
      };
      trace.push({
        kind: 'confirmed',
        at: Date.now(),
        via: 'migration-process',
        owner: confirmation.owner,
        epoch: confirmation.epoch,
        message: `搬迁进程确认：新归属 ${confirmation.owner}，epoch=${confirmation.epoch}`,
      });
      this.bus.log(
        'proxy',
        `归属确认（经搬迁进程）：${key} → ${confirmation.owner}@${confirmation.epoch}，原因=${reason}`,
        { key, reason, ...confirmation },
      );
    } else {
      const own = this.director.ownershipOf(key);
      confirmation = {
        owner: own.owner,
        epoch: own.epoch,
        fence: own.fence,
        rangeEpoch: own.rangeEpoch,
        via: 'topology-director',
        migrationPhase: own.migrationPhase,
      };
      trace.push({
        kind: 'confirmed-fallback',
        at: Date.now(),
        via: 'topology-director',
        owner: confirmation.owner,
        epoch: confirmation.epoch,
        linkDown: !!linkError,
        message: linkError
          ? `搬迁进程不可达（${linkError.message}），已由权威配置中心确认：${confirmation.owner}@${confirmation.epoch}`
          : `由权威配置中心确认：${confirmation.owner}@${confirmation.epoch}`,
      });
      this.bus.log(
        'proxy',
        `归属确认（回退配置中心）：${key} → ${confirmation.owner}@${confirmation.epoch}，原因=${reason}` +
          (linkError ? `；搬迁链路错误：${linkError.message}` : ''),
        { key, reason, linkError: linkError?.message || null, ...confirmation },
        linkError ? 'warn' : 'info',
      );
    }

    const entry = {
      owner: confirmation.owner,
      epoch: confirmation.epoch,
      fence: confirmation.fence,
      confirmedAt: Date.now(),
      via: confirmation.via,
      reason,
    };
    this.cache.set(range.id, entry);
    return { ...entry, rangeId: range.id, migrationPhase: confirmation.migrationPhase };
  }

  async write(key, value, opts = {}) {
    const startedAt = Date.now();
    const trace = [];
    const range = this.director.resolve(key);
    let entry = this.cache.get(range.id);
    if (!entry) {
      trace.push({ kind: 'cache-miss', at: Date.now(), message: '代理路由缓存未命中，发起归属确认' });
      entry = await this.confirmOwnership(key, 'cache-miss', trace);
    } else {
      const stale = this._isStale(range.id, entry);
      trace.push({
        kind: 'cache-hit',
        at: Date.now(),
        owner: entry.owner,
        epoch: entry.epoch,
        stale,
        message: `代理使用缓存路由：${entry.owner}@${entry.epoch}${stale ? '（缓存可能已过期）' : ''}`,
      });
    }

    let resp = null;
    let lastCode = null;
    for (let attempt = 1; attempt <= 6; attempt++) {
      const shard = this.shards.get(entry.owner);
      if (!shard) throw Object.assign(new Error(`路由指向不存在的分片 ${entry.owner}`), { code: 'NO_SHARD' });
      resp = shard.handleWriteRPC({ key, value, op: opts.op || 'put', fence: entry.fence ?? entry.epoch });

      if (resp.ok) {
        trace.push({
          kind: 'write-ok',
          at: Date.now(),
          attempt,
          owner: resp.owner,
          seq: resp.seq,
          message: `第 ${attempt} 次尝试写入成功，物理分片=${resp.owner}，oplog seq=${resp.seq}`,
        });
        const result = {
          ok: true,
          key,
          value: opts.op === 'delete' ? null : String(value),
          deleted: opts.op === 'delete',
          owner: resp.owner,
          shardName: resp.shardName,
          epoch: resp.epoch,
          seq: resp.seq,
          attempts: attempt,
          durationMs: Date.now() - startedAt,
          routeUsed: entry.owner,
          cacheEpoch: entry.epoch,
          neededReconfirm: trace.some((t) => t.kind === 'rejected' || t.kind === 'confirmed-fallback' || (t.kind === 'confirmed' && String(t.message).includes('拒绝'))),
          trace,
        };
        this._pushRecent({ at: Date.now(), type: opts.op === 'delete' ? 'delete' : 'write', key, value: result.value, owner: result.owner, ok: true, attempts: attempt, durationMs: result.durationMs });
        return result;
      }

      lastCode = resp.code;
      trace.push({
        kind: 'rejected',
        at: Date.now(),
        attempt,
        code: resp.code,
        owner: entry.owner,
        message: resp.message || `分片 ${entry.owner} 拒绝写入：${resp.code}`,
      });

      if (resp.code === 'NOT_OWNER' || resp.code === 'FENCE_MISMATCH') {
        entry = await this.confirmOwnership(key, resp.code, trace);
        continue;
      }
      if (resp.retryable || resp.code === 'COMMIT_BARRIER') {
        trace.push({ kind: 'backoff', at: Date.now(), message: '切换栅栏窗口，120ms 后重试同一属主…' });
        await sleep(120);
        continue;
      }
      const err = new Error(resp.message || `写入失败 ${resp.code}`);
      err.code = resp.code;
      throw err;
    }
    throw new Error(`写入 ${key} 多次重试后仍失败：${lastCode}`);
  }

  async read(key) {
    const startedAt = Date.now();
    const trace = [];
    const range = this.director.resolve(key);
    let entry = this.cache.get(range.id);
    if (!entry) {
      trace.push({ kind: 'cache-miss', at: Date.now(), message: '代理路由缓存未命中，发起归属确认' });
      entry = await this.confirmOwnership(key, 'cache-miss', trace);
    } else {
      trace.push({ kind: 'cache-hit', at: Date.now(), owner: entry.owner, epoch: entry.epoch, message: `代理使用缓存路由：${entry.owner}@${entry.epoch}` });
    }

    let resp = null;
    for (let attempt = 1; attempt <= 6; attempt++) {
      const shard = this.shards.get(entry.owner);
      resp = shard.handleReadRPC({ key, fence: entry.fence ?? entry.epoch });
      if (resp.ok) {
        trace.push({ kind: 'read-ok', at: Date.now(), attempt, owner: resp.owner, found: resp.found, message: `读取由 ${resp.owner} 提供，found=${resp.found}` });
        const result = {
          ok: true,
          key,
          found: resp.found,
          value: resp.value,
          owner: resp.owner,
          shardName: resp.shardName,
          epoch: resp.epoch,
          attempts: attempt,
          durationMs: Date.now() - startedAt,
          trace,
        };
        this._pushRecent({ at: Date.now(), type: 'read', key, value: result.value, found: result.found, owner: result.owner, ok: true, attempts: attempt, durationMs: result.durationMs });
        return result;
      }
      trace.push({ kind: 'rejected', at: Date.now(), attempt, code: resp.code, owner: entry.owner, message: resp.message });
      if (resp.code === 'NOT_OWNER' || resp.code === 'FENCE_MISMATCH') {
        entry = await this.confirmOwnership(key, resp.code, trace);
        continue;
      }
      if (resp.retryable) {
        await sleep(100);
        continue;
      }
      throw new Error(resp.message || `读取失败 ${resp.code}`);
    }
    throw new Error(`读取 ${key} 多次重试后仍失败`);
  }

  _isStale(rangeId, entry) {
    const r = this.director.ranges.find((x) => x.id === rangeId);
    if (!r) return false;
    return entry.owner !== r.owner || entry.epoch !== r.epoch;
  }

  // 显式注入一条过期缓存（模拟代理一直没刷新的旧路由）。
  injectStale(rangeId = 'range-b') {
    const r = this.director.ranges.find((x) => x.id === rangeId);
    if (!r) throw new Error(`unknown range ${rangeId}`);
    // 故意写成搬迁前的旧属主 / 旧 epoch
    const preOwner = this.migrator?.sourceIdFor?.(rangeId) || (rangeId === 'range-b' ? 'shard-2' : r.owner);
    const stale = { owner: preOwner, epoch: 1, fence: 1, confirmedAt: Date.now() - 60000, via: 'injected-stale', reason: 'fault-injection' };
    this.cache.set(rangeId, stale);
    this.bus.log('fault', `已向代理注入过期路由缓存：${rangeId} → ${preOwner}@1（权威当前 ${r.owner}@${r.epoch}）`, stale, 'warn');
    return stale;
  }

  clearCache() {
    this.cache.clear();
    this.bus.log('proxy', '代理路由缓存已清空');
  }

  cacheSnapshot() {
    return [...this.cache.entries()].map(([rangeId, e]) => {
      const r = this.director.ranges.find((x) => x.id === rangeId);
      return {
        rangeId,
        rangeLabel: r?.label || rangeId,
        cachedOwner: e.owner,
        cachedEpoch: e.epoch,
        via: e.via,
        reason: e.reason || null,
        confirmedAt: e.confirmedAt,
        ageMs: Date.now() - e.confirmedAt,
        authoritativeOwner: r?.owner || null,
        authoritativeEpoch: r?.epoch ?? null,
        configEpoch: this.director.configEpoch,
        stale: this._isStale(rangeId, e),
      };
    });
  }
}
