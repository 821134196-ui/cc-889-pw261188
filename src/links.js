// 模拟的进程间 RPC 链路与网络分区故障注入点。
// 搬迁进程 / 代理 / 分片在逻辑上是独立角色，所有跨角色调用都经过 Link，
// 因此可以真实地演练“代理与迁移进程失去连接”等场景：调用抛 LinkDownError，
// 调用方凭游标 / 缓存 / 权威配置中心重试与恢复。

export class LinkDownError extends Error {
  constructor(name) {
    super(`链路中断: ${name}`);
    this.code = 'LINK_DOWN';
    this.link = name;
  }
}

export class LinkManager {
  constructor(bus) {
    this.bus = bus;
    // key 形如 'proxy->migrator' | 'migrator->source'
    this.links = {
      'proxy->migrator': { down: false, calls: 0, failures: 0, lastError: null, lastOkAt: null },
      'migrator->source': { down: false, calls: 0, failures: 0, lastError: null, lastOkAt: null },
    };
    this.heartbeat = {
      'proxy->migrator': 'connected',
      'migrator->source': 'connected',
    };
  }

  has(name) {
    return !!this.links[name];
  }

  setDown(name, down, reason = 'manual') {
    const link = this.links[name];
    if (!link) throw new Error(`unknown link ${name}`);
    if (link.down === down) return;
    link.down = down;
    if (down) {
      this.heartbeat[name] = 'disconnected';
      this.bus.log('fault', `网络故障：${name} 连接被切断（${reason}）`, { link: name }, 'warn');
    } else {
      this.heartbeat[name] = 'connected';
      this.bus.log('fault', `网络恢复：${name} 链路重新连通，调用方将断点续传 / 重试`, { link: name }, 'success');
    }
  }

  async blip(name, ms = 2500) {
    this.setDown(name, true, 'transient-blip');
    setTimeout(() => this.setDown(name, false, 'blip-recovered'), ms).unref?.();
  }

  isDown(name) {
    return !!this.links[name]?.down;
  }

  // 通过链路执行一次 RPC。down 时快速失败，便于上层演练重试。
  async call(name, fn) {
    const link = this.links[name];
    if (!link) throw new Error(`unknown link ${name}`);
    link.calls++;
    if (link.down) {
      link.failures++;
      link.lastError = 'LINK_DOWN';
      throw new LinkDownError(name);
    }
    // 很小的异步边界，保证调用方必须按异步 RPC 处理。
    await Promise.resolve();
    try {
      const out = await fn();
      link.lastOkAt = Date.now();
      link.lastError = null;
      this.heartbeat[name] = 'connected';
      return out;
    } catch (err) {
      link.failures++;
      link.lastError = err?.message || String(err);
      throw err;
    }
  }

  snapshot() {
    const out = {};
    for (const [name, l] of Object.entries(this.links)) {
      out[name] = {
        down: l.down,
        state: l.down ? 'disconnected' : this.heartbeat[name] || 'connected',
        calls: l.calls,
        failures: l.failures,
        lastError: l.lastError,
        lastOkAt: l.lastOkAt,
      };
    }
    return out;
  }

  reset() {
    for (const name of Object.keys(this.links)) {
      this.links[name] = { down: false, calls: 0, failures: 0, lastError: null, lastOkAt: null };
      this.heartbeat[name] = 'connected';
    }
  }
}
