// 拓扑主管（配置中心）：维护权威键范围路由与全局 configEpoch。
// “确认新的写入归属”最终以这里（或搬迁进程的等价应答）为准。

export class TopologyDirector {
  constructor(bus) {
    this.bus = bus;
    this.configEpoch = 1;
    // 字典序左闭右开；start='' 表示 -∞，end=null 表示 +∞
    this.ranges = [
      { id: 'range-a', label: '范围 A', start: '', end: 'key:0400', owner: 'shard-1', epoch: 1 },
      { id: 'range-b', label: '搬迁范围 B', start: 'key:0400', end: 'key:0800', owner: 'shard-2', epoch: 1 },
      { id: 'range-c', label: '范围 C', start: 'key:0800', end: null, owner: 'shard-3', epoch: 1 },
    ];
    // 进行中 / 最近一次搬迁的元信息（供归属确认返回 migrationPhase）
    this.migrationView = null;
  }

  reset() {
    this.configEpoch = 1;
    this.ranges = [
      { id: 'range-a', label: '范围 A', start: '', end: 'key:0400', owner: 'shard-1', epoch: 1 },
      { id: 'range-b', label: '搬迁范围 B', start: 'key:0400', end: 'key:0800', owner: 'shard-2', epoch: 1 },
      { id: 'range-c', label: '范围 C', start: 'key:0800', end: null, owner: 'shard-3', epoch: 1 },
    ];
    this.migrationView = null;
  }

  resolve(key) {
    for (const r of this.ranges) {
      if (key >= r.start && (r.end == null || key < r.end)) return r;
    }
    // 理论不会发生（存在 +∞ 范围），兜底返回最后一个范围
    return this.ranges[this.ranges.length - 1];
  }

  // 权威归属确认。fence 与“该范围”的 epoch 绑定，目标分片据此拒绝过期令牌；
  // configEpoch 是全局拓扑版本，范围 epoch 才是数据面栅栏令牌。
  ownershipOf(key) {
    const r = this.resolve(key);
    return {
      key,
      rangeId: r.id,
      rangeLabel: r.label,
      start: r.start,
      end: r.end,
      owner: r.owner,
      rangeEpoch: r.epoch,
      epoch: r.epoch,
      configEpoch: this.configEpoch,
      fence: r.epoch,
      migrationPhase: this.migrationView?.phase || null,
      migrationId: this.migrationView?.id || null,
      confirmedAt: Date.now(),
      authority: 'topology-director',
    };
  }

  markMigration(view) {
    this.migrationView = view;
  }

  // 切换提交：原子推进 epoch 并修改范围属主。
  commit(rangeId, newOwner) {
    const r = this.ranges.find((x) => x.id === rangeId);
    if (!r) throw new Error(`unknown range ${rangeId}`);
    const oldOwner = r.owner;
    r.owner = newOwner;
    r.epoch += 1;
    this.configEpoch += 1;
    this.bus.log(
      'topology',
      `拓扑提交：${r.label}（${r.start || '-∞'} ~ ${r.end || '+∞'}）属主 ${oldOwner} → ${newOwner}，configEpoch=${this.configEpoch}`,
      { rangeId, oldOwner, newOwner, configEpoch: this.configEpoch },
      'success',
    );
    return { range: r, configEpoch: this.configEpoch, oldOwner, newOwner };
  }

  snapshot() {
    return {
      configEpoch: this.configEpoch,
      ranges: this.ranges.map((r) => ({ ...r })),
      migrationView: this.migrationView ? { ...this.migrationView } : null,
    };
  }
}
