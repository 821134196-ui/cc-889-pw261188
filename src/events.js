// 内存事件总线：保存最近事件，供页面 / API 轮询展示。
export class EventBus {
  constructor(limit = 400) {
    this.seq = 0;
    this.limit = limit;
    this.events = [];
  }

  log(category, message, detail = undefined, level = 'info') {
    const evt = {
      id: ++this.seq,
      ts: Date.now(),
      level,
      category, // topology | proxy | migration | shard | fault | traffic | system
      message,
      detail: detail ?? null,
    };
    this.events.push(evt);
    if (this.events.length > this.limit) {
      this.events.splice(0, this.events.length - this.limit);
    }
    return evt;
  }

  since(id = 0) {
    return this.events.filter((e) => e.id > id);
  }

  tail(n = 120) {
    return this.events.slice(-n);
  }

  reset() {
    this.seq = 0;
    this.events = [];
  }
}
