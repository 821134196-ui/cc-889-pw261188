# 分片 KV · 键范围在线搬迁演练台

一个**零第三方依赖**的精简分片键值存储，演示对某个键范围做运行中搬迁（live migration）：

- **先复制，后追增量**：搬迁进程先对范围做有序全量扫描复制（带 `lastKey` 检查点），再 tail 源分片单调 op-log 回放增量（按 seq 幂等）。
- **可暂停 / 继续**：暂停在批次 / 轮询检查点生效；暂停期间写入仍由当前权威属主受理并进 op-log，恢复后自动追平。
- **断连可恢复**：
  - `搬迁进程 ↔ 源分片` 断开：保留扫描位点与 oplog 游标，快速失败 + 退避重试，恢复后断点续传，不重复、不丢失。
  - `代理 ↔ 搬迁进程` 断开：归属确认先问搬迁进程，失败重试后回退到权威拓扑主管，数据面写入不受影响。
- **旧路由缓存下确认新写入归属**：代理长期缓存路由。切换提交推进范围 epoch / fence 后，持有旧路由的代理写入会被旧属主以 `NOT_OWNER` / `FENCE_MISMATCH` 拒绝；代理随后重新确认权威归属、刷新缓存并用新 fence 重试，对客户端返回成功，响应中带完整 `trace`。
- **页面可暂停搬迁、实写键值、核对分片状态与读结果**：内置分片物理数据表面板、单键三方核对（代理读 / 权威属主 / 逻辑真值 + 物理副本）、全量巡检、事件流与故障注入开关。

## 快速开始

```bash
npm start          # 等价于 node server.js
# 或：PORT=3000 node server.js
```

服务会同时尝试监听 `$PORT`（若指定）以及 `3000 / 8000 / 8080 / 5173 / 4173`（占用则跳过），启动后打开任一可用端口，例如：

```
http://localhost:3000/
```

需要 Node.js >= 18（使用全局 `fetch` 跑测试；服务本身仅用内置模块）。**无需 `npm install`**。

运行端到端不变量测试（会自启独立端口的服务）：

```bash
npm test
```

## 60 秒演练脚本

1. 打开页面，搬迁已自动开始：`shard-2 → shard-3` 搬迁范围 `[key:0400, key:0800)`，可看到全量复制进度与 `shard-3` 物理行增长。
2. 等状态进入 **“增量追平 / 监听中”**，点 **暂停搬迁**。
3. 在左侧输入 `key:0501` 和任意值，点 **写入键值**：
   - 返回成功，物理属主是 `shard-2`（暂停期源仍是权威属主）；
   - 页面自动实读并 **核对分片**：代理读 = 属主数据 = 已确认写入真值；
   - `shard-3` 仍显示旧值——这是预期的复制延迟，恢复后收敛。
4. 点 **继续搬迁**，打开 **“代理 ↔ 搬迁进程 失去连接”** 开关。
5. 点 **完成切换**：短暂写栅栏排空最后增量 → 拓扑 epoch `1 → 2` → `shard-3` 成为权威属主。
6. 保持故障开关打开（代理连不上搬迁进程），直接写 `key:0666`：
   - 轨迹显示：缓存命中旧路由 `shard-2@1` → 被 `NOT_OWNER` 拒绝 → 询问搬迁进程连续链路失败 → **回退拓扑主管确认 `shard-3@2`** → 重试写入 `shard-3` 成功。
7. 点 **核对分片**、查看三个分片表与读结果；关闭故障开关，再跑 **全量一致性巡检**（全部已确认键与权威属主物理数据一致）。
8. 切换后旧源分片保留 `moved-away` 旧副本（不参与读写），可用 **清理源旧副本（GC）** 物理删除。
9. 点右上角 **重置演示** 可随时回到初始播种状态并重新自动搬迁。

## HTTP / JSON API

所有接口返回 JSON，已开放 CORS。

### 数据面（经代理，带缓存路由与自愈）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/kv` | body `{ "key", "value" }` 写入 |
| `PUT/POST` | `/api/kv/:key` | 写入（JSON `{value}` 或纯文本 body） |
| `GET` | `/api/kv/:key` 或 `/api/kv?key=` | 读取（不存在时 `found:false`，HTTP 200） |
| `DELETE` | `/api/kv/:key` | 删除 |

写 / 读响应包含 `owner`（物理属主）、`attempts`、`epoch`、`neededReconfirm` 与 `trace[]`（缓存命中、拒绝、断连、归属确认、重试成功的完整轨迹）。

### 核对 / 巡检

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET/POST` | `/api/verify?key=` / `/api/verify` | 代理读、权威属主物理数据、逻辑真值、各分片副本四方核对 |
| `POST` | `/api/audit` | 全量巡检：所有已确认写入键 vs 当前权威属主物理数据 |
| `GET` | `/api/shards/:id/entries?scope=migration-range&limit=120` | 查看分片在搬迁范围内的真实物理键值 |
| `GET` | `/api/shards/:id/entries?start=&end=` | 自定义字典序范围 |

### 拓扑 / 归属

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/api/topology` | 权威范围路由与全局 configEpoch |
| `GET` | `/api/ownership?key=` | **权威归属确认**：属主、范围 epoch、fence 令牌 |
| `POST` | `/api/proxy/confirm` | body `{key}`：代理执行一次归属确认并刷新缓存，返回 trace |
| `GET` | `/api/state` | 页面所需的完整状态快照（拓扑 / 分片 / 搬迁 / 缓存 / 链路 / 事件） |

### 搬迁控制

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/migration/start` | 可选 body `{rangeId,start,end,sourceId,targetId}` |
| `POST` | `/api/migration/pause` | 暂停并等待检查点确认（最多约 2.5s） |
| `POST` | `/api/migration/resume` | 从 `lastKey` / oplog seq 继续 |
| `POST` | `/api/migration/cutover` | 追平增量并原子提交（snapshot 阶段返回 409） |
| `POST` | `/api/migration/gc` | 提交后清理旧源分片上的范围遗留副本 |
| `POST` | `/api/demo/reset` | 重置并重新播种（body `{autostart:false}` 可不自动搬迁） |

### 故障注入 / 代理缓存 / 流量

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/faults` | `{ "link": "proxy->migrator"\|"migrator->source", "down": true\|false }` |
| `POST` | `/api/faults/blip` | `{ "link", "ms": 2500 }` 瞬断后自动恢复 |
| `POST` | `/api/proxy/stale` | `{ "rangeId" }` 向代理注入一条旧路由（默认 `range-b → shard-2@1`） |
| `POST` | `/api/proxy/clear` | 清空代理缓存 |
| `POST` | `/api/proxy/aggressive` | `{ "enabled" }` 每秒主动对齐拓扑（默认关闭以复现旧缓存） |
| `POST` | `/api/traffic` | `{ "enabled": true, "intervalMs": 850 }` 开启持续实写流量 |
| `GET` | `/api/health` | 健康检查 |

## 一致性与归属协议（精简版）

- 路由按字典序左闭右开划分三个范围；`key:0001..key:1080` 补零播种，其中 `[key:0400,key:0800)` 属 `shard-2`，搬迁到 `shard-3`。
- 每个范围有独立单调 **range epoch**，作为数据面 **fence 令牌**；全局另有 `configEpoch` 表示拓扑版本。
- 搬迁期间源分片保持写属主，所有写入追加单调 oplog；目标通过快照 + oplog 幂等重放追平。
- 提交顺序：源分片置写栅栏 → 排空最后 oplog → 拓扑主管推进 epoch 并换属主 → 写入两节点归属表（源 `moved-away` 重定向、目标 `owner+fence`）→ 解除栅栏。栅栏窗口内的写请求收到可重试错误，由代理重试。
- 节点在处理代理 RPC 时**独立校验归属与 fence**，因此正确性不依赖代理缓存是否新鲜。

## 目录结构

```
server.js            入口：多端口 HTTP 监听
src/system.js        装配 / 播种 / 状态快照 / verify / audit / GC
src/topology.js      权威范围路由 + epoch + 归属确认
src/store.js         ShardNode：内存 KV + oplog + 归属/fence 校验
src/proxy.js         代理：路由缓存、归属确认、拒绝重试、trace
src/migrator.js      搬迁状态机：复制 / 追增量 / 暂停 / 断连续传 / 提交
src/links.js         RPC 链路与故障注入
src/events.js        事件总线
src/api.js           JSON API + 静态资源
public/              演练台页面（原生 HTML/CSS/JS，无构建）
test/smoke.mjs       31 项端到端不变量断言
```

数据为内存态，重启即恢复初始播种。
