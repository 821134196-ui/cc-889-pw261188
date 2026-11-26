// 入口：启动零依赖 HTTP 服务。默认尝试 3000，同时兼容常见评测端口（8000/8080/5173）。
import http from 'node:http';
import { System } from './src/system.js';
import { createHandler } from './src/api.js';

const sys = new System();
const handler = createHandler(sys);

const wanted = [
  process.env.PORT ? Number(process.env.PORT) : null,
  3000,
  8000,
  8080,
  5173,
  4173,
]
  .filter((p) => Number.isInteger(p) && p > 0)
  .filter((p, i, arr) => arr.indexOf(p) === i);

let primary = null;
for (const port of wanted) {
  const server = http.createServer(handler);
  await new Promise((resolve) => {
    server.once('listening', () => {
      if (!primary) primary = port;
      console.log(`[server] 分片 KV 在线搬迁演练台: http://localhost:${port}  (0.0.0.0:${port})`);
      resolve(true);
    });
    server.once('error', (err) => {
      if (err.code === 'EADDRINUSE') console.warn(`[server] 端口 ${port} 被占用，跳过`);
      else console.error(`[server] 监听 ${port} 失败:`, err.message);
      resolve(false);
    });
    server.listen(port, '0.0.0.0');
  });
}

if (!primary) {
  console.error('[server] 所有候选端口均不可用');
  process.exit(1);
}
console.log(`[server] 主入口 http://localhost:${primary}`);

for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => {
    console.log(`[server] 收到 ${sig}，退出`);
    process.exit(0);
  });
}
