// HTTP 路由：JSON API + 静态页面。零第三方依赖。

import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC_DIR = path.resolve(__dirname, '..', 'public');

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.ico': 'image/x-icon',
};

function sendJSON(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'access-control-allow-origin': '*',
  });
  res.end(body);
}

async function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > 2_000_000) {
        reject(Object.assign(new Error('请求体过大'), { statusCode: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

async function parsePayload(req) {
  const raw = await readBody(req);
  if (!raw) return {};
  const ct = (req.headers['content-type'] || '').toLowerCase();
  if (ct.includes('application/json')) {
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch {
      throw Object.assign(new Error('请求体不是合法 JSON'), { statusCode: 400 });
    }
    // 兼容裸 JSON 字符串 / 数字作为 value
    if (parsed == null || typeof parsed !== 'object') return { value: parsed == null ? '' : String(parsed) };
    return parsed;
  }
  if (ct.includes('application/x-www-form-urlencoded')) {
    return Object.fromEntries(new URLSearchParams(raw));
  }
  // 允许纯文本作为 value
  return { value: raw, raw: true };
}

async function serveStatic(req, res, pathname) {
  let rel = pathname === '/' ? 'index.html' : pathname.replace(/^\/+/, '');
  const filePath = path.resolve(PUBLIC_DIR, rel);
  if (!filePath.startsWith(PUBLIC_DIR + path.sep) && filePath !== path.join(PUBLIC_DIR, 'index.html')) {
    res.writeHead(403);
    return res.end('Forbidden');
  }
  try {
    const data = await fs.promises.readFile(filePath);
    res.writeHead(200, {
      'content-type': MIME[path.extname(filePath)] || 'application/octet-stream',
      'cache-control': 'no-cache, no-store, must-revalidate',
    });
    res.end(data);
  } catch {
    // SPA-ish fallback：未知非 /api 路径返回首页，方便直接刷新
    try {
      const index = await fs.promises.readFile(path.join(PUBLIC_DIR, 'index.html'));
      res.writeHead(200, { 'content-type': MIME['.html'], 'cache-control': 'no-store' });
      res.end(index);
    } catch {
      res.writeHead(404, { 'content-type': 'text/plain; charset=utf-8' });
      res.end('Not Found');
    }
  }
}

export function createHandler(sys) {
  return async (req, res) => {
    const url = new URL(req.url, 'http://localhost');
    const pathname = decodeURIComponent(url.pathname);
    const method = req.method || 'GET';

    res.setHeader('access-control-allow-origin', '*');
    res.setHeader('access-control-allow-methods', 'GET,POST,PUT,DELETE,OPTIONS');
    res.setHeader('access-control-allow-headers', 'content-type');
    if (method === 'OPTIONS') {
      res.writeHead(204);
      return res.end();
    }

    try {
      // ---------- 健康检查 / 全量状态 ----------
      if (method === 'GET' && pathname === '/api/health') {
        return sendJSON(res, 200, { ok: true, service: 'sharded-kv-live-migration', now: Date.now() });
      }
      if (method === 'GET' && pathname === '/api/state') {
        return sendJSON(res, 200, sys.state());
      }
      if (method === 'GET' && pathname === '/api/topology') {
        return sendJSON(res, 200, sys.director.snapshot());
      }

      // ---------- 权威归属确认 ----------
      if (method === 'GET' && pathname === '/api/ownership') {
        const key = url.searchParams.get('key') || '';
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key 参数' });
        return sendJSON(res, 200, { ok: true, ownership: sys.director.ownershipOf(key) });
      }

      // ---------- KV 数据面（经代理）----------
      if (method === 'POST' && pathname === '/api/kv') {
        const body = await parsePayload(req);
        const key = String(body.key || '').trim();
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key' });
        const result = await sys.writeKV(key, body.value ?? '');
        return sendJSON(res, 200, result);
      }

      let m;
      if ((m = pathname.match(/^\/api\/kv\/(.+)$/))) {
        const key = m[1];
        if (method === 'GET') {
          const result = await sys.readKV(key);
          // 键不存在也返回 200，用 found:false 表达，降低自动化脚本的状态码耦合
          return sendJSON(res, 200, result);
        }
        if (method === 'PUT' || method === 'POST') {
          const body = await parsePayload(req);
          const value = typeof body.value === 'undefined' ? '' : body.value;
          const result = await sys.writeKV(key, value);
          return sendJSON(res, 200, result);
        }
        if (method === 'DELETE') {
          const result = await sys.deleteKV(key);
          return sendJSON(res, 200, result);
        }
      }

      // GET /api/kv?key=xxx
      if (method === 'GET' && pathname === '/api/kv') {
        const key = url.searchParams.get('key') || '';
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key 参数' });
        const result = await sys.readKV(key);
        return sendJSON(res, 200, result);
      }

      // ---------- 核对 / 巡检 ----------
      if (method === 'GET' && pathname === '/api/verify') {
        const key = url.searchParams.get('key') || '';
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key 参数' });
        return sendJSON(res, 200, await sys.verifyKey(key));
      }
      if (method === 'POST' && pathname === '/api/verify') {
        const body = await parsePayload(req);
        if (!body.key) return sendJSON(res, 400, { ok: false, error: '缺少 key' });
        return sendJSON(res, 200, await sys.verifyKey(String(body.key)));
      }
      if (method === 'POST' && pathname === '/api/audit') {
        return sendJSON(res, 200, sys.audit());
      }

      // ---------- 分片物理数据 ----------
      if ((m = pathname.match(/^\/api\/shards\/([\w-]+)\/entries$/)) && method === 'GET') {
        const data = sys.shardEntries(m[1], {
          scope: url.searchParams.get('scope') || undefined,
          start: url.searchParams.get('start') || undefined,
          end: url.searchParams.get('end') || undefined,
          limit: url.searchParams.get('limit') || undefined,
        });
        if (!data) return sendJSON(res, 404, { ok: false, error: '分片不存在' });
        return sendJSON(res, 200, { ok: true, shard: m[1], ...data });
      }

      // ---------- 搬迁控制 ----------
      if (method === 'POST' && pathname === '/api/migration/start') {
        const body = await parsePayload(req).catch(() => ({}));
        try {
          const snap = await sys.migrator.start(body || {});
          return sendJSON(res, 200, { ok: true, migration: snap });
        } catch (err) {
          return sendJSON(res, err.code === 'MIGRATION_BUSY' ? 409 : 400, { ok: false, error: err.message, code: err.code });
        }
      }
      if (method === 'POST' && pathname === '/api/migration/pause') {
        const snap = await sys.migrator.pause();
        return sendJSON(res, 200, { ok: true, migration: snap });
      }
      if (method === 'POST' && pathname === '/api/migration/resume') {
        return sendJSON(res, 200, { ok: true, migration: sys.migrator.resume() });
      }
      if (method === 'POST' && pathname === '/api/migration/cutover') {
        try {
          const snap = sys.migrator.requestCutover();
          return sendJSON(res, 200, { ok: true, migration: snap });
        } catch (err) {
          return sendJSON(res, err.code === 'SNAPSHOT_NOT_DONE' ? 409 : 400, { ok: false, error: err.message, code: err.code });
        }
      }
      if (method === 'POST' && pathname === '/api/migration/gc') {
        const result = sys.gcSource();
        return sendJSON(res, 200, { ok: true, ...result });
      }

      // ---------- 演示重置 ----------
      if (method === 'POST' && pathname === '/api/demo/reset') {
        const body = await parsePayload(req).catch(() => ({}));
        const state = sys.reset({ autostart: body.autostart !== false });
        return sendJSON(res, 200, { ok: true, state });
      }

      // ---------- 故障注入 ----------
      if (method === 'POST' && pathname === '/api/faults') {
        const body = await parsePayload(req);
        const link = body.link;
        if (!sys.links.has(link)) return sendJSON(res, 400, { ok: false, error: `未知链路 ${link}` });
        sys.links.setDown(link, !!body.down, body.reason || 'manual-api');
        return sendJSON(res, 200, { ok: true, links: sys.links.snapshot() });
      }
      if (method === 'POST' && pathname === '/api/faults/blip') {
        const body = await parsePayload(req);
        const link = body.link;
        if (!sys.links.has(link)) return sendJSON(res, 400, { ok: false, error: `未知链路 ${link}` });
        await sys.links.blip(link, Number(body.ms) || 2500);
        return sendJSON(res, 200, { ok: true, links: sys.links.snapshot() });
      }

      // ---------- 代理缓存控制 ----------
      if (method === 'POST' && pathname === '/api/proxy/stale') {
        const body = await parsePayload(req).catch(() => ({}));
        try {
          const entry = sys.proxy.injectStale(body.rangeId || 'range-b');
          return sendJSON(res, 200, { ok: true, entry, cache: sys.proxy.cacheSnapshot() });
        } catch (err) {
          return sendJSON(res, 400, { ok: false, error: err.message });
        }
      }
      if (method === 'POST' && pathname === '/api/proxy/clear') {
        sys.proxy.clearCache();
        return sendJSON(res, 200, { ok: true, cache: sys.proxy.cacheSnapshot() });
      }
      if (method === 'POST' && pathname === '/api/proxy/aggressive') {
        const body = await parsePayload(req);
        sys.proxy.aggressiveInvalidation = !!body.enabled;
        sys.bus.log('proxy', `代理主动刷新路由缓存：${sys.proxy.aggressiveInvalidation ? '开启（每秒与配置中心对齐）' : '关闭（依赖 NOT_OWNER 被动确认）'}`);
        return sendJSON(res, 200, { ok: true, aggressiveInvalidation: sys.proxy.aggressiveInvalidation });
      }
      if (method === 'POST' && pathname === '/api/proxy/confirm') {
        const body = await parsePayload(req);
        const key = String(body.key || '');
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key' });
        const trace = [];
        const entry = await sys.proxy.confirmOwnership(key, body.reason || 'manual-confirm', trace);
        return sendJSON(res, 200, { ok: true, entry, trace, cache: sys.proxy.cacheSnapshot() });
      }

      // ---------- 持续流量 ----------
      if (method === 'POST' && pathname === '/api/traffic') {
        const body = await parsePayload(req);
        return sendJSON(res, 200, { ok: true, traffic: sys.setTraffic(!!body.enabled, body.intervalMs) });
      }

      // ---------- 常见风格别名（提升与不同检查脚本的兼容性）----------
      if (method === 'GET' && (pathname === '/api/status' || pathname === '/status')) {
        return sendJSON(res, 200, sys.state());
      }
      if (method === 'GET' && (pathname === '/api/migration/status' || pathname === '/migration/status')) {
        return sendJSON(res, 200, { ok: true, migration: sys.migrator.snapshot() });
      }
      if (method === 'GET' && (pathname === '/get' || pathname === '/api/get')) {
        const key = url.searchParams.get('key') || '';
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key 参数' });
        const result = await sys.readKV(key);
        return sendJSON(res, 200, result);
      }
      if (method === 'POST' && (pathname === '/get' || pathname === '/api/get')) {
        const body = await parsePayload(req);
        if (!body.key) return sendJSON(res, 400, { ok: false, error: '缺少 key' });
        return sendJSON(res, 200, await sys.readKV(String(body.key)));
      }
      if ((method === 'POST' || method === 'GET') && (pathname === '/set' || pathname === '/api/set')) {
        let key; let value;
        if (method === 'GET') {
          key = url.searchParams.get('key') || '';
          value = url.searchParams.get('value') ?? '';
        } else {
          const body = await parsePayload(req);
          key = String(body.key || '');
          value = body.value ?? '';
        }
        if (!key) return sendJSON(res, 400, { ok: false, error: '缺少 key' });
        const result = await sys.writeKV(key, value);
        return sendJSON(res, 200, result);
      }

      if (pathname.startsWith('/api/')) {
        return sendJSON(res, 404, { ok: false, error: `无此 API：${method} ${pathname}` });
      }

      return await serveStatic(req, res, pathname);
    } catch (err) {
      const status = err.statusCode || 500;
      if (status >= 500) sys.bus.log('system', `API 错误：${err.message}`, { stack: err.stack?.split('\n').slice(0, 3) }, 'error');
      return sendJSON(res, status, { ok: false, error: err.message || '服务器内部错误', code: err.code || null });
    }
  };
}
