var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// relay/worker.js
import { DurableObject } from "cloudflare:workers";
var MAX_MSG = 1e6;
var CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, PUT, POST, DELETE, OPTIONS",
  "access-control-allow-headers": "content-type",
  "access-control-max-age": "86400"
};
var worker_default = {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Focus Jams sync server is running.", { headers: { "content-type": "text/plain", ...CORS } });
    }
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    const ar = url.pathname.match(/^\/archive\/([a-z0-9]{16,40})(\/.*)?$/i);
    if (ar) {
      const box = env.ARCHIVES.get(env.ARCHIVES.idFromName(ar[1].toLowerCase()));
      return box.fetch(req);
    }
    const m = url.pathname.match(/^\/room\/([a-z0-9]{6,16})$/i);
    if (!m) return new Response("Not found", { status: 404, headers: CORS });
    if (req.headers.get("Upgrade") !== "websocket") return new Response("Expected a WebSocket", { status: 426, headers: CORS });
    const room = env.ROOMS.get(env.ROOMS.idFromName(m[1].toLowerCase()));
    return room.fetch(req);
  }
};
var Room = class extends DurableObject {
  static {
    __name(this, "Room");
  }
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.latest = null;
    this.lastSave = 0;
  }
  async fetch(req) {
    const url = new URL(req.url);
    const role = url.searchParams.get("role") === "host" ? "host" : "phone";
    const id = (url.searchParams.get("id") || "").replace(/[^\w-]/g, "").slice(0, 40) || crypto.randomUUID();
    const [client, server] = Object.values(new WebSocketPair());
    for (const old of this.ctx.getWebSockets(role === "host" ? "host" : "p:" + id)) {
      try {
        old.close(4e3, "replaced");
      } catch (e) {
      }
    }
    this.ctx.acceptWebSocket(server, role === "host" ? ["host"] : ["phone", "p:" + id]);
    server.serializeAttachment({ role, id });
    if (role === "phone") {
      const st = this.latest || await this.ctx.storage.get("state");
      if (st) server.send(st);
      server.send(JSON.stringify({ t: "_host", on: this.ctx.getWebSockets("host").length > 0 }));
      this.toHost({ k: "join", id });
    } else {
      for (const ws of this.ctx.getWebSockets("phone")) {
        const a = ws.deserializeAttachment();
        if (a) server.send(JSON.stringify({ k: "join", id: a.id }));
      }
      this.toPhones(JSON.stringify({ t: "_host", on: true }));
    }
    return new Response(null, { status: 101, webSocket: client });
  }
  toHost(obj) {
    const s = typeof obj === "string" ? obj : JSON.stringify(obj);
    for (const ws of this.ctx.getWebSockets("host")) {
      try {
        ws.send(s);
      } catch (e) {
      }
    }
  }
  toPhones(s, tag = "phone") {
    for (const ws of this.ctx.getWebSockets(tag)) {
      try {
        ws.send(s);
      } catch (e) {
      }
    }
  }
  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > MAX_MSG) return;
    let o;
    try {
      o = JSON.parse(msg);
    } catch (e) {
      return;
    }
    const a = ws.deserializeAttachment() || {};
    if (a.role === "host") {
      if (o.k === "all" && o.m) {
        const s = JSON.stringify(o.m);
        this.latest = s;
        this.toPhones(s);
        const now = Date.now();
        if (now - this.lastSave > 3e4) {
          this.lastSave = now;
          await this.ctx.storage.put("state", s);
        }
      } else if (o.k === "to" && o.id && o.m) {
        this.toPhones(JSON.stringify(o.m), "p:" + String(o.id).replace(/[^\w-]/g, ""));
      }
    } else {
      this.toHost({ k: "from", id: a.id, m: o });
    }
  }
  async webSocketClose(ws, code, reason) {
    this.gone(ws);
  }
  async webSocketError(ws) {
    this.gone(ws);
  }
  gone(ws) {
    const a = ws.deserializeAttachment() || {};
    if (a.role === "phone") {
      if (!this.ctx.getWebSockets("p:" + a.id).some((x) => x !== ws)) this.toHost({ k: "leave", id: a.id });
    } else if (this.ctx.getWebSockets("host").filter((x) => x !== ws).length === 0) this.toPhones(JSON.stringify({ t: "_host", on: false }));
    try {
      ws.close(1e3, "bye");
    } catch (e) {
    }
  }
};
var CHUNK = 9e5;
var MAX_PHOTO = 6e6;
var MAX_TOTAL = 4e9;
var MAX_PHOTOS = 2e4;
var json = /* @__PURE__ */ __name((o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json", ...CORS } }), "json");
var cleanId = /* @__PURE__ */ __name((v) => String(v || "").replace(/[^\w-]/g, "").slice(0, 60), "cleanId");
var Archive = class extends DurableObject {
  static {
    __name(this, "Archive");
  }
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS nights (id TEXT PRIMARY KEY, ended INTEGER, updated INTEGER, data TEXT);
      CREATE TABLE IF NOT EXISTS photos (id TEXT PRIMARY KEY, night TEXT, ts INTEGER, meta TEXT, thumb TEXT, size INTEGER, parts INTEGER, updated INTEGER);
      CREATE INDEX IF NOT EXISTS photos_night ON photos(night);
      CREATE TABLE IF NOT EXISTS parts (photo TEXT, i INTEGER, data BLOB, PRIMARY KEY (photo, i));
      CREATE TABLE IF NOT EXISTS deleted (id TEXT PRIMARY KEY, at INTEGER);`);
  }
  isDeleted(id) {
    return this.sql.exec("SELECT 1 FROM deleted WHERE id = ?", id).toArray().length > 0;
  }
  stats() {
    const r = this.sql.exec("SELECT COUNT(*) AS n, COALESCE(SUM(size), 0) AS b FROM photos").one();
    const nn = this.sql.exec("SELECT COUNT(*) AS n FROM nights").one();
    return { nights: nn.n, photos: r.n, bytes: r.b };
  }
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname.replace(/^\/archive\/[^/]+/, "") || "/";
    const now = Date.now();
    try {
      if (req.method === "GET" && path === "/nights") {
        const since = +url.searchParams.get("since") || 0;
        const ids = {};
        for (const r of this.sql.exec("SELECT id, ended FROM nights")) ids[r.id] = r.ended;
        const nights = this.sql.exec("SELECT data FROM nights WHERE updated > ?", since).toArray().map((r) => JSON.parse(r.data));
        const photos = this.sql.exec("SELECT id FROM photos").toArray().map((r) => r.id);
        const deleted = this.sql.exec("SELECT id FROM deleted").toArray().map((r) => r.id);
        return json({ ids, nights, photos, deleted, now, stats: this.stats() });
      }
      if (req.method === "PUT" && path === "/night") {
        const n = await req.json();
        const id = cleanId(n && n.id);
        if (!id || typeof n !== "object") return json({ error: "bad night" }, 400);
        if (this.isDeleted(id)) return json({ ok: false, deleted: true });
        const data = JSON.stringify(n);
        if (data.length > 5e5) return json({ error: "too big" }, 413);
        const ended = +n.end || +n.start || 0;
        const old = this.sql.exec("SELECT ended FROM nights WHERE id = ?", id).toArray()[0];
        if (!old || ended >= old.ended) this.sql.exec("INSERT OR REPLACE INTO nights (id, ended, updated, data) VALUES (?, ?, ?, ?)", id, ended, now, data);
        return json({ ok: true });
      }
      if (req.method === "GET" && path === "/photos") {
        const night = cleanId(url.searchParams.get("night"));
        const rows = this.sql.exec("SELECT id, night, ts, meta, thumb FROM photos WHERE night = ? ORDER BY ts DESC", night).toArray();
        return json(rows.map((r) => Object.assign({ id: r.id, night: r.night, ts: r.ts, thumb: r.thumb }, JSON.parse(r.meta || "{}"))));
      }
      if (req.method === "PUT" && path === "/photo") {
        const p = await req.json();
        const id = cleanId(p && p.id), night = cleanId(p && p.night);
        if (!id || !night || typeof p.data !== "string" || !p.data.startsWith("data:image/")) return json({ error: "bad photo" }, 400);
        if (this.isDeleted(id) || this.isDeleted(night)) return json({ ok: false, deleted: true });
        if (this.sql.exec("SELECT 1 FROM photos WHERE id = ?", id).toArray().length) return json({ ok: true, had: true });
        const bytes = Uint8Array.from(atob(p.data.slice(p.data.indexOf(",") + 1)), (c) => c.charCodeAt(0));
        if (bytes.length > MAX_PHOTO) return json({ error: "photo too big" }, 413);
        const st = this.stats();
        if (st.photos >= MAX_PHOTOS || st.bytes + bytes.length > MAX_TOTAL) return json({ error: "archive full" }, 507);
        const parts = Math.ceil(bytes.length / CHUNK);
        for (let i = 0; i < parts; i++) this.sql.exec("INSERT OR REPLACE INTO parts (photo, i, data) VALUES (?, ?, ?)", id, i, bytes.slice(i * CHUNK, (i + 1) * CHUNK).buffer);
        const meta = JSON.stringify({ name: String(p.name || "").slice(0, 80), emoji: String(p.emoji || "").slice(0, 8), from: cleanId(p.from) });
        const thumb = typeof p.thumb === "string" && p.thumb.startsWith("data:image/") && p.thumb.length < 4e5 ? p.thumb : "";
        this.sql.exec("INSERT INTO photos (id, night, ts, meta, thumb, size, parts, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", id, night, +p.ts || now, meta, thumb, bytes.length, parts, now);
        return json({ ok: true });
      }
      let m = path.match(/^\/photo\/([\w-]+)$/);
      if (req.method === "GET" && m) {
        const ph = this.sql.exec("SELECT parts FROM photos WHERE id = ?", m[1]).toArray()[0];
        if (!ph) return json({ error: "not found" }, 404);
        const chunks = this.sql.exec("SELECT data FROM parts WHERE photo = ? ORDER BY i", m[1]).toArray().map((r) => new Uint8Array(r.data));
        return new Response(new Blob(chunks, { type: "image/jpeg" }), { headers: { "content-type": "image/jpeg", "cache-control": "private, max-age=31536000, immutable", ...CORS } });
      }
      m = path.match(/^\/night\/([\w-]+)$/);
      if (req.method === "DELETE" && m) {
        const id = m[1];
        const ph = this.sql.exec("SELECT id FROM photos WHERE night = ?", id).toArray().map((r) => r.id);
        for (const pid of ph) {
          this.sql.exec("DELETE FROM parts WHERE photo = ?", pid);
          this.sql.exec("INSERT OR REPLACE INTO deleted (id, at) VALUES (?, ?)", pid, now);
        }
        this.sql.exec("DELETE FROM photos WHERE night = ?", id);
        this.sql.exec("DELETE FROM nights WHERE id = ?", id);
        this.sql.exec("INSERT OR REPLACE INTO deleted (id, at) VALUES (?, ?)", id, now);
        return json({ ok: true });
      }
      if (req.method === "GET" && path === "/") return json({ ok: true, stats: this.stats() });
      return json({ error: "not found" }, 404);
    } catch (e) {
      return json({ error: String(e && e.message || e) }, 500);
    }
  }
};

// ../../../tmp/claude-0/-home-claude/41a7e138-24e2-5a71-b6c2-5bfc9ad6696f/scratchpad/wr/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// ../../../tmp/claude-0/-home-claude/41a7e138-24e2-5a71-b6c2-5bfc9ad6696f/scratchpad/wr/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    const body = JSON.stringify(error);
    const headers = {
      "Content-Type": "application/json",
      "MF-Experimental-Error-Stack": "true"
    };
    const encoded = encodeURIComponent(body);
    if (encoded.length <= 8192) {
      headers["MF-Experimental-Error-Stack-Payload"] = encoded;
    }
    return new Response(body, { status: 500, headers });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-o1ehAp/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = worker_default;

// ../../../tmp/claude-0/-home-claude/41a7e138-24e2-5a71-b6c2-5bfc9ad6696f/scratchpad/wr/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-o1ehAp/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  scheduledTime;
  cron;
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  Archive,
  Room,
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=worker.js.map
