// Focus Jams sync server — runs free on Cloudflare Workers.
//
// Each projector's sync code gets its own "room" (a Durable Object). The projector ("host") and the phones
// connect to the room with WebSockets:
//
//   wss://<this server>/room/<code>?role=host
//   wss://<this server>/room/<code>?role=phone&id=<phone id>
//
// Messages the host sends:
//   {k:"all", m:<state>}   → saved as the latest state and sent to every phone
//   {k:"to", id, m:<msg>}  → sent to one phone (album photos, collage, …)
// Anything a phone sends is passed to the host as {k:"from", id, m:<msg>}.
// The host is told {k:"join", id} / {k:"leave", id} as phones come and go.
//
// The room uses WebSocket hibernation, so it costs (almost) nothing while nobody is sending anything,
// and "ping" frames are answered automatically without waking it up.

import { DurableObject } from "cloudflare:workers";

const MAX_MSG = 1_000_000;   // Cloudflare's WebSocket message limit is 1 MiB
const CORS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, PUT, POST, DELETE, OPTIONS",
  "access-control-allow-headers": "content-type",
  "access-control-max-age": "86400",
};

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Focus Jams sync server is running.", { headers: { "content-type": "text/plain", ...CORS } });
    }
    if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: CORS });
    // shared archive: /archive/<secret key>/...
    const ar = url.pathname.match(/^\/archive\/([a-z0-9]{16,40})(\/.*)?$/i);
    if (ar) {
      const box = env.ARCHIVES.get(env.ARCHIVES.idFromName(ar[1].toLowerCase()));
      return box.fetch(req);
    }
    // read-only view of an archive (for phones): /view/<token>/nights | /photos?night= | /photo/<id>
    const vw = url.pathname.match(/^\/view\/([a-f0-9]{32})(\/.*)?$/);
    if (vw) {
      if (req.method !== "GET") return new Response("Read-only", { status: 405, headers: CORS });
      const rest = vw[2] || "/";
      if (!/^\/(nights|photos|photo\/[\w-]+)?$/.test(rest)) return new Response("Not found", { status: 404, headers: CORS });
      const map = env.ARCHIVES.get(env.ARCHIVES.idFromName("view:" + vw[1]));
      const t = await (await map.fetch(new Request(url.origin + "/__target"))).json().catch(() => ({}));
      if (!t.key) return json({ error: "not found" }, 404);
      const box = env.ARCHIVES.get(env.ARCHIVES.idFromName(t.key));
      return box.fetch(new Request(url.origin + "/archive/" + t.key + rest + url.search, { method: "GET" }));
    }
    const m = url.pathname.match(/^\/room\/([a-z0-9]{6,16})$/i);
    if (!m) return new Response("Not found", { status: 404, headers: CORS });
    if (req.headers.get("Upgrade") !== "websocket") return new Response("Expected a WebSocket", { status: 426, headers: CORS });
    const room = env.ROOMS.get(env.ROOMS.idFromName(m[1].toLowerCase()));
    return room.fetch(req);
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    // keep-alive pings from clients are answered without waking the room
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
    this.latest = null;   // latest state from the host (also saved to storage now and then)
    this.lastSave = 0;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const role = url.searchParams.get("role") === "host" ? "host" : "phone";
    const id = (url.searchParams.get("id") || "").replace(/[^\w-]/g, "").slice(0, 40) || crypto.randomUUID();
    const [client, server] = Object.values(new WebSocketPair());

    // one connection per projector / per phone: a reconnect replaces the old one
    for (const old of this.ctx.getWebSockets(role === "host" ? "host" : "p:" + id)) {
      try { old.close(4000, "replaced"); } catch (e) {}
    }
    this.ctx.acceptWebSocket(server, role === "host" ? ["host"] : ["phone", "p:" + id]);
    server.serializeAttachment({ role, id });

    if (role === "phone") {
      const st = this.latest || (await this.ctx.storage.get("state"));
      if (st) server.send(st);
      server.send(JSON.stringify({ t: "_host", on: this.ctx.getWebSockets("host").length > 0 }));
      this.toHost({ k: "join", id });
    } else {
      // tell the (new) projector who is already here, and tell phones it's back
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
    for (const ws of this.ctx.getWebSockets("host")) { try { ws.send(s); } catch (e) {} }
  }
  toPhones(s, tag = "phone") {
    for (const ws of this.ctx.getWebSockets(tag)) { try { ws.send(s); } catch (e) {} }
  }

  async webSocketMessage(ws, msg) {
    if (typeof msg !== "string" || msg.length > MAX_MSG) return;
    let o;
    try { o = JSON.parse(msg); } catch (e) { return; }
    const a = ws.deserializeAttachment() || {};
    if (a.role === "host") {
      if (o.k === "all" && o.m) {
        const s = JSON.stringify(o.m);
        this.latest = s;
        this.toPhones(s);
        const now = Date.now();
        if (now - this.lastSave > 30000) { this.lastSave = now; await this.ctx.storage.put("state", s); }
      } else if (o.k === "to" && o.id && o.m) {
        this.toPhones(JSON.stringify(o.m), "p:" + String(o.id).replace(/[^\w-]/g, ""));
      }
    } else {
      this.toHost({ k: "from", id: a.id, m: o });
    }
  }

  async webSocketClose(ws, code, reason) { this.gone(ws); }
  async webSocketError(ws) { this.gone(ws); }
  gone(ws) {
    const a = ws.deserializeAttachment() || {};
    // a reconnecting phone replaces its old connection: only report it gone if it has no connection left
    if (a.role === "phone") { if (!this.ctx.getWebSockets("p:" + a.id).some(x => x !== ws)) this.toHost({ k: "leave", id: a.id }); }
    else if (this.ctx.getWebSockets("host").filter(x => x !== ws).length === 0) this.toPhones(JSON.stringify({ t: "_host", on: false }));
    try { ws.close(1000, "bye"); } catch (e) {}
  }
}

// ---------------------------------------------------------------------------------------------
// Shared archive: every projector that knows the same secret key shares one archive of finished
// nights (room + people stats) and all the photos. Plain HTTP + JSON, stored in SQLite.
//
//   GET    /archive/<key>/nights?since=<ms>  → {ids:{id:end}, nights:[changed since], photos:[ids], deleted:[ids], counts:{night:n}, now, stats}
//   PUT    /archive/<key>/night              ← a night summary (JSON)
//   GET    /archive/<key>/photos?night=<id>  → [{id, night, ts, name, emoji, from, thumb}]
//   PUT    /archive/<key>/photo              ← {id, night, ts, name, emoji, from, thumb, data:"data:image/jpeg;base64,…"}
//   GET    /archive/<key>/photo/<id>         → the JPEG
//   DELETE /archive/<key>/night/<id>         → removes the night and its photos everywhere
// ---------------------------------------------------------------------------------------------
const CHUNK = 900_000;               // SQLite values are capped at 2 MB, so photos are stored in pieces
const MAX_PHOTO = 6_000_000;         // bytes per photo
const MAX_TOTAL = 4_000_000_000;     // per archive (the free plan has 5 GB in total)
const MAX_PHOTOS = 20_000;
const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json", ...CORS } });
const cleanId = v => String(v || "").replace(/[^\w-]/g, "").slice(0, 60);

export class Archive extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS nights (id TEXT PRIMARY KEY, ended INTEGER, updated INTEGER, data TEXT);
      CREATE TABLE IF NOT EXISTS photos (id TEXT PRIMARY KEY, night TEXT, ts INTEGER, meta TEXT, thumb TEXT, size INTEGER, parts INTEGER, updated INTEGER);
      CREATE INDEX IF NOT EXISTS photos_night ON photos(night);
      CREATE TABLE IF NOT EXISTS parts (photo TEXT, i INTEGER, data BLOB, PRIMARY KEY (photo, i));
      CREATE TABLE IF NOT EXISTS deleted (id TEXT PRIMARY KEY, at INTEGER);`);
  }
  isDeleted(id) { return this.sql.exec("SELECT 1 FROM deleted WHERE id = ?", id).toArray().length > 0; }
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
        const nights = this.sql.exec("SELECT data FROM nights WHERE updated > ?", since).toArray().map(r => JSON.parse(r.data));
        const photos = this.sql.exec("SELECT id FROM photos").toArray().map(r => r.id);
        const deleted = this.sql.exec("SELECT id FROM deleted").toArray().map(r => r.id);
        const counts = {};
        for (const r of this.sql.exec("SELECT night, COUNT(*) AS n FROM photos GROUP BY night")) counts[r.night] = r.n;
        return json({ ids, nights, photos, deleted, counts, now, stats: this.stats() });
      }
      if (req.method === "PUT" && path === "/night") {
        const n = await req.json();
        const id = cleanId(n && n.id);
        if (!id || typeof n !== "object") return json({ error: "bad night" }, 400);
        if (this.isDeleted(id)) return json({ ok: false, deleted: true });
        const data = JSON.stringify(n);
        if (data.length > 500_000) return json({ error: "too big" }, 413);
        const ended = Math.max(+n.edited || 0, +n.end || 0) || +n.start || 0;   // "edited": photos added later
        const old = this.sql.exec("SELECT ended FROM nights WHERE id = ?", id).toArray()[0];
        if (!old || ended >= old.ended) this.sql.exec("INSERT OR REPLACE INTO nights (id, ended, updated, data) VALUES (?, ?, ?, ?)", id, ended, now, data);
        return json({ ok: true });
      }
      if (req.method === "GET" && path === "/photos") {
        const night = cleanId(url.searchParams.get("night"));
        const rows = this.sql.exec("SELECT id, night, ts, meta, thumb FROM photos WHERE night = ? ORDER BY ts DESC", night).toArray();
        return json(rows.map(r => Object.assign({ id: r.id, night: r.night, ts: r.ts, thumb: r.thumb }, JSON.parse(r.meta || "{}"))));
      }
      if (req.method === "PUT" && path === "/photo") {
        const p = await req.json();
        const id = cleanId(p && p.id), night = cleanId(p && p.night);
        if (!id || !night || typeof p.data !== "string" || !p.data.startsWith("data:image/")) return json({ error: "bad photo" }, 400);
        if (this.isDeleted(id) || this.isDeleted(night)) return json({ ok: false, deleted: true });
        if (this.sql.exec("SELECT 1 FROM photos WHERE id = ?", id).toArray().length) return json({ ok: true, had: true });
        const bytes = Uint8Array.from(atob(p.data.slice(p.data.indexOf(",") + 1)), c => c.charCodeAt(0));
        if (bytes.length > MAX_PHOTO) return json({ error: "photo too big" }, 413);
        const st = this.stats();
        if (st.photos >= MAX_PHOTOS || st.bytes + bytes.length > MAX_TOTAL) return json({ error: "archive full" }, 507);
        const parts = Math.ceil(bytes.length / CHUNK);
        for (let i = 0; i < parts; i++) this.sql.exec("INSERT OR REPLACE INTO parts (photo, i, data) VALUES (?, ?, ?)", id, i, bytes.slice(i * CHUNK, (i + 1) * CHUNK).buffer);
        const meta = JSON.stringify({ name: String(p.name || "").slice(0, 80), emoji: String(p.emoji || "").slice(0, 8), from: cleanId(p.from) });
        const thumb = typeof p.thumb === "string" && p.thumb.startsWith("data:image/") && p.thumb.length < 400_000 ? p.thumb : "";
        this.sql.exec("INSERT INTO photos (id, night, ts, meta, thumb, size, parts, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", id, night, +p.ts || now, meta, thumb, bytes.length, parts, now);
        return json({ ok: true });
      }
      let m = path.match(/^\/photo\/([\w-]+)$/);
      if (req.method === "GET" && m) {
        const ph = this.sql.exec("SELECT parts FROM photos WHERE id = ?", m[1]).toArray()[0];
        if (!ph) return json({ error: "not found" }, 404);
        const chunks = this.sql.exec("SELECT data FROM parts WHERE photo = ? ORDER BY i", m[1]).toArray().map(r => new Uint8Array(r.data));
        return new Response(new Blob(chunks, { type: "image/jpeg" }), { headers: { "content-type": "image/jpeg", "cache-control": "private, max-age=31536000, immutable", ...CORS } });
      }
      m = path.match(/^\/night\/([\w-]+)$/);
      if (req.method === "DELETE" && m) {
        const id = m[1];
        const ph = this.sql.exec("SELECT id FROM photos WHERE night = ?", id).toArray().map(r => r.id);
        for (const pid of ph) {
          this.sql.exec("DELETE FROM parts WHERE photo = ?", pid);
          this.sql.exec("INSERT OR REPLACE INTO deleted (id, at) VALUES (?, ?)", pid, now);
        }
        this.sql.exec("DELETE FROM photos WHERE night = ?", id);
        this.sql.exec("DELETE FROM nights WHERE id = ?", id);
        this.sql.exec("INSERT OR REPLACE INTO deleted (id, at) VALUES (?, ?)", id, now);
        return json({ ok: true });
      }
      // read-only link for phones: a token that can't be turned back into the key
      if (req.method === "GET" && path === "/viewtoken") {
        const key = url.pathname.split("/")[2].toLowerCase();
        const h = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode("focus-jams-view:" + key)));
        const token = [...h.slice(0, 16)].map(b => b.toString(16).padStart(2, "0")).join("");
        const map = this.env.ARCHIVES.get(this.env.ARCHIVES.idFromName("view:" + token));
        await map.fetch(new Request(url.origin + "/__settarget?key=" + encodeURIComponent(key), { method: "PUT" }));
        return json({ token });
      }
      // (only reachable from inside the worker: outside requests to /archive/... always have a key in front)
      if (path === "/__target") return json({ key: (await this.ctx.storage.get("target")) || null });
      if (path === "/__settarget" && req.method === "PUT") { await this.ctx.storage.put("target", url.searchParams.get("key")); return json({ ok: true }); }
      if (req.method === "GET" && path === "/") return json({ ok: true, stats: this.stats() });
      return json({ error: "not found" }, 404);
    } catch (e) {
      return json({ error: String(e && e.message || e) }, 500);
    }
  }
}
