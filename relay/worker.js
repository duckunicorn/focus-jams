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
const CORS = { "access-control-allow-origin": "*" };

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("Focus Jams sync server is running.", { headers: { "content-type": "text/plain", ...CORS } });
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
