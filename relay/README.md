# Focus Jams sync server

A tiny server that lets the projector and everyone's phones sync with each other, with no usage limits to
worry about. It runs on Cloudflare Workers' free plan (one "room" per projector, using WebSockets).

## One-time setup (about 10 minutes)

1. Make a free account at <https://dash.cloudflare.com/sign-up>.
2. In the dashboard, go to **Workers & Pages** → **Create** → **Import a repository**.
3. Connect your GitHub account and pick the **focus-jams** repository.
4. On the setup screen:
   - **Project name:** `focus-jams-relay` (it has to match the name in `wrangler.toml`)
   - **Root directory / path:** `relay`
   - **Build command:** leave empty
   - **Deploy command:** `npx wrangler deploy` (the default)
5. Click **Deploy**. When it finishes, it shows your server's address, something like
   `focus-jams-relay.yourname.workers.dev`.
6. Open Focus Jams on the projector → **Customize** → **👥 Group** → **Your sync server**, paste the address,
   and click **Test**. You should see "✅ Your sync server is working."
7. Everyone scans the QR code again (it now includes the server's address).

Cloudflare redeploys the server automatically whenever this folder changes on GitHub.

## Free plan

Each night uses a few thousand of the free plan's ~100,000 daily requests (keep-alive pings are answered
without counting, and the room sleeps between messages), so a study group won't come close to the limit.

## Files

- `worker.js`: the server
- `wrangler.toml`: Cloudflare settings (name, the Durable Object "room", free-plan storage type)
