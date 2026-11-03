# Focus Jams sync server

A tiny server that lets the projector and everyone's phones sync with each other, with no usage limits to
worry about. It runs on Cloudflare Workers' free plan (one "room" per projector, using WebSockets).

## One-time setup (about 10 minutes)

1. Make a free account at <https://dash.cloudflare.com/sign-up>.
2. In the dashboard, go to **Workers & Pages** → **Create** → **Import a repository**.
3. Connect your GitHub account and pick the **focus-jams** repository.
4. On the setup screen, keep the defaults:
   - **Project name:** `focus-jams` (it has to match the name in the `wrangler.toml` at the top of the repository)
   - **Build command:** leave empty
5. Click **Deploy**. When it finishes, it shows your server's address, something like
   `focus-jams.yourname.workers.dev`.
6. Open Focus Jams on the projector → **Customize** → **👥 Group** → **Your sync server**, paste the address,
   and click **Test**. You should see "✅ Your sync server is working."
7. Everyone scans the QR code again (it now includes the server's address).

Cloudflare redeploys the server automatically whenever this folder changes on GitHub.

## Shared archive

The same server also stores the **shared archive**: every finished night (room and people stats) and every
photo, shared by all your Focus Jams screens. On one screen, open **Customize → 👥 Group → Shared archive →
Create a shared archive**, copy the key, and paste it on your other screens. Anyone with the key can see the
archive, so keep it to your group. Photos use the free plan's 5 GB of storage (roughly 10,000+ photos).

## Free plan

Each night uses a few thousand of the free plan's ~100,000 daily requests (keep-alive pings are answered
without counting, and the room sleeps between messages), so a study group won't come close to the limit.

## Files

- `worker.js`: the server (live rooms for phone sync, and the shared archive)
- `../wrangler.toml` (top of the repository): Cloudflare settings (name, the Durable Object "room", free-plan storage type)
