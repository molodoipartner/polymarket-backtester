# Deployment

## Fast temporary public URL from this computer

This app serves the frontend and backend from the same FastAPI process, so the
fastest public demo is a Cloudflare Quick Tunnel.

Start the public tunnel:

```powershell
.\share.bat
```

The script installs `cloudflared` if needed, starts the app, prints a temporary
`https://*.trycloudflare.com` URL, and shows the login/password to use. Keep the
PowerShell window open while using the public site. If `.env` still has default
test passwords, the script replaces them with random safe passwords first.

Cloudflare Quick Tunnels are for testing only. They generate a random subdomain
and proxy it to your localhost app.

## Render paid web service with persistent data

`render.yaml` is included for Render Blueprint deploys. Connect the GitHub
repository to Render and create the service from the Blueprint.

The application stores users, sessions, cache, groups, strategies and
transactions as JSON/JSONL files. In production those files must live on a
Render Persistent Disk, not in the Git repository and not on your computer.

The Blueprint attaches a 20 GB disk at:

```text
/var/data
```

and sets:

```text
DATA_ROOT=/var/data/polymarket-backtester
```

That means the server writes all generated files into the mounted Render disk.
The computer that deployed the app can be turned off; the running Render server
continues to read and write data on that cloud disk.

If your current local `server_data/` is larger than 20 GB, increase
`disk.sizeGB` in `render.yaml` before creating the Render service. Render lets
you increase a disk later, but not shrink it.

Keep `numInstances: 1`. Render disks are attached to a single running service
instance, and this app's file locks are designed for one server instance.

During Blueprint setup, set the prompted secret variables:

- `USERNAME`
- `PASSWORD`
- `ADMIN_USERNAME`
- `ADMIN_PASSWORD`
- `PRIVATE_KEY`
- `PROXY_ADDRESS`
- `API_KEY`
- `API_SECRET`
- `API_PASSPHRASE`

For live Polymarket trading, keep these Render variables enabled:

- `POLYMARKET_LIVE_TRADING_ENABLED=true`
- `POLYMARKET_LIVE_ORDER_AMOUNT=1`
- `SIGNATURE_TYPE=1`

`SESSION_SECRET` and `EXPORT_TOKEN` are generated automatically by Render.

To download a server data backup from your computer, open the Render service
environment variables, copy `EXPORT_TOKEN`, and run:

```powershell
curl.exe -L `
  -H "Authorization: Bearer YOUR_EXPORT_TOKEN" `
  "https://YOUR-SERVICE.onrender.com/api/admin/export-data" `
  -o polymarket-server-data.tar.gz
```

You can also download data using Render SSH/SCP after enabling SSH for the
service.

For a Render custom domain, also update `ALLOWED_HOSTS` to include that domain.

## Fly.io app in Stockholm, Sweden

Fly deployment is configured with `Dockerfile`, `.dockerignore`, and `fly.toml`.
The Fly primary region is:

```text
arn
```

`arn` is Fly's Stockholm, Sweden region. The app uses one small shared CPU
machine and a mounted persistent volume at:

```text
/var/data
```

The server writes production data to:

```text
DATA_ROOT=/var/data/polymarket-backtester
```

The default config uses `shared-cpu-1x` with 512 MB RAM. In Stockholm this is
about $3.32/month when running continuously. Fly Volumes are $0.15/GB/month, so
the recommended 3 GB volume is about $0.45/month. Outbound internet traffic from
Europe is $0.02/GB.

Create the Fly app and a 3 GB volume:

```powershell
flyctl apps create polymarket-backtester --org personal
flyctl volumes create polymarket_data --region arn --size 3 --app polymarket-backtester
```

Set the required secrets before deploy. At minimum, set:

```powershell
flyctl secrets set `
  SESSION_SECRET="replace-with-at-least-32-random-characters" `
  EXPORT_TOKEN="replace-with-random-export-token" `
  USERNAME="your-login" `
  PASSWORD="your-password" `
  ADMIN_USERNAME="your-admin-login" `
  ADMIN_PASSWORD="your-admin-password" `
  --app polymarket-backtester
```

For live Polymarket trading, also set the wallet/API secrets used by your
account:

```powershell
flyctl secrets set `
  PRIVATE_KEY="..." `
  PROXY_ADDRESS="..." `
  API_KEY="..." `
  API_SECRET="..." `
  API_PASSPHRASE="..." `
  POLYMARKET_PRIVATE_KEY="..." `
  POLYMARKET_PROXY_ADDRESS="..." `
  POLYMARKET_FUNDER="..." `
  POLYMARKET_API_KEY="..." `
  POLYMARKET_API_SECRET="..." `
  POLYMARKET_API_PASSPHRASE="..." `
  --app polymarket-backtester
```

Deploy:

```powershell
flyctl deploy --app polymarket-backtester
```

The production URL will be:

```text
https://polymarket-backtester.fly.dev
```

If Fly reports that the app name is unavailable, create a unique app name and
update `app` plus the first value in `ALLOWED_HOSTS` in `fly.toml`.
