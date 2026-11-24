# AH Quantity Tracker (WoW: Forever)

Small self-hosted web app that watches a list of auction-house items and shows
current listed quantity + min buyout + median price. All data from
[AHledger](https://ahledger.com) (attribution shown in the UI, required by their
API terms).

## How it works
- A background poller fetches the **full market pricetable**
  (`api.ahledger.com/v1/pricetable/{market}`) every 10 minutes — one request
  covers every item in the market. ETags make unchanged polls free (304).
- The fetcher retries 3× with backoff because some home firewalls
  intermittently drop fresh outbound TLS connections.
- Item name → ID resolution uses AHledger's site search
  (`ahledger.com/api/search?q=...`), only when you add an item. If the name
  matches several items, the UI shows candidates to pick from.
- Snapshots are stored in SQLite (`~/.ah-tracker/tracker.db`, 30-day retention)
  so the page has data immediately and history is available for future charts.

## Run / install (systemd)
`ah-tracker.service` is a template — edit `User=` and the two
`EnvironmentFile=` paths (point them at your secrets env files) before
installing:
```sh
sudo cp ah-tracker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now ah-tracker
journalctl -u ah-tracker -f        # logs
```
Quick manual test (no systemd):
```sh
export AHL_API_KEY=...   # optional; raises AHledger rate limits
python3 app.py
```

## Run with Docker (optional)
The repo ships a `Dockerfile` (non-root, `python:3.14-slim`, stdlib-only app)
and a GitHub Action (`.github/workflows/build-image.yml`) that builds and
pushes the image to GHCR on every push to `main` and on `v*` tags:
`ghcr.io/lucasdina/ah-tracker` (`:latest` on the default branch, plus
`v<version>` and `sha-<commit>` tags).

`docker-compose.yml` example (Compose v2.24+ for the `required:` syntax):
```yaml
services:
  ah-tracker:
    image: ghcr.io/lucasdina/ah-tracker:latest   # or drop `image:` and add `build: .`
    restart: unless-stopped
    ports:
      - "13321:13321"
    environment:
      AHT_MARKET: forever.normal.alliance.us
      AHT_PORT: "13321"
      AHT_POLL_INTERVAL: "600"
    env_file:
      - path: ~/.ah-tracker/secrets.env   # e.g. AHL_API_KEY=...
        required: true
      - path: ~/.ah-tracker/smtp.env      # AHT_SMTP_* — alerts stay off until it exists
        required: false
    volumes:
      - ~/.ah-tracker:/data  # SQLite state; container runs as uid 1000
```
Note: the container goes out over the same host network, so any
intermittent outbound-TLS drops from your firewall affect Docker runs exactly
like the systemd one — the poller's retry/backoff handles it.

## Configuration (env vars, or edit the unit file)
| Var | Default | Meaning |
|---|---|---|
| `AHT_PORT` | `13321` | HTTP port |
| `AHT_MARKET` | `forever.normal.alliance.us` | Market: `forever.{normal\|pvp\|rp}.{alliance\|horde}.{us\|eu}` |
| `AHT_POLL_INTERVAL` | `600` | Poll interval seconds |
| `AHL_API_KEY` | *(secrets file)* | Optional; raises AHledger rate limits |
| `AHT_SMTP_HOST` / `AHT_SMTP_TO` | *(smtp file)* | **Required for alert emails**; without them alerts are only logged in the UI |
| `AHT_SMTP_PORT` | `587` | SMTP port |
| `AHT_SMTP_USER` / `AHT_SMTP_PASS` | empty | SMTP login (STARTTLS); leave empty for anonymous |
| `AHT_SMTP_FROM` | `ah-tracker@localhost` | From address |
| `AHT_TRACKER_URL` | empty | If set, alert emails end with a `Tracker: <url>` link |

Both env files are loaded by the unit; the SMTP file is optional
(`EnvironmentFile=-...`), so the service starts fine before it exists.

## Alerting
Per item you can set thresholds in the UI (⚙ button on the row):
- **qty ≤ N** — alert when listed quantity drops to/below N (deal appears)
- **qty ≥ N** — alert when quantity climbs to/above N (oversupply)
- **min buyout ≤ X copper** — alert when the lowest buyout drops (blank = off)

Alerts fire on *transitions* (crossing into the breach), are logged in the
"Recent alerts" section, and are emailed as a single batched message when SMTP
is configured. A still-broken rule re-fires at most once every 6 hours
(`REALERT_COOLDOWN`). Fired-but-unsent alerts are retried on each poll until
SMTP is configured, then resent once.

Change faction: edit `AHT_MARKET` in `/etc/systemd/system/ah-tracker.service`,
then `sudo systemctl restart ah-tracker`. (Existing snapshots for the old
market stay in the DB; the UI reads the active market only.)

## API
- `GET /api/items` — watchlist with latest quantity/prices
- `POST /api/items` `{"name": "linen cloth"}` — search + add (or `candidates`
  back if ambiguous); `{"id": 2589, "name": ...}` adds a known item
- `DELETE /api/items/2589` — remove
- `GET /api/status` — market, last poll, errors, stale flag
- `POST /api/poll` — force an immediate poll

## Files
- `app.py` — the whole app (Python stdlib only, no dependencies)
- `Dockerfile` — non-root image for Docker/GHCR builds
- `.github/workflows/build-image.yml` — CI: build + push image to GHCR
- `ah-tracker.service` — systemd unit (template: edit `User=` and the env-file paths for your setup)
- `~/.ah-tracker/tracker.db` — SQLite state

## Security note
This is a single-user app with **no authentication** — anyone who can reach
the port can view data and modify the watchlist. Keep it on a trusted network
(LAN/tailnet) or put it behind an authenticating reverse proxy. It only talks
out to `api.ahledger.com` and (optionally) your SMTP host.

## License
MIT — see [LICENSE](LICENSE). Market data © AHledger; attribution is shown in
the UI per their API terms.
