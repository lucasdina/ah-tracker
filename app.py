#!/usr/bin/env python3
"""
AH Quantity Tracker — WoW: Forever auction house quantity watcher.

Data sources (AHledger, attribution required):
  - api.ahledger.com/v1/pricetable/{market}  full market snapshot (ETag/304)
  - ahledger.com/api/search?q=...            item name -> blizzardId (site search)
  - api.ahledger.com/v1/items/{id}           item detail (game=forever)

Stdlib only. Runs under systemd (ah-tracker.service).
"""

import json
import os
import re
import signal
import sqlite3
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# ---------------------------------------------------------------- config

HOST = os.environ.get("AHT_HOST", "0.0.0.0")
PORT = int(os.environ.get("AHT_PORT", "13321"))
# Your market: ruleset is normal|pvp|rp, faction alliance|horde, region us|eu
MARKET = os.environ.get("AHT_MARKET", "forever.normal.alliance.us")
POLL_INTERVAL = int(os.environ.get("AHT_POLL_INTERVAL", "600"))  # 10 min

AH_API = "https://api.ahledger.com"
AH_SITE = "https://ahledger.com"
KEY = os.environ.get("AHL_API_KEY", "")  # optional, raises rate limits

BASE_DIR = os.path.expanduser(os.environ.get("AHT_HOME", "~/.ah-tracker"))
DB_PATH = os.path.join(BASE_DIR, "tracker.db")
USER_AGENT = "ah-tracker/1.0"

# ---------------------------------------------------------------- db

def db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    os.makedirs(BASE_DIR, exist_ok=True)
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS watchlist (
              id INTEGER PRIMARY KEY,        -- blizzard item id
              name TEXT NOT NULL,
              icon TEXT,
              quality TEXT,
              added_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS snapshots (
              market TEXT NOT NULL,
              item_id INTEGER NOT NULL,
              ts TEXT NOT NULL,              -- market data time (UTC)
              fetched TEXT NOT NULL,         -- when we pulled it (UTC)
              quantity INTEGER,
              min_buyout INTEGER,
              median INTEGER,
              auctions INTEGER,
              PRIMARY KEY (market, item_id, ts)
            );
            CREATE TABLE IF NOT EXISTS meta (
              key TEXT PRIMARY KEY,
              value TEXT
            );
            CREATE TABLE IF NOT EXISTS thresholds (
              item_id INTEGER PRIMARY KEY,
              qty_below INTEGER,      -- alert when quantity <= this
              qty_above INTEGER,      -- alert when quantity >= this
              price_below INTEGER     -- alert when min buyout (copper) <= this
            );
            CREATE TABLE IF NOT EXISTS alerts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts TEXT NOT NULL,
              item_id INTEGER,
              name TEXT,
              kind TEXT,
              message TEXT,
              emailed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS alert_state (
              item_id INTEGER NOT NULL,
              kind TEXT NOT NULL,
              active INTEGER NOT NULL DEFAULT 0,
              last_at TEXT,
              PRIMARY KEY (item_id, kind)
            );
            """
        )

def meta_get(key, default=None):
    with db() as conn:
        row = conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default

def meta_set(key, value):
    with db() as conn:
        conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )

# ---------------------------------------------------------------- http (resilient)

def http_get(url, headers=None, timeout=20, attempts=3):
    """GET with retry/backoff — some home firewalls intermittently drop
    fresh outbound TLS connections, so one drop is expected, not an error."""
    headers = dict(headers or {})
    headers.setdefault("User-Agent", USER_AGENT)
    headers.setdefault("Accept", "application/json")
    if KEY:
        headers.setdefault("Authorization", f"Bearer {KEY}")
    last = None
    for i in range(attempts):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, dict(r.headers), r.read()
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return 304, dict(e.headers), b""
            if e.code == 429:
                last = e
                wait = int(e.headers.get("Retry-After") or 10)
                time.sleep(min(wait, 30))
                continue
            last = e
            if 400 <= e.code < 500:
                break  # client error, retrying won't help
        except Exception as e:  # URLError, timeout, SSL EOF, ...
            last = e
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"GET {url} failed after {attempts} attempts: {last}")

# ---------------------------------------------------------------- polling

def parse_pricetable(raw):
    lines = raw.decode("utf-8", errors="replace").splitlines()
    if not lines or not lines[0].startswith("AHL1|"):
        raise ValueError("unexpected pricetable format")
    header = lines[0].split("|")
    data_ts = datetime.fromtimestamp(int(header[2]), tz=timezone.utc)
    items = {}
    for line in lines[1:]:
        parts = line.split(":")
        if len(parts) < 9:
            continue
        def int_or_none(s):
            return int(s) if s != "" else None
        items[int(parts[0])] = {
            "quantity": int_or_none(parts[3]),
            "min_buyout": int_or_none(parts[2]),
            "median": int_or_none(parts[1]),
        }
    return data_ts, items

poll_wake = threading.Event()
poll_running = threading.Event()

def poll_once():
    """Fetch the market pricetable, store snapshots for watchlist items."""
    poll_running.set()
    try:
        return _poll_once_inner()
    finally:
        poll_running.clear()

def _poll_once_inner():
    # Items added since the last full fetch force a real fetch (no 304),
    # so they get their first snapshot.
    missing = [r["id"] for r in db().execute(
        "SELECT w.id FROM watchlist w WHERE NOT EXISTS"
        " (SELECT 1 FROM snapshots s WHERE s.market=? AND s.item_id=w.id)",
        (MARKET,)).fetchall()]
    etag = meta_get("etag")
    headers = {"Accept": "text/plain"}
    if etag and not missing:
        headers["If-None-Match"] = etag
    try:
        status, rheaders, raw = http_get(
            f"{AH_API}/v1/pricetable/{MARKET}", headers=headers
        )
    except Exception as e:
        meta_set("last_error", f"{e}")
        meta_set("last_attempt", utcnow())
        log(f"poll failed: {e}")
        return False

    meta_set("last_attempt", utcnow())
    meta_set("last_error", "")

    def run_alert_check():
        """Evaluate thresholds against the latest snapshots.

        Runs on BOTH 200 and 304 polls: a 304 means the market hasn't
        changed, but the last stored snapshot is still the truth to check.
        """
        try:
            fired = evaluate_alerts()
            if fired:
                send_alert_email(fired)
        except Exception as e:
            log(f"alert evaluation failed: {e}")

    if status == 304:
        meta_set("last_poll", utcnow())
        meta_set("poll_note", "unchanged")
        run_alert_check()
        return True

    data_ts, items = parse_pricetable(raw)
    new_etag = rheaders.get("ETag") or rheaders.get("etag")
    if new_etag:
        meta_set("etag", new_etag)

    ids = [r["id"] for r in db().execute("SELECT id FROM watchlist").fetchall()]
    fetched = utcnow()
    with db() as conn:
        for iid in ids:
            row = items.get(iid)
            if row is None:
                # not listed in this market right now -> zero stock
                row = {"quantity": 0, "min_buyout": None, "median": None}
            conn.execute(
                "INSERT OR REPLACE INTO snapshots"
                "(market,item_id,ts,fetched,quantity,min_buyout,median,auctions)"
                " VALUES(?,?,?,?,?,?,?,?)",
                (MARKET, iid, data_ts.isoformat(), fetched,
                 row["quantity"], row["min_buyout"], row["median"], None),
            )
        # trim history older than 30 days (cheap, keep the table small)
        conn.execute(
            "DELETE FROM snapshots WHERE fetched < datetime('now','-30 day')"
        )
    meta_set("last_poll", fetched)
    meta_set("poll_note", f"{len(items)} items in market")
    log(f"poll ok: {len(items)} items, data as of {data_ts.isoformat()}")
    run_alert_check()
    return True

def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def log(msg):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}", flush=True)

def poller_loop():
    while True:
        t0 = time.time()
        try:
            poll_once()
        except Exception as e:
            log(f"poller error: {e}")
        poll_wake.wait(POLL_INTERVAL)
        poll_wake.clear()
        elapsed = time.time() - t0
        if 0 < POLL_INTERVAL - elapsed < POLL_INTERVAL:
            time.sleep(POLL_INTERVAL - elapsed)

# ---------------------------------------------------------------- alerts + smtp

REALERT_COOLDOWN = 6 * 3600  # seconds before the same rule may re-fire

def smtp_configured():
    return bool(os.environ.get("AHT_SMTP_HOST")
                and os.environ.get("AHT_SMTP_TO"))

def evaluate_alerts():
    """Check watchlist thresholds against the latest snapshot.
    Fires on transition (condition crossing into breach); a rule already
    active can re-fire at most once per REALERT_COOLDOWN."""
    now = datetime.now(timezone.utc)
    fired = []
    with db() as conn:
        for it in conn.execute("SELECT id, name FROM watchlist").fetchall():
            th = conn.execute(
                "SELECT * FROM thresholds WHERE item_id=?", (it["id"],)
            ).fetchone()
            if not th:
                continue
            snap = conn.execute(
                "SELECT * FROM (SELECT * FROM snapshots "
                "WHERE market=? AND item_id=? ORDER BY ts DESC LIMIT 1)",
                (MARKET, it["id"]),
            ).fetchone()
            if not snap:
                continue
            qty = snap["quantity"] or 0
            minbuy = snap["min_buyout"]
            rules = [
                ("qty_below",
                 th["qty_below"] is not None and qty <= th["qty_below"],
                 f"{it['name']}: quantity {qty} (threshold <= {th['qty_below']}), "
                 f"min buyout {fmt_gold(minbuy)}"),
                ("qty_above",
                 th["qty_above"] is not None and qty >= th["qty_above"],
                 f"{it['name']}: quantity {qty} (threshold >= {th['qty_above']}), "
                 f"min buyout {fmt_gold(minbuy)}"),
                ("price_below",
                 th["price_below"] is not None and minbuy is not None
                 and minbuy <= th["price_below"],
                 f"{it['name']}: min buyout {fmt_gold(minbuy)} "
                 f"(threshold <= {fmt_gold(th['price_below'])}), "
                 f"quantity {qty}"),
            ]
            for kind, active, msg in rules:
                prev = conn.execute(
                    "SELECT active, last_at FROM alert_state "
                    "WHERE item_id=? AND kind=?", (it["id"], kind)
                ).fetchone()
                was_active = bool(prev["active"]) if prev else False
                fired_now = False
                if active and not was_active:
                    last_at = prev["last_at"] if prev else None
                    cooled = True
                    if last_at:
                        cooled = (now - datetime.fromisoformat(last_at)
                                  ).total_seconds() > REALERT_COOLDOWN
                    if cooled:
                        fired.append({"item_id": it["id"], "name": it["name"],
                                      "kind": kind, "message": msg})
                        conn.execute(
                            "INSERT INTO alerts(ts,item_id,name,kind,message)"
                            " VALUES(?,?,?,?,?)",
                            (now.isoformat(timespec="seconds"), it["id"],
                             it["name"], kind, msg))
                        fired_now = True
                conn.execute(
                    "INSERT INTO alert_state(item_id,kind,active,last_at) "
                    "VALUES(?,?,?,?) "
                    "ON CONFLICT(item_id,kind) DO UPDATE SET "
                    "active=excluded.active, "
                    "last_at=CASE WHEN excluded.last_at IS NOT NULL "
                    "THEN excluded.last_at ELSE alert_state.last_at END",
                    (it["id"], kind, int(active),
                     now.isoformat(timespec="seconds") if fired_now else None))
    for a in fired:
        log(f"ALERT: {a['message']}")
    return fired

def send_alert_email(alerts):
    """Send fired alerts as one email. Returns True on success."""
    host = os.environ.get("AHT_SMTP_HOST")
    to = os.environ.get("AHT_SMTP_TO")
    if not host or not to:
        log("alerts fired but SMTP not configured (AHT_SMTP_HOST/AHT_SMTP_TO)")
        return False
    import smtplib
    from email.message import EmailMessage
    port = int(os.environ.get("AHT_SMTP_PORT") or 587)
    user = os.environ.get("AHT_SMTP_USER") or ""
    pw = os.environ.get("AHT_SMTP_PASS") or ""
    sender = os.environ.get("AHT_SMTP_FROM") or user or "ah-tracker@localhost"
    lines = [f"Market: {MARKET}",
             f"Data as of: {meta_get('last_poll', '?')}", ""]
    for a in alerts:
        lines.append(f"* {a['message']}")
    base = os.environ.get("AHT_TRACKER_URL")
    if base:
        lines += ["", f"Tracker: {base}"]
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to
    msg["Subject"] = (f"AH alert ({len(alerts)}): "
                      f"{alerts[0]['message'].split(':')[0]}")
    msg.set_content("\n".join(lines))
    last = None
    for attempt in range(2):
        try:
            if port == 465:
                s = smtplib.SMTP_SSL(host, port, timeout=20)
            else:
                s = smtplib.SMTP(host, port, timeout=20)
                s.ehlo()
                try:
                    s.starttls()
                    s.ehlo()
                except smtplib.SMTPNotSupportedError:
                    pass
            with s:
                if user:
                    s.login(user, pw)
                s.send_message(msg)
            with db() as conn:
                conn.execute("UPDATE alerts SET emailed=1 WHERE emailed=0")
            log(f"alert email sent to {to}")
            return True
        except Exception as e:
            last = e
            time.sleep(3)
    log(f"alert email FAILED (alerts kept, retry next fire): {last}")
    return False

# ---------------------------------------------------------------- handlers

CANDIDATE_FIELDS = ("blizzardId", "name", "icon", "quality")

def search_items(q):
    q = q.strip()
    if not q:
        return []
    url = f"{AH_SITE}/api/search?q={urllib.parse.quote(q)}"
    status, _, raw = http_get(url, attempts=2)
    if status != 200:
        return []
    data = json.loads(raw)
    out = []
    for it in data.get("items", [])[:5]:
        out.append({k: it.get(k) for k in CANDIDATE_FIELDS
                    if it.get(k) is not None})
    return out

def fmt_gold(copper):
    if copper is None:
        return None
    g, rem = divmod(copper, 10000)
    s, c = divmod(rem, 100)
    if g:
        return f"{g}g {s:02d}s {c:02d}c"
    if s:
        return f"{s}s {c:02d}c"
    return f"{c}c"

def latest_rows():
    with db() as conn:
        return conn.execute(
            """
            SELECT w.id, w.name, w.icon, w.quality,
                   s.quantity, s.min_buyout, s.median, s.fetched,
                   s.ts AS data_ts
            FROM watchlist w
            LEFT JOIN snapshots s
              ON s.market = ?
             AND s.item_id = w.id
             AND s.ts = (SELECT MAX(ts2.ts) FROM snapshots ts2
                         WHERE ts2.market = s.market AND ts2.item_id = w.id)
            ORDER BY w.name
            """,
            (MARKET,),
        ).fetchall()

def api_items():
    out = []
    for r in latest_rows():
        with db() as conn:
            th = conn.execute(
                "SELECT * FROM thresholds WHERE item_id=?", (r["id"],)
            ).fetchone()
        out.append({
            "id": r["id"], "name": r["name"], "icon": r["icon"],
            "quality": r["quality"],
            "quantity": r["quantity"],
            "minBuyout": fmt_gold(r["min_buyout"]),
            "median": fmt_gold(r["median"]),
            "rawMinBuyout": r["min_buyout"], "rawMedian": r["median"],
            "updated": r["fetched"], "dataTs": r["data_ts"],
            "thresholds": {
                "qtyBelow": th["qty_below"] if th else None,
                "qtyAbove": th["qty_above"] if th else None,
                "priceBelow": th["price_below"] if th else None,
            },
        })
    return out

def api_alerts(limit=20):
    with db() as conn:
        rows = conn.execute(
            "SELECT ts, item_id, name, kind, message, emailed "
            "FROM alerts ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in rows]

def api_status():
    last_poll = meta_get("last_poll")
    last_attempt = meta_get("last_attempt")
    error = meta_get("last_error")
    stale = None
    if last_poll:
        age_h = (datetime.now(timezone.utc)
                 - datetime.fromisoformat(last_poll)).total_seconds() / 3600
        stale = age_h > 3
    return {
        "market": MARKET,
        "lastPoll": last_poll,
        "lastAttempt": last_attempt,
        "error": error or None,
        "note": meta_get("poll_note"),
        "stale": stale,
        "pollInterval": POLL_INTERVAL,
        "polling": poll_running.is_set(),
        "smtpConfigured": smtp_configured(),
    }

def add_item_by_id(iid, name, icon, quality):
    with db() as conn:
        if conn.execute("SELECT 1 FROM watchlist WHERE id=?", (iid,)).fetchone():
            return {"error": f"{name} is already on the watchlist", "status": 409}
        conn.execute(
            "INSERT INTO watchlist(id,name,icon,quality,added_at)"
            " VALUES(?,?,?,?,?)",
            (iid, name, icon, quality, utcnow()),
        )
    log(f"watchlist + {iid} {name}")
    poll_wake.set()  # fetch fresh data for the new item promptly
    return {"status": 200, "added": {"id": iid, "name": name}}

class Handler(BaseHTTPRequestHandler):
    server_version = "AHTracker/1.0"

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}") if n else {}

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            body = HTML.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/items":
            self._json({"items": api_items()})
        elif path == "/api/status":
            self._json(api_status())
        elif path == "/api/alerts":
            self._json({"alerts": api_alerts()})
        elif path == "/health":
            self._json({"ok": True})
        else:
            self._json({"error": "not found"}, 404)

    def do_POST(self):
        path = self.path.split("?")[0]
        try:
            payload = self._body()
        except Exception:
            return self._json({"error": "invalid json"}, 400)
        if path == "/api/items":
            if "id" in payload:
                res = add_item_by_id(
                    int(payload["id"]), payload.get("name", f"item {payload['id']}"),
                    payload.get("icon"), payload.get("quality"))
                return self._json(res, res.get("status", 200))
            name = (payload.get("name") or "").strip()
            if not name:
                return self._json({"error": "name required"}, 400)
            try:
                cands = search_items(name)
            except Exception as e:
                return self._json({"error": f"search failed: {e}"}, 502)
            if not cands:
                return self._json({"error": f"no items found for {name!r}"}, 404)
            if len(cands) == 1:
                c = cands[0]
                res = add_item_by_id(c["blizzardId"], c["name"],
                                     c.get("icon"), c.get("quality"))
                return self._json(res, res.get("status", 200))
            return self._json({"candidates": cands})
        elif path.startswith("/api/items/") and path.endswith("/thresholds"):
            iid = int(path[len("/api/items/"):-len("/thresholds")])
            def optint(k):
                v = payload.get(k)
                if v in (None, "", 0):
                    return None
                return int(v)
            th = {"qty_below": optint("qtyBelow"),
                  "qty_above": optint("qtyAbove"),
                  "priceBelow": optint("priceBelow")}
            with db() as conn:
                exists = conn.execute(
                    "SELECT 1 FROM watchlist WHERE id=?", (iid,)
                ).fetchone()
                if not exists:
                    return self._json({"error": "no such item"}, 404)
                conn.execute(
                    "INSERT INTO thresholds(item_id,qty_below,qty_above,price_below) "
                    "VALUES(?,?,?,?) ON CONFLICT(item_id) DO UPDATE SET "
                    "qty_below=excluded.qty_below, qty_above=excluded.qty_above, "
                    "price_below=excluded.price_below",
                    (iid, th["qty_below"], th["qty_above"], th["priceBelow"]))
            log(f"thresholds {iid}: {th}")
            self._json({"ok": True, "thresholds": th})
        elif path == "/api/poll":
            if poll_running.is_set():
                self._json({"status": 200, "poll": "already running"})
                return
            threading.Thread(target=poll_once, daemon=True).start()
            self._json({"status": 200, "poll": "started"})
        else:
            self._json({"error": "not found"}, 404)

    def do_DELETE(self):
        m = re.fullmatch(r"/api/items/(\d+)", self.path.split("?")[0])
        if not m:
            return self._json({"error": "not found"}, 404)
        iid = int(m.group(1))
        with db() as conn:
            cur = conn.execute("DELETE FROM watchlist WHERE id=?", (iid,))
        if cur.rowcount:
            log(f"watchlist - {iid}")
            self._json({"status": 200, "removed": iid})
        else:
            self._json({"error": "not on watchlist"}, 404)

    def log_message(self, *a):
        pass  # keep journald quiet; app logs itself

# ---------------------------------------------------------------- ui

HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AH Quantity Tracker</title>
<style>
  :root {
    --bg: #0f0d0a; --panel: #1a1712; --panel2: #221e17;
    --border: #3a3226; --text: #e8e2d4; --dim: #9a8f7a;
    --gold: #c9a227; --red: #c94f4f; --green: #7fb069; --yellow: #d9b44a;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font: 15px/1.45 system-ui, -apple-system, sans-serif;
  }
  #bar {
    position: fixed; top: 0; left: 0; height: 3px; width: 0;
    background: var(--gold); z-index: 99; opacity: 0;
  }
  #bar.show { opacity: 1; animation: bar 1.2s ease-in-out infinite; }
  @keyframes bar { 0% { width: 0; } 55% { width: 72%; } 100% { width: 100%; } }
  .wrap { max-width: 960px; margin: 0 auto; padding: 24px 16px 48px; }
  h1 { font-size: 22px; margin: 0 0 4px; letter-spacing: .3px; }
  h1 .sub { color: var(--dim); font-size: 14px; font-weight: 400; }
  h2 { font-size: 15px; margin: 22px 0 8px; }
  .status {
    display: flex; flex-wrap: wrap; gap: 8px 18px; align-items: center;
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 8px; padding: 10px 14px; margin: 14px 0;
    font-size: 13px; color: var(--dim);
  }
  .status b { color: var(--text); font-weight: 600; }
  .status .ok { color: var(--green); }
  .status .bad { color: var(--red); }
  .banner {
    border: 1px solid var(--yellow); color: var(--yellow);
    background: rgba(217,180,74,.08); border-radius: 8px;
    padding: 8px 14px; margin: 0 0 14px; font-size: 13px;
  }
  .banner.err { border-color: var(--red); color: var(--red); background: rgba(201,79,79,.08); }
  .addrow { display: flex; gap: 8px; margin-bottom: 8px; }
  input[type=text] {
    flex: 1; background: var(--panel); color: var(--text);
    border: 1px solid var(--border); border-radius: 8px;
    padding: 10px 12px; font-size: 15px; outline: none;
  }
  input[type=text]:focus { border-color: var(--gold); }
  button {
    background: var(--gold); color: #14110a; border: 0; border-radius: 8px;
    padding: 10px 16px; font-size: 15px; font-weight: 700; cursor: pointer;
  }
  button:disabled { opacity: .55; cursor: default; filter: none; }
  button.ghost {
    background: transparent; color: var(--dim); border: 1px solid var(--border);
    font-weight: 400; padding: 10px 12px; font-size: 13px;
  }
  button:hover:not(:disabled) { filter: brightness(1.1); }
  .cands { display: none; flex-direction: column; gap: 4px; margin: 0 0 10px; }
  .cands.show { display: flex; }
  .cand {
    display: flex; align-items: center; gap: 10px; cursor: pointer;
    background: var(--panel2); border: 1px solid var(--border);
    border-radius: 8px; padding: 8px 12px;
  }
  .cand:hover { border-color: var(--gold); }
  .cand img { width: 28px; height: 28px; }
  .cand .cname { flex: 1; font-weight: 600; }
  .cand .q { font-size: 12px; color: var(--dim); white-space: nowrap; }
  .cand .addbtn { padding: 6px 16px; font-size: 13px; flex: none; }
  .tblwrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  table { width: 100%; border-collapse: collapse; background: var(--panel);
    border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
  th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--border); }
  th { font-size: 12px; text-transform: uppercase; letter-spacing: .8px;
    color: var(--dim); font-weight: 600; background: var(--panel2); }
  tr:last-child td { border-bottom: 0; }
  td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
  td.qty { font-size: 17px; font-weight: 700; }
  td.qty.zero { color: var(--red); }
  td.name { display: flex; align-items: center; gap: 10px; font-weight: 600; }
  td.name img { width: 28px; height: 28px; flex: none; }
  .common { color: #e8e2d4; } .uncommon { color: #1eff00; }
  .rare { color: #0070dd; } .epic { color: #a335ee; } .legendary { color: #ff8000; }
  .chip {
    display: inline-block; border: 1px solid var(--gold); color: var(--gold);
    border-radius: 10px; padding: 1px 8px; font-size: 11px; margin: 1px 3px 1px 0;
    white-space: nowrap;
  }
  .del {
    background: none; border: 0; color: var(--dim); cursor: pointer;
    font-size: 16px; padding: 2px 6px; border-radius: 4px;
  }
  .del:hover:not(:disabled) { color: var(--red); filter: none; }
  .editrow td { background: var(--bg); }
  .edwrap { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }
  .edwrap label { font-size: 12px; color: var(--dim); display: flex; align-items: center; gap: 6px; }
  input[type=number] {
    background: var(--panel); color: var(--text); border: 1px solid var(--border);
    border-radius: 6px; padding: 6px 8px; width: 92px; font-size: 13px; outline: none;
  }
  input[type=number]:focus { border-color: var(--gold); }
  .edwrap button { padding: 6px 14px; font-size: 13px; }
  .dim { color: var(--dim); }
  .empty { text-align: center; color: var(--dim); padding: 40px 0; }
  #alerts {
    list-style: none; margin: 0; padding: 0; background: var(--panel);
    border: 1px solid var(--border); border-radius: 8px; overflow: hidden;
  }
  #alerts li { padding: 8px 14px; border-bottom: 1px solid var(--border); font-size: 13px; }
  #alerts li:last-child { border-bottom: 0; }
  .toast {
    position: fixed; bottom: 22px; left: 50%; transform: translateX(-50%);
    background: var(--panel2); border: 1px solid var(--gold); color: var(--text);
    padding: 9px 18px; border-radius: 8px; font-size: 14px;
    max-width: calc(100vw - 32px); width: max-content; text-align: center;
    opacity: 0; pointer-events: none; transition: opacity .2s; z-index: 100;
  }
  .toast.show { opacity: 1; }
  .toast.err { border-color: var(--red); color: var(--red); }
  footer { margin-top: 18px; font-size: 12px; color: var(--dim); }
  footer a { color: var(--gold); text-decoration: none; }

  /* tablet: keep the table inside the column */
  @media (max-width: 920px) {
    th, td { padding: 9px 10px; }
  }

  /* mobile: table rows become stacked cards */
  @media (max-width: 720px) {
    .wrap { padding: 16px 12px 40px; }
    h1 { font-size: 19px; }
    h1 .sub { display: block; margin: 2px 0 0; }
    .status { font-size: 12px; gap: 5px 12px; padding: 10px 12px; }
    .addrow { flex-wrap: wrap; row-gap: 8px; }
    .addrow input[type=text] { flex: 1 1 100%; }
    .addrow button { flex: 1; padding: 11px 10px; white-space: normal; line-height: 1.15; }
    .addrow button.ghost { flex: 1.3; }
    .cand { flex-wrap: wrap; }
    .cand .q { flex-basis: 100%; order: 3; white-space: normal; }
    .del { font-size: 18px; padding: 6px 10px; }

    .tblwrap { overflow: visible; }
    table { display: block; border: 0; border-radius: 0; background: transparent; overflow: visible; }
    thead { display: none; }
    tbody, tr, td { display: block; }
    tr[data-row] {
      background: var(--panel); border: 1px solid var(--border);
      border-radius: 10px; margin-bottom: 10px; overflow: hidden;
    }
    td {
      display: flex; justify-content: space-between; align-items: center;
      gap: 12px; padding: 9px 12px; text-align: right; font-size: 14px;
      border-bottom: 1px solid var(--border);
    }
    tr[data-row] td:last-child { border-bottom: 0; }
    td::before {
      content: attr(data-label); color: var(--dim); font-size: 11px;
      text-transform: uppercase; letter-spacing: .6px; font-weight: 600; flex: none;
    }
    td.name {
      justify-content: flex-start; padding: 12px; font-size: 15px;
    }
    td.name::before { content: none; }
    td.name span { flex: 1; word-break: break-word; }
    td.qty { font-size: 18px; }
    td.th { justify-content: flex-start; flex-wrap: wrap; text-align: left; }
    td.th .chip, td.th .dim { margin-left: 0; }
    td.act { justify-content: flex-end; background: var(--panel2); border-top: 1px solid var(--border); }
    td.act::before { content: none; }

    .editrow td { display: block; padding: 12px; border: 0; background: var(--bg); }
    .edwrap { gap: 10px; }
    .edwrap label { flex: 1 1 100%; justify-content: space-between; }
    .edwrap label input { flex: 1; width: auto; margin-left: 8px; }
  }
</style>
</head>
<body>
<div id="bar"></div>
<div class="wrap">
  <h1>AH Quantity Tracker <span class="sub">· WoW: Forever</span></h1>
  <div class="status" id="status">loading…</div>
  <div class="banner" id="banner" style="display:none"></div>

  <div class="addrow">
    <input type="text" id="add" placeholder="Add an item — e.g. linen cloth, wool, runecloth…"
           autocomplete="off">
    <button id="addbtn">Add</button>
    <button class="ghost" id="pollbtn" title="Fetch market now">↻ Refresh now</button>
  </div>
  <div class="cands" id="cands"></div>

  <div class="tblwrap">
  <table id="table" style="display:none">
    <thead><tr>
      <th>Item</th>
      <th>Alerts</th>
      <th class="num">Quantity</th>
      <th class="num">Min buyout</th>
      <th class="num">Median</th>
      <th class="num">Updated</th>
      <th></th>
    </tr></thead>
    <tbody id="rows"></tbody>
  </table>
  </div>
  <div class="empty" id="empty">No items on the watchlist yet — add one above.</div>

  <div id="alertsec" style="display:none">
    <h2>Recent alerts <span class="dim" id="alertmail" style="font-size:12px;font-weight:400"></span></h2>
    <ul id="alerts"></ul>
  </div>

  <footer>
    Prices from <a href="https://ahledger.com" target="_blank">AHledger</a> ·
    polls every 10 min · data refreshed ~hourly per market
  </footer>
</div>
<div class="toast" id="toast"></div>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? "").replace(/[&<>"']/g,
  c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

let pending = 0;
function setBusy(n) {
  pending = Math.max(0, pending + n);
  $("bar").classList.toggle("show", pending > 0);
}
async function api(path, opts) {
  setBusy(1);
  try {
    const r = await fetch(path, opts);
    return await r.json();
  } finally { setBusy(-1); }
}
function toast(msg, ok = true) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast show" + (ok ? "" : " err");
  clearTimeout(t._h);
  t._h = setTimeout(() => t.classList.remove("show"), 2600);
}

function timeAgo(iso) {
  if (!iso) return "—";
  const s = (Date.now() - new Date(iso)) / 1000;
  if (s < 90) return "just now";
  if (s < 3600) return Math.round(s/60) + "m ago";
  if (s < 86400) return Math.round(s/3600) + "h ago";
  return Math.round(s/86400) + "d ago";
}
function fmtCopper(v) {
  if (v == null) return "—";
  return v >= 10000
    ? (v/10000).toLocaleString(undefined, {maximumFractionDigits: 2}) + "g"
    : v.toLocaleString() + "c";
}

async function loadStatus() {
  try {
    const d = await api("/api/status");
    $("status").innerHTML =
      `Market: <b>${esc(d.market)}</b>` +
      (d.lastPoll ? ` · Last poll: <b class="${d.error ? "bad" : "ok"}">${timeAgo(d.lastPoll)}</b>` : "") +
      (d.note ? ` · ${esc(d.note)}` : "") +
      ` · Email: <b class="${d.smtpConfigured ? "ok" : "bad"}">${d.smtpConfigured ? "alerts on" : "not set"}</b>`;
    $("alertmail").textContent = d.smtpConfigured ? "· email delivery on" : "· email not configured — alerts logged here only";
    const b = $("banner");
    if (d.error) {
      b.className = "banner err"; b.style.display = "block";
      b.textContent = "Last poll failed (retrying automatically): " + d.error;
    } else if (d.stale) {
      b.className = "banner"; b.style.display = "block";
      b.textContent = "Data may be stale — no successful poll in over 3 hours.";
    } else {
      b.style.display = "none";
    }
  } catch (e) { /* server briefly away */ }
}

let items = [];
function thChips(t) {
  const chips = [];
  if (t.qtyBelow != null) chips.push(`qty ≤ ${t.qtyBelow.toLocaleString()}`);
  if (t.qtyAbove != null) chips.push(`qty ≥ ${t.qtyAbove.toLocaleString()}`);
  if (t.priceBelow != null) chips.push(`≤ ${fmtCopper(t.priceBelow)}`);
  return chips.length
    ? chips.map(c => `<span class="chip">${c}</span>`).join(" ")
    : '<span class="dim">no alerts</span>';
}

async function loadItems() {
  const d = await api("/api/items");
  items = d.items || [];
  $("table").style.display = items.length ? "table" : "none";
  $("empty").style.display = items.length ? "none" : "block";
  $("rows").innerHTML = items.map(r => `
    <tr data-row="${r.id}">
      <td class="name">
        ${r.icon ? `<img src="${esc(r.icon)}" alt="" loading="lazy">` : ""}
        <span class="${esc(r.quality ? r.quality.toLowerCase() : "common")}">${esc(r.name)}</span>
      </td>
      <td class="th" data-label="Alerts">${thChips(r.thresholds)}</td>
      <td class="num qty ${r.quantity === 0 ? "zero" : ""}" data-label="Quantity">
        ${r.quantity === null ? "—" : r.quantity.toLocaleString()}
      </td>
      <td class="num" data-label="Min buyout">${r.minBuyout ?? "—"}</td>
      <td class="num" data-label="Median">${r.median ?? "—"}</td>
      <td class="num dim" data-label="Updated">${timeAgo(r.updated)}</td>
      <td class="act">
        <button class="del" data-act="gear" data-id="${r.id}" title="Set alert thresholds">⚙</button>
        <button class="del" data-act="del" data-id="${r.id}" title="Remove">✕</button>
      </td>
    </tr>`).join("");
}

function toggleEdit(id) {
  const existing = document.querySelector(`.editrow[data-id="${id}"]`);
  if (existing) { existing.remove(); return; }
  const it = items.find(x => x.id === id);
  if (!it) return;
  const t = it.thresholds;
  const tr = document.createElement("tr");
  tr.className = "editrow";
  tr.dataset.id = id;
  tr.innerHTML = `<td colspan="7"><div class="edwrap">
    <label>Alert when qty ≤ <input type="number" min="1" data-k="qtyBelow" value="${t.qtyBelow ?? ""}"></label>
    <label>qty ≥ <input type="number" min="1" data-k="qtyAbove" value="${t.qtyAbove ?? ""}"></label>
    <label>min buyout ≤ <input type="number" min="0" data-k="priceBelow" value="${t.priceBelow ?? ""}"> copper</label>
    <button data-act="save" data-id="${id}">Save</button>
    <button class="ghost" data-act="clearth" data-id="${id}">Clear</button>
    <span class="dim" style="font-size:11px">blank = off · price in copper</span>
  </div></td>`;
  document.querySelector(`tr[data-row="${id}"]`).after(tr);
  tr.querySelector("input").focus();
}

async function saveTh(id) {
  const tr = document.querySelector(`.editrow[data-id="${id}"]`);
  if (!tr) return;
  const payload = {};
  for (const inp of tr.querySelectorAll("input")) {
    const v = inp.value.trim() === "" ? null : Math.max(0, Math.round(+inp.value || 0));
    payload[inp.dataset.k] = v;
  }
  const r = await api(`/api/items/${id}/thresholds`, {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify(payload)
  });
  if (r.error) { toast(r.error, false); return; }
  toast("Thresholds saved");
  loadItems();
}

let cands = [];
function hideCands() {
  $("cands").classList.remove("show");
  $("cands").innerHTML = "";
  cands = [];
}
function showCands(list) {
  cands = list;
  $("cands").innerHTML = list.map((c, i) => `
    <div class="cand" data-idx="${i}">
      ${c.icon ? `<img src="${esc(c.icon)}" alt="">` : ""}
      <span class="cname">${esc(c.name)}</span>
      <span class="q">${esc(c.quality || "")} · id ${c.blizzardId}</span>
      <button class="addbtn" data-idx="${i}">Add</button>
    </div>`).join("");
  $("cands").classList.add("show");
}

async function addCand(i) {
  const c = cands[i];
  if (!c) return;
  const el = document.querySelector(`.cand[data-idx="${i}"]`);
  const btn = el ? el.querySelector(".addbtn") : null;
  if (btn) { btn.disabled = true; btn.textContent = "…"; }
  const r = await api("/api/items", {
    method: "POST", headers: {"Content-Type": "application/json"},
    body: JSON.stringify({id: c.blizzardId, name: c.name,
                          icon: c.icon, quality: c.quality})
  });
  if (r.error) {
    toast(r.error, false);
    if (btn) { btn.disabled = false; btn.textContent = "Add"; }
    return;
  }
  toast(`Added ${c.name}`);
  $("add").value = "";
  hideCands();
  loadItems();
}

async function delItem(id) {
  const r = await api("/api/items/" + id, {method: "DELETE"});
  toast(r.error ? r.error : "Removed");
  loadItems();
}

async function addItem() {
  const name = $("add").value.trim();
  if (!name) return;
  const btn = $("addbtn");
  if (btn.disabled) return;
  hideCands();
  btn.disabled = true;
  btn.textContent = "Searching…";
  try {
    const r = await api("/api/items", {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({name})
    });
    if (r.error) { toast(r.error, false); return; }
    if (r.candidates) {
      showCands(r.candidates);
      toast(`${r.candidates.length} match${r.candidates.length > 1 ? "es" : ""} — pick one`);
      return;
    }
    $("add").value = "";
    toast(`Added ${r.name || name}`);
    loadItems();
  } finally {
    btn.disabled = false;
    btn.textContent = "Add";
  }
}

async function loadAlerts() {
  const d = await api("/api/alerts");
  const list = d.alerts || [];
  $("alertsec").style.display = list.length ? "block" : "none";
  $("alerts").innerHTML = list.map(a =>
    `<li><span class="dim">${timeAgo(a.ts)}</span> — ${esc(a.message)}${
      a.emailed ? "" : ' <span class="dim">(email pending)</span>'}</li>`
  ).join("");
}

$("addbtn").onclick = addItem;
$("add").addEventListener("keydown", e => { if (e.key === "Enter") addItem(); });
$("cands").addEventListener("click", e => {
  const el = e.target.closest(".cand");
  if (el) addCand(+el.dataset.idx);
});
$("rows").addEventListener("click", e => {
  const b = e.target.closest("button[data-act]");
  if (!b) return;
  const id = +b.dataset.id;
  const act = b.dataset.act;
  if (act === "del") { delItem(id); }
  else if (act === "gear") { toggleEdit(id); }
  else if (act === "save") { saveTh(id); }
  else if (act === "clearth") {
    api(`/api/items/${id}/thresholds`, {
      method: "POST", headers: {"Content-Type": "application/json"},
      body: JSON.stringify({qtyBelow: null, qtyAbove: null, priceBelow: null})
    }).then(r => {
      if (r.error) toast(r.error, false);
      else toast("Alerts cleared");
      loadItems();
    });
  }
});
$("pollbtn").onclick = async () => {
  if ($("pollbtn").disabled) return;
  $("pollbtn").disabled = true;
  $("pollbtn").textContent = "Polling…";
  toast("Fetching market data…");
  setBusy(1);
  try {
    await fetch("/api/poll", {method: "POST"});
    // hold the bar until the server-side fetch actually finishes
    for (let i = 0; i < 150; i++) {
      await new Promise(r => setTimeout(r, 2000));
      const st = await (await fetch("/api/status")).json();
      if (!st.polling) break;
    }
  } finally { setBusy(-1); }
  loadStatus(); loadItems(); loadAlerts();
  $("pollbtn").disabled = false;
  $("pollbtn").textContent = "↻ Refresh now";
};

loadStatus(); loadItems(); loadAlerts();
setInterval(() => { loadStatus(); loadItems(); loadAlerts(); }, 30000);
</script>
</body>
</html>
"""

# ---------------------------------------------------------------- main

def main():
    init_db()
    threading.Thread(target=poller_loop, daemon=True).start()
    httpd = ThreadingHTTPServer((HOST, PORT), Handler)
    httpd.daemon_threads = True
    log(f"ah-tracker listening on {HOST}:{PORT} market={MARKET}")
    def shutdown(signum, frame):
        log(f"shutting down (signal {signum})")
        threading.Thread(target=httpd.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    httpd.serve_forever()
    httpd.server_close()

if __name__ == "__main__":
    main()
