"""Convexity100 live options pre-ignition scanner (paper alerts only).

Market-wide minute-aggregate discovery (AM.*), selectively subscribes to live
NBBO option quotes (Q.<contract>). No automatic trading or claimed probabilities.
"""
from __future__ import annotations

import argparse
import asyncio
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, time, timezone
import gzip
import json
import logging
import math
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import os
from pathlib import Path
import re
import sqlite3
import time as time_module
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()
NY = ZoneInfo("America/New_York")
LOG = logging.getLogger("convexity100")
SYMBOL_PATTERN = re.compile(r"^O:([A-Z0-9.\-]+?)(\d{6})([CP])(\d{8})$")


def env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


@dataclass
class Settings:
    key: str = os.getenv("MASSIVE_API_KEY", "")
    feed: str = os.getenv("FEED", "realtime").lower()
    db_path: str = os.getenv("DB_PATH", "data/scanner.sqlite")
    capture_path: str = os.getenv("CAPTURE_PATH", "")
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    min_bar_volume: int = env_int("MIN_BAR_VOLUME", 35)
    min_day_volume: int = env_int("MIN_DAY_VOLUME", 120)
    min_burst: float = env_float("MIN_VOLUME_BURST", 2.5)
    min_momentum: float = env_float("MIN_PRICE_MOMENTUM", 0.08)
    max_gain_from_open: float = env_float("MAX_GAIN_FROM_OPEN", 0.75)
    min_option_ask: float = env_float("MIN_OPTION_ASK", 0.15)
    max_option_ask: float = env_float("MAX_OPTION_ASK", 8.0)
    max_spread: float = env_float("MAX_SPREAD", 0.40)
    max_quote_subs: int = min(env_int("MAX_QUOTE_SUBS", 500), 950)
    watch_minutes: int = env_int("WATCH_MINUTES", 12)
    cooldown_minutes: int = env_int("COOLDOWN_MINUTES", 60)

    min_quote_size: int = env_int("MIN_QUOTE_SIZE", 1)
    max_quote_age_ms: int = env_int("MAX_QUOTE_AGE_MS", 5000)
    slippage: float = env_float("SLIPPAGE_PER_SHARE", 0.01)
    fee: float = env_float("FEE_PER_CONTRACT", 0.65)

    def validate(self) -> None:
        if self.min_quote_size < 1 or self.max_quote_age_ms < 1:
            raise ValueError("Quote size and age must be positive")
        if not all(math.isfinite(v) and v >= 0 for v in (self.slippage, self.fee)):
            raise ValueError("Invalid transaction costs")
        if self.feed not in {"realtime", "delayed"}:
            raise ValueError("FEED must be realtime or delayed")
        if not 0 < self.max_spread <= 1:
            raise ValueError("MAX_SPREAD must be between 0 and 1 (spread/midpoint)")
        if not 1 <= self.max_quote_subs <= 950:
            raise ValueError("MAX_QUOTE_SUBS must be between 1 and 950")


def parse_contract(ticker: str) -> dict | None:
    match = SYMBOL_PATTERN.fullmatch(ticker)
    if not match:
        return None
    root, short_date, cp, encoded_strike = match.groups()
    try:
        expiration = datetime.strptime(short_date, "%y%m%d").date()
    except ValueError:
        return None
    return {"underlying": root, "expiration": expiration,
            "side": "CALL" if cp == "C" else "PUT",
            "strike": int(encoded_strike) / 1000}


def as_utc_ms(ms: int | float) -> datetime:
    return datetime.fromtimestamp(ms / 1000.0, timezone.utc)


def next_weekday(d):
    # MVP calendar: US equity holiday handling must be upgraded before live order routing.
    d += timedelta(days=1)
    while d.weekday() > 4:
        d += timedelta(days=1)
    return d


def cutoff_ms(signal_ms: int, expiration_date) -> int:
    import exchange_calendars as xc
    calendar = xc.get_calendar("XNYS")
    day = as_utc_ms(signal_ms).astimezone(NY).date()
    sessions = calendar.sessions_in_range(day, day + timedelta(days=15))
    future = [d for d in sessions if d.date() > day]
    if not future:
        raise ValueError("Calendar has no next session")
    end = min(future[0].date(), expiration_date)
    eligible = calendar.sessions_in_range(end - timedelta(days=10), end)
    return int(calendar.session_close(eligible[-1]).timestamp() * 1000)


class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS signals (
                ticker TEXT PRIMARY KEY, underlying TEXT NOT NULL, expiration TEXT NOT NULL,
                side TEXT NOT NULL, strike REAL NOT NULL,
                signal_ms INTEGER NOT NULL, alert_ms INTEGER NOT NULL,
                entry_ask REAL NOT NULL, entry_bid REAL NOT NULL,
                spread_pct REAL NOT NULL, burst REAL NOT NULL,
                momentum REAL NOT NULL, priority INTEGER NOT NULL,
                cutoff_ms INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN',
                target100_ms INTEGER, target200_ms INTEGER, target300_ms INTEGER,
                stop_ms INTEGER, completed_ms INTEGER
            );
        """)
        existing = {row[1] for row in self.conn.execute("PRAGMA table_info(signals)")}
        for name, kind in (("entry_cost", "REAL"), ("last_quote_ms", "INTEGER"),
                           ("max_return", "REAL"), ("entry_size", "INTEGER")):
            if name not in existing:
                self.conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {kind}")
        self.conn.execute("CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, body TEXT NOT NULL, sent INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0)")
        self.conn.commit()

    def enqueue(self, body):
        self.conn.execute("INSERT INTO outbox(body) VALUES (?)", (body,))
        self.conn.commit()

    def add_alert(self, record: dict) -> bool:
        columns = ",".join(record)
        values = ",".join("?" for _ in record)
        cursor = self.conn.execute(f"INSERT OR IGNORE INTO signals ({columns}) VALUES ({values})", tuple(record.values()))
        self.conn.commit()
        return cursor.rowcount == 1

    def update_status(self, ticker: str, timestamp_ms: int, bid: float, slippage=0.0, fee=0.0):
        record = self.conn.execute("""
            SELECT entry_ask, cutoff_ms, status, target100_ms, target200_ms,
                   target300_ms, stop_ms, alert_ms, entry_cost, last_quote_ms FROM signals WHERE ticker=?
        """, (ticker,)).fetchone()
        if not record:
            return None
        entry, cutoff, status, t100, t200, t300, stop, alert_ms, cost, last = record
        entry = cost or entry
        bid = max(0, bid - slippage - fee / 100)
        if timestamp_ms <= max(alert_ms, last or 0) or timestamp_ms > cutoff or status not in {"OPEN", "HIT_100"}:
            return None
        self.conn.execute("UPDATE signals SET last_quote_ms=?, max_return=MAX(COALESCE(max_return,-1), ?) WHERE ticker=?", (timestamp_ms, bid / entry - 1, ticker))
        self.conn.commit()
        updates = {}
        if bid >= entry * 2 and t100 is None and stop is None:
            updates["target100_ms"] = timestamp_ms
            updates["status"] = "HIT_100"
        if bid >= entry * 3 and t200 is None and stop is None:
            updates["target200_ms"] = timestamp_ms
        if bid >= entry * 4 and t300 is None and stop is None:
            updates["target300_ms"] = timestamp_ms
        if bid <= entry * 0.50 and stop is None and t100 is None:
            updates["stop_ms"] = timestamp_ms
            updates["status"] = "STOP_FIRST"
        if updates:
            statement = ",".join(f"{k}=?" for k in updates)
            self.conn.execute(f"UPDATE signals SET {statement} WHERE ticker=?", (*updates.values(), ticker))
            self.conn.commit()
            return updates
        return None

    def settle_expired(self, now_ms: int):
        self.conn.execute("""UPDATE signals SET status=CASE WHEN status='HIT_100' THEN 'COMPLETE' ELSE 'TIMEOUT' END, completed_ms=?
            WHERE status IN ('OPEN','HIT_100') AND cutoff_ms < ?""", (now_ms, now_ms))
        self.conn.commit()

    def summary(self):
        return self.conn.execute("""SELECT status, COUNT(*) FROM signals GROUP BY status""").fetchall()


class Capture:
    def __init__(self, root: str):
        self.root = Path(root) if root else None
        self.current_date = None
        self.fp = None

    def write(self, obj: dict):
        if not self.root:
            return
        stamp = obj.get("e") or obj.get("t") or 0
        date = as_utc_ms(stamp).astimezone(NY).strftime("%Y-%m-%d")
        if self.current_date != date:
            if self.fp:
                self.fp.close()
            self.root.mkdir(parents=True, exist_ok=True)
            self.fp = gzip.open(self.root / f"options-{date}.jsonl.gz", "at", encoding="utf8")
            self.current_date = date
        self.fp.write(json.dumps(obj, separators=(",", ":")) + "\n")


class Scanner:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = Store(settings.db_path)
        self.capture = Capture(settings.capture_path)
        self.bars = defaultdict(lambda: deque(maxlen=7))
        self.candidates = {}
        self.subscribed = {}  # ticker -> subscription timestamp ms
        self.last_alert = {}
        self.pending_commands = []
        self.delayed_ms = 900_000 if settings.feed == "delayed" else 0
        self.alerts = deque(maxlen=1000)
        self.quote_order = {}
        self.rejections = defaultdict(int)
        self.received_ms = 0
        self.connected = False

    def on_aggregate(self, m: dict) -> None:
        self.capture.write(m)
        sym = m.get("sym", "")
        contract = parse_contract(sym)
        if not contract:
            return
        now_local = as_utc_ms(m.get("e", 0)).astimezone(NY)
        days_to_exp = (contract["expiration"] - now_local.date()).days
        if days_to_exp < 0 or days_to_exp > 10:
            return
        close, opened, vol = (float(m.get(k, 0) or 0) for k in ("c", "op", "v"))
        minute_ts = int(m.get("e", 0))
        if close < self.settings.min_option_ask or close > self.settings.max_option_ask:
            return
        if not all(math.isfinite(x) for x in (close, opened, vol)):
            return
        if self.bars[sym] and minute_ts <= self.bars[sym][-1][0]:
            # Continuously updated bars must not count as independent minutes.
            if minute_ts == self.bars[sym][-1][0]:
                self.bars[sym][-1] = (minute_ts, close, vol)
            self.rejections["duplicate_or_old_bar"] += 1
            return
        prev = list(self.bars[sym])
        self.bars[sym].append((minute_ts, close, vol))
        if len(prev) < 3:
            return
        # Require recent observations; stale prior trades are not baseline activity.
        recent = [p for p in prev if 0 < minute_ts - p[0] <= 8 * 60_000]
        if len(recent) < 3:
            return
        last3 = recent[-3:]
        avg = sum(x[2] for x in last3) / len(last3)
        burst = vol / max(avg, 3)
        momentum = close / max(last3[0][1], 0.01) - 1
        gain_open = close / opened - 1 if opened > 0 else None
        if vol < self.settings.min_bar_volume or float(m.get("av", 0) or 0) < self.settings.min_day_volume:
            return
        ignition = burst >= self.settings.min_burst and momentum >= self.settings.min_momentum
        accumulation = burst >= max(4.0, self.settings.min_burst) and -0.03 <= momentum < self.settings.min_momentum
        if not (ignition or accumulation):
            return
        # If the official open is missing, don't claim 'not yet doubled'.
        if gain_open is None or gain_open > self.settings.max_gain_from_open:
            return
        prior_alert = self.last_alert.get(sym, 0)
        if minute_ts - prior_alert < self.settings.cooldown_minutes * 60_000:
            return
        if sym in self.subscribed:
            return
        if len(self.subscribed) >= self.settings.max_quote_subs:
            self.rejections["subscription_capacity"] += 1
            return
        priority = min(99, int(20 + 10 * min(burst, 5) + 120 * min(momentum, 0.5)))
        self.candidates[sym] = {"signal_ms": minute_ts, "burst": burst,
                                "momentum": momentum, "priority": priority,
                                "gain_open": gain_open, "contract": contract}
        self.subscribed[sym] = minute_ts
        self.pending_commands.append(("subscribe", sym))
        LOG.info("Candidate %s burst %.2fx mom %+0.1f%% day %+0.1f%%", sym,
                 burst, momentum * 100, gain_open * 100)

    def on_quote(self, m: dict, now_ms: int | None = None) -> str | None:
        self.capture.write(m)
        sym = m.get("sym", "")
        quote_ms = int(m.get("t", 0))
        bid, ask = float(m.get("bp", 0) or 0), float(m.get("ap", 0) or 0)
        if not all(math.isfinite(x) for x in (bid, ask)) or bid <= 0 or ask <= 0 or bid > ask:
            return None
        actual_ms = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
        if quote_ms > actual_ms + 15_000 or actual_ms - quote_ms > self.delayed_ms + self.settings.max_quote_age_ms:
            return None
        if int(m.get("bs", 0)) < self.settings.min_quote_size or int(m.get("as", 0)) < self.settings.min_quote_size:
            self.rejections["insufficient_quote_size"] += 1
            return None
        ordering = (quote_ms, int(m.get("q", 0)))
        if ordering <= self.quote_order.get(sym, (0, 0)):
            self.rejections["old_quote"] += 1
            return None
        self.quote_order[sym] = ordering
        alert = self.store.update_status(sym, quote_ms, bid, self.settings.slippage, self.settings.fee)
        if alert:
            LOG.info("Outcome %s -> %s", sym, alert)
            self.store.enqueue(f"Convexity100 outcome {sym}: {json.dumps(alert)}; observed sized bid ${bid:.2f}. Paper tracking only.")
        candidate = self.candidates.get(sym)
        if not candidate:
            return None
        signal_ms = candidate["signal_ms"]
        if quote_ms < signal_ms or quote_ms - signal_ms > self.settings.watch_minutes * 60_000:
            return None
        # Reject stale/replayed delayed events masquerading as actionable real-time.
        spread = (ask - bid) / ((ask + bid) / 2.0)
        if (spread > self.settings.max_spread or ask < self.settings.min_option_ask or
                ask > self.settings.max_option_ask):
            return None
        info = candidate["contract"]
        deadline = cutoff_ms(quote_ms, info["expiration"])
        if quote_ms >= deadline:
            return None
        row = {
            "ticker": sym, "underlying": info["underlying"],
            "expiration": info["expiration"].isoformat(), "side": info["side"],
            "strike": info["strike"], "signal_ms": signal_ms,
            "alert_ms": quote_ms, "entry_ask": ask, "entry_bid": bid,
            "spread_pct": spread, "burst": candidate["burst"],
            "momentum": candidate["momentum"], "priority": candidate["priority"],
            "cutoff_ms": deadline, "status": "OPEN",
            "entry_cost": ask + self.settings.slippage + self.settings.fee / 100,
            "entry_size": int(m.get("as", 0)), "last_quote_ms": quote_ms
        }
        if not self.store.add_alert(row):
            del self.candidates[sym]
            return None
        self.last_alert[sym] = quote_ms
        del self.candidates[sym]
        stamp = as_utc_ms(quote_ms).astimezone(NY).strftime("%Y-%m-%d %H:%M:%S ET")
        label = "DELAYED / NOT TRADABLE AS REAL-TIME" if self.settings.feed == "delayed" else "REAL-TIME PAPER SIGNAL"
        msg = (f"Convexity100 {label}\n{sym} | {info['underlying']} "
               f"{info['side']} {info['strike']:g} expires {info['expiration']}\n"
               f"Quote {stamp}: bid ${bid:.2f} / ask ${ask:.2f}; spread {spread:.1%}\n"
               f"Quote-based hypothetical entry ask ${ask:.2f}\n"
               f"Exit BID targets: +100% ${ask*2:.2f} | +200% ${ask*3:.2f} | +300% ${ask*4:.2f}\n"
               f"Volume burst {candidate['burst']:.1f}x; option momentum {candidate['momentum']:+.1%}; "
               f"priority {candidate['priority']}/99 (NOT probability)\n"
               f"Paper stop BID ${ask*0.5:.2f}; maximum hold until "
               f"{as_utc_ms(deadline).astimezone(NY):%Y-%m-%d %H:%M ET}.\n"
               f"Signal is experimental, not a trade instruction.")
        cost = row["entry_cost"]
        targets = [cost * multiple + self.settings.slippage + self.settings.fee / 100 for multiple in (2, 3, 4)]
        msg += (f"\nCost-adjusted BID barriers: +100% ${targets[0]:.4f}; +200% ${targets[1]:.4f}; +300% ${targets[2]:.4f}. Sized quotes are observations, not confirmed fills.")
        self.store.enqueue(msg)
        self.alerts.append(msg)
        return msg

    def maintenance(self, event_ms: int):
        active = {row[0] for row in self.store.conn.execute(
            "SELECT ticker FROM signals WHERE status IN ('OPEN','HIT_100') AND cutoff_ms >= ?",
            (event_ms,))}
        stale = [s for s, t in self.subscribed.items()
                 if event_ms - t > self.settings.watch_minutes * 60_000 and s not in active]
        for sym in stale:
            self.pending_commands.append(("unsubscribe", sym))
            self.subscribed.pop(sym, None)
            self.candidates.pop(sym, None)
        # Bound quote subscriptions: completed alerts still get tracked until cutoff.
        if len(self.subscribed) >= self.settings.max_quote_subs:
            LOG.warning("Quote subscription capacity reached (%d). Some candidates will be skipped.",
                        self.settings.max_quote_subs)
        self.store.settle_expired(event_ms)
        self.quote_order = {k:v for k,v in self.quote_order.items() if k in self.subscribed}
        self.last_alert = {k:v for k,v in self.last_alert.items() if event_ms-v < 86400000}
        # Keep memory bounded across long-lived market sessions.
        stale_bars = [sym for sym, bars in self.bars.items() if bars and event_ms - bars[-1][0] > 2*3600_000]
        for sym in stale_bars:
            self.bars.pop(sym, None)


def post_telegram(token: str, chat_id: str, message: str):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    req = Request(url, data=urlencode({"chat_id": chat_id, "text": message}).encode(),
                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urlopen(req, timeout=10) as res:
        payload = json.loads(res.read())
        if res.status != 200 or not payload.get("ok"):
            raise RuntimeError("Telegram rejected message")
        return res.status


async def run_live(settings: Settings):
    if not settings.key:
        raise SystemExit("MASSIVE_API_KEY missing: set as secret environment variable")
    import websockets
    scanner = Scanner(settings)
    host = "socket.massive.com" if settings.feed == "realtime" else "delayed.massive.com"
    backoff = 2
    sender = asyncio.create_task(deliver_outbox(scanner))
    start_health_server(scanner)
    while True:
        try:
            async with websockets.connect(f"wss://{host}/options", max_size=16_000_000,
                                          ping_interval=20, ping_timeout=25) as ws:
                LOG.info("Connected to %s / options; authorizing", host)
                await ws.send(json.dumps({"action": "auth", "params": settings.key}))
                scanner.connected = False
                authorized = False
                for _ in range(5):
                    first = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                    statuses = first if isinstance(first, list) else [first]
                    for obj in statuses:
                        if obj.get("status") == "auth_success":
                            authorized = True
                        if obj.get("status") in {"auth_failed", "max_connections"}:
                            raise RuntimeError("Massive authorization error")
                    if authorized:
                        break
                if not authorized:
                    raise RuntimeError("No auth_success received; verify API key/plan")
                await ws.send(json.dumps({"action": "subscribe", "params": "AM.*"}))
                LOG.info("Subscribed to market-wide options minute bars (AM.*)")
                # Re-subscribe open alerts after reconnect so success/failure is not lost.
                open_rows = scanner.store.conn.execute(
                    "SELECT ticker, signal_ms FROM signals WHERE status IN ('OPEN','HIT_100') AND cutoff_ms>=?", (int(datetime.now(timezone.utc).timestamp()*1000)-scanner.delayed_ms,)).fetchall()
                scanner.connected = True
                scanner.pending_commands.clear()
                scanner.subscribed.clear()
                scanner.candidates.clear()
                for symbol, alert_ts in open_rows:
                    scanner.subscribed[symbol] = alert_ts
                    scanner.pending_commands.append(("subscribe", symbol))
                backoff = 2
                last_maint = 0
                last_report = time_module.monotonic()
                bar_count = 0
                while True:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        LOG.info("No option events for 60s; may be outside options hours or feed is not entitled")
                        now_ms = int(datetime.now(timezone.utc).timestamp()*1000)-scanner.delayed_ms
                        scanner.maintenance(now_ms)
                        continue
                    scanner.received_ms = int(time_module.time()*1000)
                    parsed = json.loads(raw)
                    events = parsed if isinstance(parsed, list) else [parsed]
                    for m in events:
                        if m.get("ev") == "AM":
                            bar_count += 1
                            scanner.on_aggregate(m)
                        elif m.get("ev") == "Q":
                            msg = scanner.on_quote(m)
                            if msg:
                                LOG.warning("%s", msg.replace("\n", " | "))
                        elif m.get("ev") == "status":
                            LOG.info("Massive subscription status: %s", m.get("status"))
                            if m.get("status") in {"auth_failed", "max_connections", "not_authorized"}:
                                raise RuntimeError("Massive status error")
                    # Commands are sent on same Options connection (no second socket needed).
                    while scanner.pending_commands:
                        action, symbol = scanner.pending_commands.pop(0)
                        if action == "subscribe" and len(scanner.subscribed) > settings.max_quote_subs:
                            scanner.subscribed.pop(symbol, None)
                            scanner.candidates.pop(symbol, None)
                            continue
                        await ws.send(json.dumps({"action": action, "params": f"Q.{symbol}"}))
                    if time_module.monotonic() - last_report >= 300:
                        LOG.info("5-minute heartbeat: %d option bars, %d quote subscriptions, %d paper alerts",
                                 bar_count, len(scanner.subscribed), len(scanner.alerts))
                        bar_count = 0
                        last_report = time_module.monotonic()
                    new_ms = int(datetime.now(timezone.utc).timestamp()*1000) - scanner.delayed_ms
                    if new_ms - last_maint >= 60_000:
                        scanner.maintenance(new_ms)
                        last_maint = new_ms
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            scanner.connected = False
            LOG.error("Connection interrupted (%s); retrying in %ds", type(exc).__name__, backoff)
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)


async def deliver_outbox(scanner):
    """Durable at-least-once delivery; retry failures without printing tokens."""
    while True:
        settings = scanner.settings
        if settings.telegram_token and settings.telegram_chat_id:
            row = scanner.store.conn.execute("SELECT id,body,attempts FROM outbox WHERE sent=0 ORDER BY id LIMIT 1").fetchone()
            if row:
                try:
                    await asyncio.to_thread(post_telegram, settings.telegram_token, settings.telegram_chat_id, row[1])
                    scanner.store.conn.execute("UPDATE outbox SET sent=1 WHERE id=?", (row[0],))
                except Exception:
                    scanner.store.conn.execute("UPDATE outbox SET attempts=attempts+1 WHERE id=?", (row[0],))
                    scanner.store.conn.commit()
                    LOG.error("Telegram delivery failed; retained in outbox for retry")
                    await asyncio.sleep(min(60, 2 ** min(row[2]+1, 6)))
                scanner.store.conn.commit()
        await asyncio.sleep(0.25)


def start_health_server(scanner):
    class Health(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path not in {"/healthz", "/readyz"}:
                self.send_error(404); return
            ready = scanner.connected and time_module.time()*1000 - scanner.received_ms < 90000
            self.send_response(200 if self.path == "/healthz" or ready else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"alive": True, "feed_connected": scanner.connected, "recent_feed": ready}).encode())
        def log_message(self, *args):
            pass
    server = HTTPServer(("0.0.0.0", int(os.getenv("PORT", "8080"))), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def run_demo():
    now = datetime.now(timezone.utc)
    expiry = next_weekday(now.astimezone(NY).date())
    sym = f"O:SPY{expiry:%y%m%d}C00600000"
    demo_db = "data/demo.sqlite"
    # A demo is intentionally repeatable: never mix synthetic prices with real alerts.
    for suffix in ("", "-wal", "-shm"):
        Path(demo_db + suffix).unlink(missing_ok=True)
    settings = Settings(db_path=demo_db, slippage=0, fee=0)
    sc = Scanner(settings)
    base = int(now.timestamp()*1000) - 3 * 60_000
    for i, (value, volume) in enumerate(zip([1.00,1.00,1.02,1.18], [20,24,18,100])):
        t = base + i*60_000
        sc.on_aggregate({"ev":"AM", "sym":sym,"s":t-60_000,"e":t,
                         "o":value*.95,"c":value,"op":.90,"v":volume,"av":200+i*100})
    signal = base+3*60_000
    alert = sc.on_quote({"ev":"Q","sym":sym,"t":signal+3000,"bp":1.15,"ap":1.25,"bs":10,"as":10})
    if not alert:
        raise RuntimeError("Demo signal did not fire")
    print(alert)
    sc.on_quote({"ev":"Q","sym":sym,"t":signal+15000,"bp":2.60,"ap":2.70,"bs":10,"as":10})
    print("Audit", sc.store.summary())
    print("Demo produced simulated market data, not actual live prices.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--demo", action="store_true", help="simulate a positive signal without API key")
    parser.add_argument("--summary", action="store_true", help="show paper-trade outcomes")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings()
    settings.validate()
    if args.demo:
        run_demo()
    elif args.summary:
        print(Scanner(settings).store.summary())
    else:
        asyncio.run(run_live(settings))
