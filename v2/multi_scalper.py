"""
multi_scalper.py - Kalshi 7-Market Paper Scalper

Runs a momentum-follow strategy across all 7 crypto 15-min markets
simultaneously using one thread per series.

Strategy:
  1. Wait 3 min after contract open for direction to establish.
  2. If YES bid >= 60c (bullish) → buy YES at ask.
     If NO bid  >= 60c (bearish) → buy NO  at ask.
     If between 40-60c (no signal) → skip.
  3. TP=82c, SL=50c mid, time-stop last 2 min.

All trades are PAPER ONLY — no real orders placed.

Run:
    python multi_scalper.py

Output:
    logs/multi_scalper.log
    data/bot.db  (bracket_trade_log table)
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import os
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Series configuration
# ---------------------------------------------------------------------------

SERIES = [
    "KXBTC15M",
    "KXETH15M",
    "KXSOL15M",
    "KXXRP15M",
    "KXDOGE15M",
    "KXBNB15M",
    "KXHYPE15M",
]

# Strategy parameters - Momentum Follow
MOMENTUM_THRESHOLD_CENTS = 65   # raised from 60: require stronger conviction
MOMENTUM_MIN_ENTRY_CENTS = 62   # skip if ask is below this (weak conviction)
MOMENTUM_MAX_ENTRY_CENTS = 72   # skip if ask already above this (bad R/R)
ENTRY_WAIT_SECONDS       = 180  # wait 3 min for direction to establish
SCAN_WINDOW_SECONDS      = 900  # scan until time-stop kicks in (tte check inside loop)
TAKE_PROFIT_CENTS        = 85   # exit when bid reaches this
STOP_LOSS_CENTS          = 50   # exit if mid drops back to 50c
SL_OFFSET_CENTS          = 12   # SL = entry - SL_OFFSET_CENTS (floored at STOP_LOSS_CENTS)
SL_ALERT_CENTS           = 55   # switch to fast polling when mid drops here
TIME_STOP_SECONDS        = 120  # exit at whatever price with 2 min left
CONTRACTS                = 1

MARKET_POLL_INTERVAL = 1   # seconds between contract detection polls
ORDER_POLL_INTERVAL  = 1   # seconds between fill/exit checks
SL_POLL_INTERVAL     = 1   # fast polling when near stop loss

# ---------------------------------------------------------------------------
# Risk profiles — loaded at startup via --profile arg
# ---------------------------------------------------------------------------

PROFILES: dict[str, dict] = {
    "conservative": {
        "MOMENTUM_THRESHOLD_CENTS": 62,
        "MOMENTUM_MIN_ENTRY_CENTS": 62,
        "MOMENTUM_MAX_ENTRY_CENTS": 72,
        "ENTRY_WAIT_SECONDS":        60,
        "SCAN_WINDOW_SECONDS":      900,
        "TAKE_PROFIT_CENTS":        82,
        "STOP_LOSS_CENTS":          50,
        "SL_OFFSET_CENTS":          12,
        "SL_ALERT_CENTS":           57,
        "TIME_STOP_SECONDS":        120,
        "DAILY_LOSS_LIMIT_CENTS":  -300,
        "EXCLUDED_SERIES":          {"KXHYPE15M", "KXBNB15M"},
    },
    "moderate": {
        "MOMENTUM_THRESHOLD_CENTS": 65,
        "MOMENTUM_MIN_ENTRY_CENTS": 62,
        "MOMENTUM_MAX_ENTRY_CENTS": 72,
        "ENTRY_WAIT_SECONDS":       180,
        "SCAN_WINDOW_SECONDS":      900,
        "TAKE_PROFIT_CENTS":        85,
        "STOP_LOSS_CENTS":          50,
        "SL_OFFSET_CENTS":          12,
        "SL_ALERT_CENTS":           55,
        "TIME_STOP_SECONDS":        120,
        "DAILY_LOSS_LIMIT_CENTS":  -500,
    },
    "og-risky": {
        "MOMENTUM_THRESHOLD_CENTS": 62,
        "MOMENTUM_MIN_ENTRY_CENTS": 62,
        "MOMENTUM_MAX_ENTRY_CENTS": 68,
        "ENTRY_WAIT_SECONDS":        0,
        "SCAN_WINDOW_SECONDS":      900,
        "TAKE_PROFIT_CENTS":        90,
        "STOP_LOSS_CENTS":          45,
        "SL_OFFSET_CENTS":          15,
        "SL_ALERT_CENTS":           52,
        "TIME_STOP_SECONDS":        90,
        "DAILY_LOSS_LIMIT_CENTS":  -800,
        "EXCLUDED_SERIES":          {"KXHYPE15M", "KXBNB15M"},
    },
    "moderate-rr12": {
        "MOMENTUM_THRESHOLD_CENTS": 65,
        "MOMENTUM_MIN_ENTRY_CENTS": 62,
        "MOMENTUM_MAX_ENTRY_CENTS": 72,
        "ENTRY_WAIT_SECONDS":       180,
        "SCAN_WINDOW_SECONDS":      900,
        "TAKE_PROFIT_CENTS":        85,
        "STOP_LOSS_CENTS":          50,
        "SL_OFFSET_CENTS":          12,
        "SL_ALERT_CENTS":           55,
        "TIME_STOP_SECONDS":        120,
        "DAILY_LOSS_LIMIT_CENTS":  -300,
        "EXCLUDED_SERIES":          {"KXHYPE15M", "KXBNB15M"},
    },
    # ------------------------------------------------------------------
    # Bracket profiles — place limit BUY on both YES and NO at open,
    # ride whichever fills to a TP alert, then SELL limit at (alert-3c).
    # No stop loss. Loop within window. Paper only.
    # ------------------------------------------------------------------
    "risky-low": {
        "strategy":                       "bracket",
        "BRACKET_ENTRY_CENTS":            36,
        "BRACKET_TP_ALERT_CENTS":         55,
        "BRACKET_SELL_CENTS":             52,
        "BRACKET_SL_CENTS":               22,   # market sell if mid drops here (-14c max loss)
        "BRACKET_SL_ALERT_CENTS":         28,   # switch to 1s polling when mid drops here
        "BRACKET_WINDOW_START_SECONDS":    0,
        "BRACKET_WINDOW_DURATION_SECONDS": 300,
        "DAILY_LOSS_LIMIT_CENTS":        -500,
    },
    "risky-mid": {
        "strategy":                       "bracket",
        "BRACKET_ENTRY_CENTS":            45,
        "BRACKET_TP_ALERT_CENTS":         64,
        "BRACKET_SELL_CENTS":             61,
        "BRACKET_SL_CENTS":               30,   # market sell if mid drops here (-15c max loss)
        "BRACKET_SL_ALERT_CENTS":         36,   # switch to 1s polling when mid drops here
        "BRACKET_WINDOW_START_SECONDS":    0,
        "BRACKET_WINDOW_DURATION_SECONDS": 300,
        "DAILY_LOSS_LIMIT_CENTS":        -500,
    },
    "risky-high-early": {
        "strategy":                       "bracket",
        "BRACKET_ENTRY_CENTS":            70,
        "BRACKET_TP_ALERT_CENTS":         90,
        "BRACKET_SELL_CENTS":             87,    # fallback; dynamic sell = ask-3c at alert time
        "BRACKET_SL_CENTS":               50,    # SL: active mid monitor, market sell if hit
        "BRACKET_WINDOW_START_SECONDS":    0,
        "BRACKET_WINDOW_DURATION_SECONDS": 300,
        "DAILY_LOSS_LIMIT_CENTS":        -500,
        "EXCLUDED_SERIES":               {"KXHYPE15M", "KXBNB15M"},
    },
    "risky-high-late": {
        "strategy":                       "bracket",
        "BRACKET_ENTRY_CENTS":            70,
        "BRACKET_TP_ALERT_CENTS":         90,
        "BRACKET_SELL_CENTS":             87,    # fallback; dynamic sell = ask-3c at alert time
        "BRACKET_SL_CENTS":               60,    # tightened from 50c (-10c max loss)
        "BRACKET_SL_ALERT_CENTS":         64,    # switch to 1s polling here
        "BRACKET_WINDOW_START_SECONDS":   420,   # start at 7 min from contract open
        "BRACKET_WINDOW_DURATION_SECONDS": 480,  # last 8 min of contract
        "DAILY_LOSS_LIMIT_CENTS":        -500,
        "EXCLUDED_SERIES":               {"KXHYPE15M", "KXBNB15M"},
    },
    # ------------------------------------------------------------------
    # late-sniper — inspired by Telegram community data from competing
    # bot users. Enter only when one side is already at 82-87c (strong
    # conviction), last 4 min of contract. Both YES+NO attempted but
    # only the dominant side will be in the entry band.
    # Entry band: [82c, 87c]. TP at 93c (+6c). SL at 70c (-17c max).
    # BTC-only initially — skip low-volume alts.
    # ------------------------------------------------------------------
    "late-sniper": {
        "strategy":                       "bracket",
        "BRACKET_ENTRY_CENTS":            90,
        "BRACKET_ENTRY_MIN_CENTS":        80,    # entry band: 80-90c
        "BRACKET_TP_ALERT_CENTS":         100,   # no TP — ride to expiry at 100c
        "BRACKET_SELL_CENTS":             100,
        # No SL — data shows SL at any level (80c/50c/30c) was wrong 67-93% of the time.
        # Markets at 80-90c with 4min left almost always resolve correctly even after dips.
        "BRACKET_WINDOW_START_SECONDS":   660,   # start at 11 min in (last 4 min)
        "BRACKET_WINDOW_DURATION_SECONDS": 240,  # 4-min window
        "DAILY_LOSS_LIMIT_CENTS":        -500,
        "EXCLUDED_SERIES":               {"KXHYPE15M", "KXBNB15M"},
    },
    # Paper: wider entry band + mid SL — compare vs live's 30c SL
    "late-sniper-ride": {
        "strategy":                       "bracket",
        "BRACKET_ENTRY_CENTS":            90,
        "BRACKET_ENTRY_MIN_CENTS":        80,    # entry band: 80-90c (was 85-90c)
        "BRACKET_TP_ALERT_CENTS":         100,   # no TP — ride to expiry
        "BRACKET_SELL_CENTS":             100,
        "BRACKET_SL_CENTS":               70,    # paper: tighter than live 30c to compare outcomes
        "BRACKET_SL_ALERT_CENTS":         73,
        "BRACKET_WINDOW_START_SECONDS":   660,
        "BRACKET_WINDOW_DURATION_SECONDS": 240,
        "DAILY_LOSS_LIMIT_CENTS":        -500,
        "EXCLUDED_SERIES":               {"KXHYPE15M", "KXBNB15M"},
    },
}

# Active profile name — set by --profile arg in main()
ACTIVE_PROFILE = "moderate"

# ---------------------------------------------------------------------------
# Circuit breaker — daily loss limit (shared across all worker threads)
# ---------------------------------------------------------------------------

_daily_pnl_lock       = threading.Lock()
_daily_pnl_cents      = 0    # running P&L for today (UTC)
_daily_pnl_date       = ""   # date string "YYYY-MM-DD" — reset when day rolls over
_cb_alerted_date      = ""   # date we last sent a circuit breaker Discord alert
DAILY_LOSS_LIMIT_CENTS = -500  # overridden by profile at startup

def _check_and_reset_daily() -> None:
    """Reset daily P&L counter when UTC date changes."""
    global _daily_pnl_cents, _daily_pnl_date
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _daily_pnl_lock:
        if _daily_pnl_date != today:
            _daily_pnl_cents = 0
            _daily_pnl_date  = today

def circuit_breaker_open() -> bool:
    """Return True if daily loss limit has been hit — no new trades.
    Always returns False in paper mode (circuit breaker is live-only)."""
    if not LIVE_MODE:
        return False
    _check_and_reset_daily()
    with _daily_pnl_lock:
        return _daily_pnl_cents <= DAILY_LOSS_LIMIT_CENTS

def record_daily_pnl(pnl_cents: int) -> None:
    global _daily_pnl_cents
    _check_and_reset_daily()
    with _daily_pnl_lock:
        _daily_pnl_cents += pnl_cents

# ---------------------------------------------------------------------------
# Discord alerts (optional — requires DISCORD_WEBHOOK_URL in .env)
# ---------------------------------------------------------------------------

def _is_edt() -> bool:
    """True if US Eastern Daylight Time is active (2nd Sun Mar – 1st Sun Nov)."""
    now = datetime.now(timezone.utc)
    y = now.year
    # Second Sunday of March
    mar = datetime(y, 3, 8, tzinfo=timezone.utc)
    while mar.weekday() != 6:
        mar += __import__("datetime").timedelta(days=1)
    # First Sunday of November
    nov = datetime(y, 11, 1, tzinfo=timezone.utc)
    while nov.weekday() != 6:
        nov += __import__("datetime").timedelta(days=1)
    return mar <= now < nov

def _et_hour() -> int:
    """Current hour in US Eastern time (handles EDT/EST automatically)."""
    from datetime import timedelta
    offset = timedelta(hours=-4 if _is_edt() else -5)
    return (datetime.now(timezone.utc) + offset).hour

def _send_discord(content: str) -> None:
    url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not url:
        return
    try:
        httpx.post(url, json={"content": content}, timeout=5.0)
    except Exception:
        pass  # never let Discord errors crash the bot

def _mode_tag(series: str) -> str:
    return "🔴 LIVE" if (LIVE_MODE and (not LIVE_SERIES or series in LIVE_SERIES)) else "📄 PAPER"

def tg_trade_entry(profile: str, series: str, side: str, entry: int) -> None:
    _send_discord(
        f"🟡 **ENTRY** | {profile}\n"
        f"Market: {series} | Side: {side.upper()} @ {entry}c\n"
        f"TP: {TAKE_PROFIT_CENTS}c | SL: {STOP_LOSS_CENTS}c | {_mode_tag(series)}"
    )

def tg_trade_exit(profile: str, series: str, side: str, entry: int,
                  exit_c: int, reason: str, pnl: int) -> None:
    icon = "✅" if pnl > 0 else "❌" if pnl < 0 else "➖"
    label = reason.replace("_", " ").title()
    _send_discord(
        f"{icon} **{label}** | {profile}\n"
        f"Market: {series} | Side: {side.upper()} | Entry: {entry}c → Exit: {exit_c}c\n"
        f"P&L: {pnl:+}c (${pnl/100:+.2f}) | {_mode_tag(series)}"
    )

def tg_circuit_breaker(profile: str, daily_pnl: int) -> None:
    _send_discord(
        f"🚨 **CIRCUIT BREAKER** | {profile}\n"
        f"Daily loss limit hit: {daily_pnl:+}c (${daily_pnl/100:+.2f})\n"
        f"No new trades until midnight UTC."
    )

def tg_daily_summary(profile: str, trades: int, wins: int, pnl: int) -> None:
    """Called by the scheduled summary thread — not the 15min stats reporter."""
    wp = wins / trades * 100 if trades else 0
    icon = "📈" if pnl > 0 else "📉"
    from datetime import timedelta
    offset = timedelta(hours=-4 if _is_edt() else -5)
    et_time = (datetime.now(timezone.utc) + offset).strftime("%I:%M %p ET")
    _send_discord(
        f"{icon} **Summary** | {profile} | {et_time}\n"
        f"Trades: {trades} | Wins: {wins} | Win%: {wp:.1f}%\n"
        f"Session P&L: {pnl:+}c (${pnl/100:+.2f})"
    )

DAILY_LOSS_LIMIT_CENTS   = -500  # overridden per profile at startup

# Live trading — use --live flag to enable real orders
LIVE_MODE            = False   # overridden at startup from --live flag
LIVE_SERIES: set     = set()   # empty = all series live; populated from --live-series
LIMIT_ORDER_TIMEOUT  = 45      # seconds to wait for limit fill before fallback

# Per-series momentum threshold overrides (added on top of profile threshold)
# XRP and ETH historically weaker signals — require higher conviction to enter
SERIES_THRESHOLD_OVERRIDE = {
    "KXXRP15M": 70,
    "KXETH15M": 67,
}

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

KALSHI_BASE_URL  = "https://api.elections.kalshi.com/trade-api/v2"
DEFAULT_KEY_PATH = "/opt/kalshi-bot/kalshi_private_key.pem"
DB_PATH          = "./data/bot.db"
LOG_PATH         = "./logs/multi_scalper.log"

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_dotenv(path: str = ".env") -> None:
    p = Path(path)
    if not p.exists():
        return
    for raw in p.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k and k not in os.environ:
            os.environ[k] = v

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(series)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    logger = logging.getLogger("multi_scalper")
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    logger.propagate = False
    return logger


_root_log: logging.Logger


def get_log(series: str) -> logging.LoggerAdapter:
    return logging.LoggerAdapter(_root_log, {"series": series})

# ---------------------------------------------------------------------------
# Database (shared, thread-safe via WAL)
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bracket_trade_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at    TEXT    NOT NULL DEFAULT (datetime('now','utc')),
    kalshi_ticker TEXT    NOT NULL,
    series        TEXT    NOT NULL,
    profile       TEXT    NOT NULL DEFAULT 'moderate',
    mode          TEXT    NOT NULL,   -- 'bracket' or 'directional'
    side          TEXT    NOT NULL,
    contracts     INTEGER NOT NULL,
    entry_cents   INTEGER NOT NULL,
    exit_cents    INTEGER,
    exit_reason   TEXT,
    pnl_cents     INTEGER,
    pnl_usd       REAL
);
"""

_db_lock = threading.Lock()


def init_db(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(CREATE_TABLE_SQL)
    # Migrate: add series column if missing (old bracket_scalper.py schema)
    cols = {row[1] for row in conn.execute("PRAGMA table_info(bracket_trade_log)")}
    if "series" not in cols:
        conn.execute("ALTER TABLE bracket_trade_log ADD COLUMN series TEXT NOT NULL DEFAULT ''")
    if "profile" not in cols:
        conn.execute("ALTER TABLE bracket_trade_log ADD COLUMN profile TEXT NOT NULL DEFAULT 'moderate'")
    conn.commit()
    return conn


def log_trade(
    conn: sqlite3.Connection,
    ticker: str,
    series: str,
    profile: str,
    mode: str,
    side: str,
    entry_cents: int,
    exit_cents: int,
    exit_reason: str,
    pnl_cents: int,
) -> None:
    with _db_lock:
        conn.execute(
            """INSERT INTO bracket_trade_log
               (kalshi_ticker,series,profile,mode,side,contracts,entry_cents,
                exit_cents,exit_reason,pnl_cents,pnl_usd)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (ticker, series, profile, mode, side, CONTRACTS, entry_cents,
             exit_cents, exit_reason, pnl_cents, pnl_cents / 100.0),
        )
        conn.commit()

# ---------------------------------------------------------------------------
# Kalshi Auth
# ---------------------------------------------------------------------------

class KalshiAuth:
    def __init__(self, api_key: str, key_path: str) -> None:
        self._key_id = api_key
        with open(key_path, "rb") as f:
            self._key = serialization.load_pem_private_key(f.read(), password=None)

    def headers(self, method: str, path: str) -> dict:
        ts = str(int(time.time() * 1000))
        msg = (ts + method.upper() + path).encode()
        sig = self._key.sign(
            msg,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }

# ---------------------------------------------------------------------------
# WebSocket ticker feed (live mode only)
# ---------------------------------------------------------------------------

class KalshiTickerWS:
    """Real-time price cache via Kalshi WebSocket ticker channel.
    Replaces REST orderbook polling in live mode — zero API calls for price data.
    """
    WS_URL  = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    WS_PATH = "/trade-api/ws/v2"

    def __init__(self, auth: "KalshiAuth") -> None:
        self._auth       = auth
        self._prices:    dict[str, dict[str, int]] = {}
        self._lock       = threading.RLock()
        self._ready      = threading.Event()
        self._ws         = None
        self._subscribed: set[str] = set()
        self._cmd_id     = 1

    def start(self) -> None:
        t = threading.Thread(target=self._run, daemon=True, name="kalshi-ws")
        t.start()
        if not self._ready.wait(timeout=15):
            _ws_log().warning("WebSocket did not connect within 15s — will fall back to REST polling")

    def subscribe(self, tickers: list[str]) -> None:
        with self._lock:
            new = [t for t in tickers if t not in self._subscribed]
            if not new:
                return
            self._subscribed.update(new)
            if self._ws:
                self._send({"id": self._cmd_id, "cmd": "subscribe",
                            "params": {"channels": ["ticker"], "market_tickers": new}})
                self._cmd_id += 1

    def get_prices(self, ticker: str) -> tuple[int, int]:
        with self._lock:
            p = self._prices.get(ticker, {})
            return p.get("yes_ask", 0), p.get("no_ask", 0)

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def _send(self, msg: dict) -> None:
        try:
            if self._ws:
                self._ws.send(json.dumps(msg))
        except Exception as e:
            _ws_log().debug(f"WS send error: {e}")

    def _on_open(self, ws) -> None:
        self._ws = ws
        with self._lock:
            if self._subscribed:
                self._send({"id": self._cmd_id, "cmd": "subscribe",
                            "params": {"channels": ["ticker"], "market_tickers": list(self._subscribed)}})
                self._cmd_id += 1
        self._ready.set()
        _ws_log().info("WebSocket connected and ready")

    def _on_message(self, ws, raw: str) -> None:
        try:
            data = json.loads(raw)
            if data.get("type") not in ("ticker",):
                return
            msg    = data.get("msg", {})
            ticker = msg.get("market_ticker")
            if not ticker:
                return
            with self._lock:
                p = self._prices.setdefault(ticker, {})
                for field in ("yes_ask", "no_ask", "yes_bid", "no_bid"):
                    v = msg.get(field)
                    if v is not None:
                        p[field] = int(v)
        except Exception as e:
            _ws_log().debug(f"WS message parse error: {e}")

    def _on_error(self, ws, error) -> None:
        _ws_log().warning(f"WebSocket error: {error}")

    def _on_close(self, ws, code, reason) -> None:
        _ws_log().warning(f"WebSocket closed: {code}")
        self._ready.clear()
        self._ws = None

    def _run(self) -> None:
        try:
            import websocket as ws_lib
        except ImportError:
            _ws_log().error("websocket-client not installed — run: pip install websocket-client")
            return

        while True:
            # Regenerate auth headers on every connect attempt — Kalshi timestamps expire
            headers = self._auth.headers("GET", self.WS_PATH)
            ws_headers = {k: v for k, v in headers.items() if k != "Content-Type"}
            try:
                wsa = ws_lib.WebSocketApp(
                    self.WS_URL,
                    header=ws_headers,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                wsa.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                _ws_log().warning(f"WebSocket run error: {e}")
            time.sleep(5)


def _ws_log() -> logging.LoggerAdapter:
    return logging.LoggerAdapter(logging.getLogger("multi_scalper"), {"series": "WS"})


# Global WebSocket instance — initialized in main() for live mode only
_ticker_ws: Optional[KalshiTickerWS] = None

# ---------------------------------------------------------------------------
# Process-level API rate limiter — max 8 calls/second across all threads
# ---------------------------------------------------------------------------

_api_rate_lock    = threading.Lock()
_api_last_call_ts = 0.0
_API_MIN_INTERVAL = 1.0 / 8  # 125ms between calls = 8/s max

def _api_throttle() -> None:
    """Block until it is safe to make another API call."""
    global _api_last_call_ts
    with _api_rate_lock:
        now  = time.time()
        wait = _api_last_call_ts + _API_MIN_INTERVAL - now
        if wait > 0:
            time.sleep(wait)
        _api_last_call_ts = time.time()

# ---------------------------------------------------------------------------
# Kalshi HTTP client
# ---------------------------------------------------------------------------

@dataclass
class OrderBook:
    ticker: str
    yes_bid: int
    yes_ask: int
    no_bid: int
    no_ask: int
    expiry_ts: float

    @property
    def mid_yes(self) -> int:
        return round((self.yes_bid + self.yes_ask) / 2)

    @property
    def seconds_to_expiry(self) -> float:
        return max(0.0, self.expiry_ts - time.time())


def _parse_iso(s: str) -> float:
    if not s:
        return 0.0
    try:
        return datetime.fromisoformat(s.rstrip("Z").split("+")[0]).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0.0


class KalshiClient:
    def __init__(self, auth: KalshiAuth) -> None:
        self._auth = auth
        self._http = httpx.Client(timeout=10.0)

    def get_open_markets(self, series_ticker: str, limit: int = 5) -> list[dict]:
        path = "/trade-api/v2/markets"
        r = self._http.get(
            KALSHI_BASE_URL + "/markets",
            headers=self._auth.headers("GET", path),
            params={"series_ticker": series_ticker, "status": "open", "limit": limit},
        )
        r.raise_for_status()
        return r.json().get("markets", [])

    def get_orderbook(self, ticker: str, expiry_ts: float) -> OrderBook:
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        r = self._http.get(
            KALSHI_BASE_URL + f"/markets/{ticker}/orderbook",
            headers=self._auth.headers("GET", path),
            params={"depth": 1},
        )
        r.raise_for_status()
        ob = r.json().get("orderbook_fp", {})
        yes_bids = ob.get("yes_dollars", [])
        no_bids  = ob.get("no_dollars", [])

        yes_bid = int(float(yes_bids[0][0]) * 100) if yes_bids else 0
        no_bid  = int(float(no_bids[0][0]) * 100)  if no_bids  else 0
        yes_ask = 100 - no_bid
        no_ask  = 100 - yes_bid

        return OrderBook(
            ticker=ticker,
            yes_bid=yes_bid, yes_ask=yes_ask,
            no_bid=no_bid,   no_ask=no_ask,
            expiry_ts=expiry_ts,
        )

    def get_market_result(self, ticker: str) -> Optional[str]:
        """Return 'yes', 'no', or None if not yet finalized."""
        try:
            path = f"/trade-api/v2/markets/{ticker}"
            r = self._http.get(
                KALSHI_BASE_URL + f"/markets/{ticker}",
                headers=self._auth.headers("GET", path),
            )
            r.raise_for_status()
            m = r.json().get("market", {})
            if m.get("status") == "finalized":
                return m.get("result")  # "yes" or "no"
        except Exception:
            pass
        return None

    def get_balance(self) -> int:
        """Return available balance in cents."""
        path = "/trade-api/v2/portfolio/balance"
        r = self._http.get(
            KALSHI_BASE_URL + "/portfolio/balance",
            headers=self._auth.headers("GET", path),
        )
        r.raise_for_status()
        return int(r.json().get("balance", 0))

    def place_order(
        self,
        ticker: str,
        side: str,
        order_type: str,
        price_cents: Optional[int],
        action: str = "buy",
    ) -> Optional[str]:
        """Place a limit or market order. Returns order_id or None on failure."""
        path = "/trade-api/v2/portfolio/orders"
        body: dict = {
            "ticker": ticker,
            "action": action,
            "side": side,
            "type": order_type,
            "count": CONTRACTS,
        }
        if price_cents is not None:
            # Kalshi treats yes_price as the canonical price field.
            # For NO orders, send yes_price = 100 - no_price so the limit
            # is correctly interpreted (sending no_price causes taker fills).
            # Market sells also require a price field -- send yes_price=1 to
            # guarantee fill at best available bid.
            if side == "yes":
                body["yes_price"] = price_cents
            else:
                body["yes_price"] = 100 - price_cents
        r = self._http.post(
            KALSHI_BASE_URL + "/portfolio/orders",
            headers=self._auth.headers("POST", path),
            json=body,
        )
        if not r.is_success:
            raise Exception(f"place_order {r.status_code}: {r.text[:500]}  body={body}")
        return r.json().get("order", {}).get("order_id")

    def get_order_status(self, order_id: str, side: str = "yes") -> tuple[str, int]:
        """Return (status, filled_price_cents). Status: resting/filled/canceled.
        side must match the order side so the correct price field is read."""
        path = f"/trade-api/v2/portfolio/orders/{order_id}"
        r = self._http.get(
            KALSHI_BASE_URL + f"/portfolio/orders/{order_id}",
            headers=self._auth.headers("GET", path),
        )
        r.raise_for_status()
        order = r.json().get("order", {})
        raw_status = order.get("status", "error")
        # Kalshi returns "executed" for filled orders
        status = "filled" if raw_status == "executed" else raw_status
        # Read the price field matching the order side
        if side == "yes":
            filled_price = order.get("yes_price_dollars") or 0
        else:
            filled_price = order.get("no_price_dollars") or 0
        return status, int(round(float(filled_price) * 100))

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a resting order. Returns True if successful."""
        path = f"/trade-api/v2/portfolio/orders/{order_id}"
        try:
            r = self._http.delete(
                KALSHI_BASE_URL + f"/portfolio/orders/{order_id}",
                headers=self._auth.headers("DELETE", path),
            )
            return r.status_code in (200, 204)
        except Exception:
            return False

    def close(self) -> None:
        self._http.close()

# ---------------------------------------------------------------------------
# Strategy helpers
# ---------------------------------------------------------------------------

def find_next_contract(client: KalshiClient, series: str) -> Optional[tuple[str, float, float]]:
    """Return (ticker, expiry_ts, volume_fp) for the soonest open contract with >90s remaining."""
    try:
        markets = client.get_open_markets(series, limit=5)
    except Exception:
        return None

    now = time.time()
    candidates = []
    for m in markets:
        exp = _parse_iso(m.get("close_time") or m.get("expiration_time") or "")
        tte = exp - now
        if tte > 90:
            vol = float(m.get("volume_fp", 0) or 0)
            candidates.append((tte, m["ticker"], exp, vol))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    _, ticker, exp, vol = candidates[0]
    return ticker, exp, vol


def next_quarter_hour_ts() -> float:
    quarter = 15 * 60
    return math.ceil(time.time() / quarter) * quarter


def sleep_until_next_contract(log: logging.LoggerAdapter, buffer_s: float = 1.5) -> None:
    target = next_quarter_hour_ts() - buffer_s
    wait = target - time.time()
    if wait > 5:
        log.info(f"Sleeping {wait:.0f}s until next contract open (~{wait/60:.1f}min)")
        time.sleep(wait)


def current_mid(ob: OrderBook, side: str) -> int:
    return ob.mid_yes if side == "yes" else (100 - ob.mid_yes)

# ---------------------------------------------------------------------------
# Core bracket cycle (one contract)
# ---------------------------------------------------------------------------

def run_cycle(
    client: KalshiClient,
    conn: sqlite3.Connection,
    log: logging.LoggerAdapter,
    series: str,
    profile: str,
    ticker: str,
    expiry_ts: float,
    skip_wait: bool = False,
    scan_end_ts: Optional[float] = None,
) -> str:
    """Momentum-follow cycle: wait, enter on strong directional signal.
    Returns:
        "traded"   — trade was opened and closed (SL, TP, or expiry)
        "no_entry" — no trade made (no signal, window expired, circuit breaker)
        "retry"    — transient error, caller should retry immediately
    """

    # Circuit breaker — skip new trades if daily loss limit hit
    if circuit_breaker_open():
        log.info(f"[{ticker}] Circuit breaker active — skipping (daily loss limit hit)")
        return "no_entry"

    tte = expiry_ts - time.time()
    min_required = (0 if skip_wait else ENTRY_WAIT_SECONDS) + 60 + TIME_STOP_SECONDS + 30
    if tte < min_required:
        log.info(f"[{ticker}] Not enough time (tte={tte:.0f}s < {min_required}s) — skipping")
        return "no_entry"

    # ---- Phase 1: wait for direction to establish ----
    if not skip_wait:
        log.info(f"[{ticker}] MOMENTUM mode — waiting {ENTRY_WAIT_SECONDS}s for direction...")
        time.sleep(ENTRY_WAIT_SECONDS)
    else:
        log.info(f"[{ticker}] MOMENTUM mode — re-scanning (wait already done)")

    threshold = max(MOMENTUM_THRESHOLD_CENTS, SERIES_THRESHOLD_OVERRIDE.get(series, 0))
    series_is_live = LIVE_MODE and (not LIVE_SERIES or series in LIVE_SERIES)

    # Note: duplicate entry prevention is handled by series_worker's synchronous
    # 3-window flow. run_cycle no longer needs to check for prior trades.

    tte = expiry_ts - time.time()
    if tte <= TIME_STOP_SECONDS + 30:
        log.info(f"[{ticker}] Too late to enter after wait (tte={tte:.0f}s) — skipping")
        return "no_entry"

    filled_side:  Optional[str] = None
    entry_cents:  Optional[int] = None
    signal_ask:   int           = 0

    # ---- Phase 2: scan for momentum signal ----
    # Early-warning: when bid crosses (threshold - 8c), switch to 1s polling so
    # we catch fast-moving markets before the ask blows past max_entry_cents.
    # We do NOT pre-place orders at that point — the ask is still well below
    # threshold and a limit buy at max_entry would fill immediately at the low ask.
    EARLY_WARNING_OFFSET = 8   # cents below threshold to trigger fast polling
    in_early_warning = False

    scan_deadline = scan_end_ts if scan_end_ts is not None else (time.time() + SCAN_WINDOW_SECONDS)
    while time.time() < scan_deadline:
        poll_interval = 1 if in_early_warning else ORDER_POLL_INTERVAL
        time.sleep(poll_interval)
        tte = expiry_ts - time.time()
        if tte <= TIME_STOP_SECONDS + 30:
            log.info(f"[{ticker}] Too late to enter (tte={tte:.0f}s) — no trade")
            return "no_entry"
        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error: {e}")
            continue

        # Signal check: ask >= threshold means the crowd is paying threshold cents for that side.
        # At 50/50 market, ask ~51c. As momentum builds, ask rises (63c, 70c, etc).
        # Trigger when ask reaches threshold — enter at ask-1c (maker, no fee).
        yes_ask = ob.yes_ask or 0
        no_ask  = ob.no_ask  or 0
        log.info(f"[{ticker}] scan: yes_ask={yes_ask} no_ask={no_ask} threshold={threshold} tte={tte:.0f}s early={in_early_warning}")
        for side, ask_val in [("yes", yes_ask), ("no", no_ask)]:
            if ask_val == 0:
                continue
            if ask_val >= threshold:
                if ask_val < MOMENTUM_MIN_ENTRY_CENTS:
                    log.debug(f"[{ticker}] {side.upper()} signal but ask={ask_val}c < min {MOMENTUM_MIN_ENTRY_CENTS}c — skipping")
                    continue
                if ask_val > MOMENTUM_MAX_ENTRY_CENTS:
                    log.debug(f"[{ticker}] {side.upper()} signal but ask={ask_val}c > max {MOMENTUM_MAX_ENTRY_CENTS}c — skipping")
                    continue
                filled_side = side
                signal_ask  = ask_val
                break
        if filled_side:
            break

        # Early warning: ask crossed (threshold - 8c) rising — switch to 1s polling
        warning_ask = threshold - EARLY_WARNING_OFFSET
        if not in_early_warning and (yes_ask >= warning_ask or no_ask >= warning_ask):
            in_early_warning = True
            log.info(f"[{ticker}] EARLY WARNING: ask={max(yes_ask, no_ask)}c >= {warning_ask}c — switching to 1s polling")
        elif in_early_warning and yes_ask < warning_ask - 3 and no_ask < warning_ask - 3:
            in_early_warning = False
            log.info(f"[{ticker}] Ask retreated — back to {ORDER_POLL_INTERVAL}s polling")
    else:
        log.info(f"[{ticker}] No momentum signal in scan window — no entry")
        return "no_entry"

    # ---- Entry execution: limit order with market fallback ----
    limit_price = signal_ask - 1

    if not series_is_live:
        entry_cents = limit_price
        log.info(f"[{ticker}] PAPER ENTRY: {filled_side.upper()} @ {entry_cents}c (ask was {signal_ask}c)")
    else:
        try:
            balance = client.get_balance()
            if balance < limit_price * CONTRACTS:
                log.warning(f"[{ticker}] Insufficient balance: {balance}c — skipping")
                return "no_entry"
        except Exception as e:
            log.warning(f"[{ticker}] Balance check failed: {e} — skipping")
            return "retry"

        order_id = client.place_order(ticker, filled_side, "limit", limit_price)
        if not order_id:
            log.warning(f"[{ticker}] Limit order placement failed — retrying")
            return "retry"
        log.info(f"[{ticker}] LIMIT ORDER: {filled_side.upper()} @ {limit_price}c  id={order_id}")

        fill_deadline = time.time() + LIMIT_ORDER_TIMEOUT
        while time.time() < fill_deadline:
            time.sleep(3)
            try:
                status, filled_price = client.get_order_status(order_id, filled_side)
            except Exception:
                continue
            if status == "filled":
                entry_cents = filled_price
                log.info(f"[{ticker}] Limit FILLED @ {entry_cents}c")
                break
            elif status in ("canceled", "error"):
                log.warning(f"[{ticker}] Limit order {status} unexpectedly — retrying")
                return "retry"

            # Cancel immediately if ask has blown past max entry — no point waiting
            try:
                ob_check = client.get_orderbook(ticker, expiry_ts)
                current_ask = ob_check.yes_ask if filled_side == "yes" else ob_check.no_ask
                if current_ask > MOMENTUM_MAX_ENTRY_CENTS:
                    client.cancel_order(order_id)
                    log.info(f"[{ticker}] Ask={current_ask}c > max {MOMENTUM_MAX_ENTRY_CENTS}c — cancelling limit order, moving to next window")
                    return "no_entry"
            except Exception:
                pass

        if entry_cents is None:
            client.cancel_order(order_id)
            log.info(f"[{ticker}] Limit not filled in {LIMIT_ORDER_TIMEOUT}s — cancelling, skip to next contract")
            return "no_entry"

    # ---- Phase 3: manage position ----
    if series_is_live:
        tg_trade_entry(profile, series, filled_side, entry_cents)
    log.info(
        f"[{ticker}] Position open: LONG {filled_side.upper()} @ {entry_cents}c  "
        f"TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c(mid)"
    )

    exit_reason: Optional[str] = None
    exit_cents:  Optional[int] = None
    dynamic_sl  = entry_cents - SL_OFFSET_CENTS  # SL = entry - 12c
    time_stop_active = False  # True during final TIME_STOP_SECONDS — SL disabled, ride to expiry
    tp_order_id: Optional[str] = None  # resting SELL limit at TP price

    def _market_sell_live() -> Optional[int]:
        """Place a market SELL (taker) and return actual fill price, or None on failure."""
        try:
            sell_id = client.place_order(ticker, filled_side, "market", None, action="sell")
            if not sell_id:
                return None
            for _ in range(15):
                time.sleep(2)
                status, actual_exit = client.get_order_status(sell_id, filled_side)
                if status == "filled":
                    return actual_exit
        except Exception as e:
            log.warning(f"[{ticker}] Market sell error: {e}")
        return None

    # Place resting TP limit SELL immediately — sits as maker, fills when price gets there.
    # Price is well below TP at entry so this will never fill immediately.
    if series_is_live:
        try:
            tp_order_id = client.place_order(ticker, filled_side, "limit", TAKE_PROFIT_CENTS, action="sell")
            if tp_order_id:
                log.info(f"[{ticker}] Resting TP limit SELL @ {TAKE_PROFIT_CENTS}c  id={tp_order_id}")
            else:
                log.warning(f"[{ticker}] Failed to place resting TP order")
        except Exception as e:
            log.warning(f"[{ticker}] TP order error: {e}")

    while True:
        # Check if resting TP order filled
        if series_is_live and tp_order_id:
            try:
                tp_status, tp_filled_price = client.get_order_status(tp_order_id, filled_side)
                if tp_status == "filled":
                    exit_reason = "take_profit"
                    exit_cents  = tp_filled_price
                    tp_order_id = None
                    log.info(f"[{ticker}] TP limit FILLED @ {exit_cents}c")
                    break
            except Exception:
                pass

        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error: {e}")
            time.sleep(ORDER_POLL_INTERVAL)
            continue

        tte = expiry_ts - time.time()
        mid = current_mid(ob, filled_side)
        bid = ob.yes_bid if filled_side == "yes" else ob.no_bid

        log.debug(f"[{ticker}] pos: side={filled_side} mid={mid}c bid={bid}c sl={dynamic_sl}c tte={tte:.0f}s")

        if tte <= TIME_STOP_SECONDS and not time_stop_active:
            # Let it ride to expiry — binary resolves at 0 or 100, no middle ground.
            # Disable SL so we don't exit in the final seconds when the market is thin.
            time_stop_active = True
            log.info(f"[{ticker}] TIME STOP  tte={tte:.0f}s — SL disabled, letting contract expire")

        # Paper TP check
        if not series_is_live and bid >= TAKE_PROFIT_CENTS:
            exit_reason = "take_profit"
            exit_cents  = bid
            log.info(f"[{ticker}] PAPER TP  bid={bid}c")
            break

        # SL: mid dropped — cancel TP order then market sell
        if mid <= dynamic_sl and not time_stop_active:
            exit_reason = "stop_loss"
            if series_is_live:
                if tp_order_id:
                    client.cancel_order(tp_order_id)
                    tp_order_id = None
                actual = _market_sell_live()
                exit_cents = actual if actual is not None else mid
                log.info(f"[{ticker}] SL hit mid={mid}c — market sold @ {exit_cents}c")
            else:
                exit_cents = dynamic_sl
                log.info(f"[{ticker}] PAPER SL  mid={mid}c  exit@{exit_cents}c")
            break

        if tte <= 0:
            exit_reason = "expired"
            if series_is_live and tp_order_id:
                client.cancel_order(tp_order_id)
                tp_order_id = None
            # Poll for actual Kalshi settlement (may take a few seconds after :00/:15/:30/:45)
            result = None
            for _ in range(15):
                result = client.get_market_result(ticker)
                if result is not None:
                    break
                time.sleep(2)
            if result is not None:
                exit_cents = 100 if result == filled_side else 0
            else:
                exit_cents = mid  # fallback if settlement delayed beyond 30s
            log.info(f"[{ticker}] EXPIRED  result={result}  exit@{exit_cents}c")
            break

        time.sleep(SL_POLL_INTERVAL if mid <= SL_ALERT_CENTS else ORDER_POLL_INTERVAL)

    if exit_cents is None:
        exit_cents = dynamic_sl  # fallback estimate

    # ---- Record ----
    assert exit_reason and exit_cents is not None and entry_cents is not None
    pnl_cents = exit_cents - entry_cents
    pnl_usd   = pnl_cents / 100.0 * CONTRACTS
    sign = "+" if pnl_cents >= 0 else ""

    log_trade(
        conn, ticker, series, profile, "momentum", filled_side,
        entry_cents, exit_cents, exit_reason, pnl_cents * CONTRACTS,
    )

    # Update circuit breaker counter and send Telegram alert
    record_daily_pnl(pnl_cents * CONTRACTS)
    if series_is_live:
        tg_trade_exit(profile, series, filled_side, entry_cents, exit_cents, exit_reason, pnl_cents)
    with _daily_pnl_lock:
        daily_total = _daily_pnl_cents
    if circuit_breaker_open():
        global _cb_alerted_date
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        with _daily_pnl_lock:
            already_alerted = (_cb_alerted_date == today)
            if not already_alerted:
                _cb_alerted_date = today
        if not already_alerted:
            tg_circuit_breaker(profile, daily_total)
            log.warning(f"CIRCUIT BREAKER: daily loss limit hit ({daily_total}c) — pausing new trades")

    log.info(
        f"[{ticker}] CLOSED  side={filled_side}  entry={entry_cents}c  "
        f"exit={exit_cents}c  reason={exit_reason}  "
        f"PnL={sign}{pnl_cents}c / {sign}${pnl_usd:.4f}"
    )
    return "traded"

# ---------------------------------------------------------------------------
# Bracket strategy (risky-low / risky-mid / risky-high-early / risky-high-late)
# ---------------------------------------------------------------------------

# Derived from PROFILES — any profile with strategy="bracket" is a bracket profile.
# Never needs manual updating when new bracket profiles are added.
BRACKET_PROFILE_NAMES = {
    name for name, cfg in PROFILES.items() if cfg.get("strategy") == "bracket"
}


def run_bracket_cycle(
    client: KalshiClient,
    conn: sqlite3.Connection,
    log: logging.LoggerAdapter,
    series: str,
    profile: str,
    ticker: str,
    expiry_ts: float,
) -> None:
    """
    Bracket strategy for one contract window:
      1. Wait until window opens.
      2. Poll orderbook until YES ask OR NO ask falls to BRACKET_ENTRY_CENTS.
      3. Whichever side fills first — the other is implicitly cancelled (paper).
      4. Monitor bid for filled side until it hits BRACKET_TP_ALERT_CENTS.
      5. Place SELL limit at BRACKET_SELL_CENTS (fills immediately in paper since
         bid >= alert >= sell price at that moment).
      6. Loop back to step 2 if still within window.
      7. After window closes, let last open trade ride to natural expiry.
    """
    if circuit_breaker_open():
        log.info(f"[{ticker}] Circuit breaker active — skipping bracket cycle")
        return

    cfg            = PROFILES[profile]
    entry_c        = cfg["BRACKET_ENTRY_CENTS"]
    entry_min_c    = cfg.get("BRACKET_ENTRY_MIN_CENTS", entry_c - 5)  # lower bound of entry band
    tp_alert_c     = cfg["BRACKET_TP_ALERT_CENTS"]
    sell_c         = cfg["BRACKET_SELL_CENTS"]
    sl_c           = cfg.get("BRACKET_SL_CENTS", None)       # None = no SL
    sl_alert_c     = cfg.get("BRACKET_SL_ALERT_CENTS", None) # fast-poll trigger
    win_start_off  = cfg["BRACKET_WINDOW_START_SECONDS"]
    win_duration   = cfg["BRACKET_WINDOW_DURATION_SECONDS"]

    # Each 15-min contract is 900 seconds
    contract_open_ts = expiry_ts - 900
    window_start_ts  = contract_open_ts + win_start_off
    window_end_ts    = window_start_ts + win_duration

    # Wait until window opens (relevant for risky-high-late)
    now = time.time()
    if now < window_start_ts:
        wait_s = window_start_ts - now
        log.info(f"[{ticker}] Bracket window opens in {wait_s:.0f}s — waiting...")
        time.sleep(wait_s)

    if time.time() >= window_end_ts:
        log.info(f"[{ticker}] Bracket window already closed — skipping")
        return

    log.info(
        f"[{ticker}] BRACKET START  entry=[{entry_min_c},{entry_c}]c  "
        f"tp_alert={tp_alert_c}c  sell={sell_c}c  "
        f"window={win_duration}s  profile={profile}"
    )

    round_num = 0
    while time.time() < window_end_ts:
        if circuit_breaker_open():
            log.info(f"[{ticker}] Circuit breaker hit — stopping bracket rounds")
            break

        round_num += 1
        log.info(f"[{ticker}] Bracket round {round_num} — waiting for {entry_c}c fill")

        # ---- Phase 1: enter position ----
        filled_side: Optional[str] = None
        entry_filled: int           = 0

        series_is_live = LIVE_MODE and (not LIVE_SERIES or series in LIVE_SERIES)

        if series_is_live:
            # Live: watch price until one side's ask reaches entry_c,
            # THEN place a limit BUY on that side only (prevents both sides
            # filling immediately as takers when market is undecided).
            # Uses WebSocket price feed if available, otherwise falls back to REST polling.
            yes_oid: Optional[str] = None
            no_oid:  Optional[str] = None

            use_ws = _ticker_ws is not None and _ticker_ws.is_ready()
            if use_ws:
                _ticker_ws.subscribe([ticker])
                log.debug(f"[{ticker}] Using WebSocket feed for entry scan")

            _ws_zero_streak = 0  # consecutive 0,0 reads before falling back to REST

            while time.time() < window_end_ts and not filled_side:
                if use_ws:
                    yes_ask, no_ask = _ticker_ws.get_prices(ticker)
                    if yes_ask == 0 and no_ask == 0:
                        _ws_zero_streak += 1
                        if _ws_zero_streak >= 40:  # ~2s of 0,0 — WS has no data for this ticker
                            log.warning(f"[{ticker}] WebSocket no data after 2s — falling back to REST")
                            use_ws = False
                        else:
                            time.sleep(0.05)
                        continue
                    _ws_zero_streak = 0
                    log.debug(f"[{ticker}] live scan (WS): yes_ask={yes_ask}c no_ask={no_ask}c target<={entry_c}c")
                else:
                    try:
                        ob = client.get_orderbook(ticker, expiry_ts)
                    except Exception as e:
                        log.warning(f"[{ticker}] Orderbook error in entry scan: {e}")
                        time.sleep(1)
                        continue
                    yes_ask = ob.yes_ask or 0
                    no_ask  = ob.no_ask  or 0
                    log.debug(f"[{ticker}] live scan (REST): yes_ask={yes_ask}c no_ask={no_ask}c target<={entry_c}c")

                # Place order when ask is within the entry band
                if yes_ask > 0 and entry_min_c <= yes_ask <= entry_c and not yes_oid:
                    yes_oid = client.place_order(ticker, "yes", "limit", entry_c)
                    log.info(f"[{ticker}] YES ask={yes_ask}c in band [{entry_min_c},{entry_c}] — placed BUY limit id={yes_oid}")
                if no_ask > 0 and entry_min_c <= no_ask <= entry_c and not no_oid:
                    no_oid = client.place_order(ticker, "no", "limit", entry_c)
                    log.info(f"[{ticker}] NO ask={no_ask}c in band [{entry_min_c},{entry_c}] — placed BUY limit id={no_oid}")

                # Check fill status of any placed orders
                for side, oid in [("yes", yes_oid), ("no", no_oid)]:
                    if not oid:
                        continue
                    try:
                        status, fill_p = client.get_order_status(oid, side)
                    except Exception:
                        continue
                    if status == "filled":
                        filled_side  = side
                        entry_filled = fill_p
                        other_oid = no_oid if side == "yes" else yes_oid
                        if other_oid:
                            client.cancel_order(other_oid)
                        break

                if not filled_side:
                    time.sleep(0.05 if use_ws else ORDER_POLL_INTERVAL)

            # Window closed with no fill — cancel any open orders
            if not filled_side:
                for oid in [yes_oid, no_oid]:
                    if oid:
                        client.cancel_order(oid)
                log.info(f"[{ticker}] Bracket r{round_num}: window closed with no fill — done")
                break

        else:
            # Paper: simulate both resting limits — whichever ask drops to entry_c fills first
            log.info(f"[{ticker}] PAPER: resting BUY limits YES@{entry_c}c and NO@{entry_c}c")
            while time.time() < window_end_ts:
                try:
                    ob = client.get_orderbook(ticker, expiry_ts)
                except Exception as e:
                    log.warning(f"[{ticker}] Orderbook error: {e}")
                    time.sleep(2)
                    continue

                yes_ask = ob.yes_ask or 0
                no_ask  = ob.no_ask  or 0
                log.debug(f"[{ticker}] bracket scan r{round_num}: yes_ask={yes_ask}c no_ask={no_ask}c target<={entry_c}c")

                # Fill when ask is within the entry band
                if yes_ask > 0 and entry_min_c <= yes_ask <= entry_c:
                    filled_side  = "yes"
                    entry_filled = yes_ask
                    break
                if no_ask > 0 and entry_min_c <= no_ask <= entry_c:
                    filled_side  = "no"
                    entry_filled = no_ask
                    break

                time.sleep(ORDER_POLL_INTERVAL)

            if not filled_side:
                log.info(f"[{ticker}] Bracket r{round_num}: window closed with no fill — done")
                break

        log.info(f"[{ticker}] Bracket FILL: {filled_side.upper()} @ {entry_filled}c (limit was {entry_c}c)")
        if sl_c is not None:
            log.info(f"[{ticker}] SL active @ {sl_c}c — monitoring mid, will market sell if hit")

        # Place resting TP limit SELL at BRACKET_SELL_CENTS immediately after entry.
        # Sits as a maker order in the book — fills when bid reaches that price.
        tp_order_id: Optional[str] = None
        if series_is_live:
            try:
                tp_order_id = client.place_order(ticker, filled_side, "limit", sell_c, action="sell")
                if tp_order_id:
                    log.info(f"[{ticker}] Resting TP limit SELL @ {sell_c}c  id={tp_order_id}")
                else:
                    log.warning(f"[{ticker}] Failed to place resting TP order")
            except Exception as e:
                log.warning(f"[{ticker}] TP order error: {e}")

        # ---- Phase 2: monitor for TP fill and SL (active mid watch) ----
        exit_cents:  int = 0
        exit_reason: str = "expired"
        sl_active    = sl_c is not None  # SL only when BRACKET_SL_CENTS is set
        fast_polling = False             # switched on when mid drops near SL alert

        while True:
            # Check if resting TP order filled
            if series_is_live and tp_order_id:
                try:
                    tp_status, tp_filled_price = client.get_order_status(tp_order_id, filled_side)
                    if tp_status == "filled":
                        exit_reason = "take_profit"
                        exit_cents  = tp_filled_price
                        tp_order_id = None
                        log.info(f"[{ticker}] TP limit FILLED @ {exit_cents}c")
                        break
                except Exception:
                    pass

            try:
                ob = client.get_orderbook(ticker, expiry_ts)
            except Exception as e:
                log.warning(f"[{ticker}] Orderbook error: {e}")
                time.sleep(2)
                continue

            tte = expiry_ts - time.time()
            bid = ob.yes_bid if filled_side == "yes" else ob.no_bid
            mid = ob.mid_yes if filled_side == "yes" else (100 - ob.mid_yes)

            # Paper TP check
            if not series_is_live and bid >= tp_alert_c:
                exit_reason = "take_profit"
                exit_cents  = sell_c
                log.info(f"[{ticker}] PAPER TP  bid={bid}c >= {tp_alert_c}c — exit@{sell_c}c")
                break

            # SL alert: switch to 1s polling when mid drops near SL threshold
            if sl_active and sl_alert_c is not None and not fast_polling and mid <= sl_alert_c:
                fast_polling = True
                log.info(f"[{ticker}] SL alert  mid={mid}c <= {sl_alert_c}c — switching to 1s polling")

            # SL: mid dropped — cancel TP order then limit sell at sl_c (live) / exit at sl_c (paper)
            # Using limit sell instead of market sell prevents catastrophic fills
            # when the market has gapped far below sl_c with no liquidity.
            if sl_active and mid <= sl_c:
                exit_reason = "stop_loss"
                if series_is_live:
                    if tp_order_id:
                        client.cancel_order(tp_order_id)
                        tp_order_id = None
                    try:
                        sell_id = client.place_order(ticker, filled_side, "limit", sl_c, action="sell")
                        actual = None
                        if sell_id:
                            for _ in range(30):
                                time.sleep(2)
                                status, fill_p = client.get_order_status(sell_id, filled_side)
                                if status == "filled":
                                    actual = fill_p
                                    break
                            if actual is None:
                                # Limit didn't fill in 60s — cancel and accept mid as exit
                                client.cancel_order(sell_id)
                                actual = mid
                        exit_cents = actual if actual is not None else mid
                    except Exception as e:
                        log.warning(f"[{ticker}] SL limit sell error: {e}")
                        exit_cents = mid
                else:
                    exit_cents = sl_c
                log.info(f"[{ticker}] STOP_LOSS  mid={mid}c <= {sl_c}c — exit@{exit_cents}c")
                break

            if tte <= 0:
                exit_reason = "expired"
                if series_is_live and tp_order_id:
                    client.cancel_order(tp_order_id)
                    tp_order_id = None
                result = None
                for _ in range(15):
                    result = client.get_market_result(ticker)
                    if result is not None:
                        break
                    time.sleep(2)
                if result is not None:
                    exit_cents = 100 if result == filled_side else 0
                else:
                    exit_cents = mid
                log.info(f"[{ticker}] EXPIRED  result={result}  exit@{exit_cents}c")
                break

            time.sleep(1 if fast_polling else 2)

        pnl_cents = exit_cents - entry_filled
        sign = "+" if pnl_cents >= 0 else ""
        log.info(
            f"[{ticker}] CLOSED  side={filled_side}  entry={entry_filled}c  "
            f"exit={exit_cents}c  reason={exit_reason}  "
            f"PnL={sign}{pnl_cents}c / {sign}${pnl_cents/100:.4f}"
        )

        log_trade(
            conn, ticker, series, profile, "bracket", filled_side,
            entry_filled, exit_cents, exit_reason, pnl_cents,
        )
        record_daily_pnl(pnl_cents)

        # If contract expired, no more rounds possible
        if exit_reason == "expired":
            break

        # One trade per contract in both paper and live mode
        log.info(f"[{ticker}] One trade per contract — done")
        break


# ---------------------------------------------------------------------------
# Per-series worker thread
# ---------------------------------------------------------------------------

def recover_open_position(client: KalshiClient, series: str, log: logging.LoggerAdapter) -> dict:
    """On startup, check Kalshi for any open position in this series.
    Returns dict of ticker -> (side, qty) for positions that need monitoring."""
    recovered: dict[str, tuple[str, int]] = {}
    if not (LIVE_MODE and (not LIVE_SERIES or series in LIVE_SERIES)):
        return recovered
    try:
        data = client._http.get(
            KALSHI_BASE_URL + "/portfolio/positions",
            headers=client._auth.headers("GET", "/trade-api/v2/portfolio/positions"),
        )
        data.raise_for_status()
        raw = data.json()
        log.info(f"Startup position API keys: {list(raw.keys())}")
        positions = raw.get("market_positions", raw.get("positions", raw.get("holdings", [])))
        log.info(f"Startup positions found: {len(positions)}")
    except Exception as e:
        log.warning(f"Startup position check failed: {e}")
        return recovered

    for p in positions:
        ticker = p.get("ticker", "")
        qty    = int(float(p.get("position_fp", 0) or 0))
        if series not in ticker or qty == 0:
            continue
        side = "yes" if qty > 0 else "no"
        qty  = abs(qty)
        log.warning(f"[{ticker}] Found unmanaged {side.upper()} x{qty} position on startup — will resume monitoring")
        recovered[ticker] = (side, qty)
    return recovered


def resume_live_monitor(
    client: KalshiClient,
    log: logging.LoggerAdapter,
    ticker: str,
    side: str,
    expiry_ts: float,
    cfg: dict,
) -> None:
    """Resume monitoring an existing live position after a crash/restart.
    Places a fresh TP limit sell and runs the SL watch loop — skips entry phase."""
    sell_c     = cfg.get("BRACKET_SELL_CENTS", 87)
    sl_c       = cfg.get("BRACKET_SL_CENTS", None)
    sl_alert_c = cfg.get("BRACKET_SL_ALERT_CENTS", None)

    log.info(f"[{ticker}] RESUME MONITOR  side={side.upper()}  TP={sell_c}c  SL={sl_c}c")

    # Place resting TP limit sell
    tp_order_id: Optional[str] = None
    try:
        tp_order_id = client.place_order(ticker, side, "limit", sell_c, action="sell")
        if tp_order_id:
            log.info(f"[{ticker}] Recovery TP SELL placed @ {sell_c}c  id={tp_order_id}")
        else:
            log.warning(f"[{ticker}] Recovery TP order returned no ID")
    except Exception as e:
        log.warning(f"[{ticker}] Recovery TP order error: {e}")

    # Monitor loop — same logic as Phase 2 in run_bracket_cycle
    exit_cents:  int = 0
    exit_reason: str = "expired"
    sl_active    = sl_c is not None
    fast_polling = False

    while True:
        if tp_order_id:
            try:
                tp_status, tp_filled_price = client.get_order_status(tp_order_id, side)
                if tp_status == "filled":
                    exit_reason = "take_profit"
                    exit_cents  = tp_filled_price
                    tp_order_id = None
                    log.info(f"[{ticker}] Recovery TP FILLED @ {exit_cents}c")
                    break
            except Exception:
                pass

        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error in recovery monitor: {e}")
            time.sleep(2)
            continue

        tte = expiry_ts - time.time()
        mid = ob.mid_yes if side == "yes" else (100 - ob.mid_yes)

        if sl_active and sl_alert_c is not None and not fast_polling and mid <= sl_alert_c:
            fast_polling = True
            log.info(f"[{ticker}] SL alert  mid={mid}c <= {sl_alert_c}c — switching to 1s polling")

        if sl_active and mid <= sl_c:
            exit_reason = "stop_loss"
            if tp_order_id:
                client.cancel_order(tp_order_id)
                tp_order_id = None
            try:
                sell_id = client.place_order(ticker, side, "limit", sl_c, action="sell")
                actual = None
                if sell_id:
                    for _ in range(30):
                        time.sleep(2)
                        status, fill_p = client.get_order_status(sell_id, side)
                        if status == "filled":
                            actual = fill_p
                            break
                    if actual is None:
                        client.cancel_order(sell_id)
                        actual = mid
                exit_cents = actual if actual is not None else mid
            except Exception as e:
                log.warning(f"[{ticker}] Recovery SL limit sell error: {e}")
                exit_cents = mid
            log.info(f"[{ticker}] STOP_LOSS  mid={mid}c <= {sl_c}c — exit@{exit_cents}c")
            break

        if tte <= 0:
            exit_reason = "expired"
            if tp_order_id:
                client.cancel_order(tp_order_id)
                tp_order_id = None
            result = None
            for _ in range(15):
                result = client.get_market_result(ticker)
                if result is not None:
                    break
                time.sleep(2)
            if result is not None:
                exit_cents = 100 if result == side else 0
            else:
                exit_cents = mid
            log.info(f"[{ticker}] EXPIRED  result={result}  exit@{exit_cents}c")
            break

        time.sleep(1 if fast_polling else 2)

    log.info(f"[{ticker}] RECOVERY CLOSED  side={side}  exit={exit_cents}c  reason={exit_reason}")


def series_worker(
    series: str,
    profile: str,
    auth: KalshiAuth,
    conn: sqlite3.Connection,
    stop_event: threading.Event,
) -> None:
    log = get_log(series)
    client = KalshiClient(auth)
    known_ticker: Optional[str] = None

    log.info(f"Worker started — series={series} profile={profile}")

    # Check if this series is excluded for this profile
    excluded = PROFILES.get(profile, {}).get("EXCLUDED_SERIES", set())
    if series in excluded:
        log.info(f"Series {series} is excluded for profile {profile} — worker idle")
        stop_event.wait()  # sleep forever until stop signal
        return

    # In-memory guard: tickers we've already entered (or attempted) this session.
    # Prevents re-entry even if the Kalshi position check fails.
    # Seeded with any positions recovered on startup so restarts don't double-enter.
    recovered_positions: dict[str, tuple[str, int]] = recover_open_position(client, series, log)
    traded_tickers: set[str] = set(recovered_positions.keys())

    while not stop_event.is_set():
        try:
            result = find_next_contract(client, series)
        except Exception as e:
            log.error(f"Contract detection error: {e}")
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        if result is None:
            log.debug("No suitable contract found")
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        ticker, expiry_ts, _ = result
        tte = expiry_ts - time.time()

        if ticker == known_ticker:
            log.debug(f"Contract unchanged  tte={tte:.0f}s")
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        if ticker in traded_tickers:
            if ticker in recovered_positions:
                # Crash recovery: resume monitoring the existing open position
                side, qty = recovered_positions.pop(ticker)
                log.warning(f"[{ticker}] Recovered {side.upper()} x{qty} position — resuming TP/SL monitor")
                cfg = PROFILES.get(profile, {})
                try:
                    resume_live_monitor(client, log, ticker, side, expiry_ts, cfg)
                except Exception as e:
                    log.error(f"[{ticker}] Resume monitor error: {e}", exc_info=True)
                if not stop_event.is_set() and profile in BRACKET_PROFILE_NAMES:
                    sleep_until_next_contract(log, buffer_s=0)
            else:
                log.warning(f"[{ticker}] Already traded this contract this session — skipping")
                time.sleep(MARKET_POLL_INTERVAL)
            continue

        log.info(f"New contract: {ticker}  tte={tte:.0f}s")
        known_ticker = ticker

        # Bracket profiles use their own cycle function
        if profile in BRACKET_PROFILE_NAMES:
            try:
                run_bracket_cycle(client, conn, log, series, profile, ticker, expiry_ts)
            except Exception as e:
                log.error(f"Bracket cycle error: {e}", exc_info=True)
            # Do NOT sleep here — the next contract opens at the same boundary
            # this one expired, so sleeping to the next quarter mark skips it.
            # The polling loop above catches it within ~40s naturally.
            continue

        # Momentum profiles: 3-window entry system
        # Window 1: after ENTRY_WAIT_SECONDS, scan until 5-min mark
        # Window 2: spot check at 5-min mark (scan ~60s)
        # Window 3: spot check at 10-min mark / last 5 min (scan ~60s)
        # Re-entry is allowed in later windows if a prior trade already closed.
        contract_open_ts = expiry_ts - 900
        w1_start_ts = contract_open_ts + ENTRY_WAIT_SECONDS
        w2_ts       = contract_open_ts + 300   # 5 min in
        w3_ts       = contract_open_ts + 600   # 10 min in (last 5 min)

        windows = [
            (1, w1_start_ts, w2_ts,        False),  # (window#, start, scan_end, spot_check)
            (2, w2_ts,       w2_ts + 60,   True),
            (3, w3_ts,       w3_ts + 60,   True),
        ]

        for win_num, win_start, scan_end, spot_check in windows:
            if stop_event.is_set():
                break

            # Skip window if not enough time left
            tte_now = expiry_ts - time.time()
            if tte_now < TIME_STOP_SECONDS + 60:
                log.info(f"[{ticker}] Not enough time for window {win_num} — done with contract")
                break

            # Sleep until window opens
            wait_s = win_start - time.time()
            if wait_s > 0:
                log.info(f"[{ticker}] Window {win_num} opens in {wait_s:.0f}s — waiting")
                time.sleep(wait_s)

            if circuit_breaker_open():
                log.info(f"[{ticker}] Circuit breaker — skipping remaining windows")
                break

            log.info(f"[{ticker}] Window {win_num} {'spot check' if spot_check else 'scan'} starting")

            # Retry loop within each window (handles transient API errors)
            while not stop_event.is_set():
                try:
                    status = run_cycle(
                        client, conn, log, series, profile,
                        ticker, expiry_ts,
                        skip_wait=True,          # timing managed here in series_worker
                        scan_end_ts=scan_end,
                    )
                except Exception as e:
                    log.error(f"Cycle error (window {win_num}): {e}", exc_info=True)
                    status = "retry"
                    time.sleep(3)

                if status == "traded":
                    log.info(f"[{ticker}] Window {win_num}: trade closed — next window eligible for re-entry")
                    break  # move to next window
                elif status == "no_entry":
                    log.info(f"[{ticker}] Window {win_num}: no entry — moving to next window")
                    break  # move to next window
                elif status == "retry":
                    log.info(f"[{ticker}] Window {win_num}: transient error — retrying within window")
                    # Only retry if scan window hasn't expired
                    if time.time() >= scan_end:
                        break

        if not stop_event.is_set():
            sleep_until_next_contract(log)

    client.close()
    log.info("Worker stopped.")

# ---------------------------------------------------------------------------
# Stats reporter (runs every 15 min)
# ---------------------------------------------------------------------------

def stats_reporter(conn: sqlite3.Connection, stop_event: threading.Event) -> None:
    log = get_log("STATS")
    while not stop_event.is_set():
        time.sleep(900)  # every 15 min
        try:
            rows = conn.execute(
                """SELECT series, mode, COUNT(*) as trades,
                          SUM(pnl_cents) as total_pnl,
                          SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins
                   FROM bracket_trade_log
                   WHERE created_at >= datetime('now', '-24 hours')
                   GROUP BY series, mode
                   ORDER BY series"""
            ).fetchall()

            if not rows:
                log.info("No trades in last 24h yet")
                continue

            log.info("─" * 50)
            log.info("24h PAPER TRADE SUMMARY:")
            total_pnl = 0
            for series, mode, trades, pnl, wins in rows:
                win_rate = wins / trades if trades else 0
                pnl_usd = (pnl or 0) / 100.0
                total_pnl += pnl or 0
                log.info(
                    f"  {series:>12} [{mode:>11}]  "
                    f"trades={trades:>3}  win%={win_rate:.0%}  "
                    f"PnL={pnl or 0:>+5}c  ${pnl_usd:>+6.2f}"
                )
            total_wins  = sum(wins for _, _, _, _, wins in rows)
            total_trades_all = sum(trades for _, _, trades, _, _ in rows)
            log.info(f"  {'TOTAL':>12}                    PnL={total_pnl:>+5}c  ${total_pnl/100:.2f}")
            log.info("─" * 50)
        except Exception as e:
            log.error(f"Stats error: {e}")

# ---------------------------------------------------------------------------
# Scheduled Discord summary (8am and 8pm ET, all profiles, last 12h)
# ---------------------------------------------------------------------------

_SUMMARY_HOURS_ET = {8, 20}

def discord_summary_scheduler(conn: sqlite3.Connection, stop_event: threading.Event) -> None:
    """Fire a Discord summary at 8am and 8pm ET showing last 12h across all profiles."""
    fired_hours: set[str] = set()  # "YYYY-MM-DD-HH" keys to prevent double-fire
    while not stop_event.is_set():
        time.sleep(60)
        try:
            now_et = datetime.now(timezone.utc) + timedelta(hours=-4 if _is_edt() else -5)
            hour = now_et.hour
            if hour not in _SUMMARY_HOURS_ET:
                continue
            key = now_et.strftime("%Y-%m-%d-") + str(hour)
            if key in fired_hours:
                continue

            et_time = now_et.strftime("%I:%M %p ET")
            label   = "Morning" if hour == 8 else "Evening"

            # Pull last 12h stats by profile
            profile_rows = conn.execute(
                """SELECT profile,
                          COUNT(*) as trades,
                          SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins,
                          SUM(pnl_cents) as pnl
                   FROM bracket_trade_log
                   WHERE created_at >= datetime('now', '-12 hours')
                   GROUP BY profile
                   ORDER BY pnl DESC"""
            ).fetchall()

            if not profile_rows:
                fired_hours.add(key)
                continue

            # Pull last 12h stats by profile + market
            mkt_rows = conn.execute(
                """SELECT profile, series,
                          COUNT(*) as trades,
                          SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins,
                          SUM(pnl_cents) as pnl
                   FROM bracket_trade_log
                   WHERE created_at >= datetime('now', '-12 hours')
                   GROUP BY profile, series
                   ORDER BY profile, pnl DESC"""
            ).fetchall()

            # Index market rows by profile
            from collections import defaultdict
            mkt_by_profile: dict = defaultdict(list)
            for profile, series, t, w, p in mkt_rows:
                short = series.replace("KX", "").replace("15M", "")  # e.g. BTC, ETH
                mkt_by_profile[profile].append((short, t or 0, w or 0, p or 0))

            total_pnl = sum(r[3] or 0 for r in profile_rows)
            icon = "📈" if total_pnl >= 0 else "📉"
            lines = [f"{icon} **{label} Summary** | {et_time} | Last 12h\n"]

            for profile, trades, wins, pnl in profile_rows:
                trades = trades or 0
                wins   = wins or 0
                pnl    = pnl or 0
                wp     = wins / trades * 100 if trades else 0
                p_icon = "✅" if pnl > 0 else "❌" if pnl < 0 else "➖"
                lines.append(
                    f"{p_icon} **{profile}** — {trades}t | {wp:.0f}% win | {pnl:+}c (${pnl/100:+.2f})"
                )
                # Per-market breakdown (compact, one line)
                mkts = mkt_by_profile.get(profile, [])
                if mkts:
                    mkt_parts = []
                    for short, t, w, p in mkts:
                        wp_m = w / t * 100 if t else 0
                        mkt_parts.append(f"{short} {t}t {wp_m:.0f}% {p:+}c")
                    lines.append("  " + " | ".join(mkt_parts))

            lines.append(f"\n**Combined: {total_pnl:+}c (${total_pnl/100:+.2f})**")
            _send_discord("\n".join(lines))

            fired_hours.add(key)
            if len(fired_hours) > 50:
                fired_hours = set(list(fired_hours)[-20:])
        except Exception as e:
            get_log("DISCORD").error(f"Summary scheduler error: {e}")


# ---------------------------------------------------------------------------
# Report HTTP server
# ---------------------------------------------------------------------------

_report_app = Flask(__name__)
_report_conn: Optional[sqlite3.Connection] = None
_report_auth: Optional[KalshiAuth] = None
_bot_start_time = time.time()


def _check_api_key() -> bool:
    key = os.environ.get("BOT_API_KEY", "")
    if not key:
        return True  # no key configured = open
    return request.headers.get("X-Bot-Api-Key") == key


@_report_app.route("/health")
def health():
    return jsonify({"status": "ok", "uptime_s": int(time.time() - _bot_start_time)})


@_report_app.route("/heatmap")
def heatmap():
    """Win rate and P&L broken down by hour of day (UTC), across all time."""
    if not _check_api_key():
        return jsonify({"error": "unauthorized"}), 401
    if _report_conn is None:
        return jsonify({"error": "db not ready"}), 503

    rows = _report_conn.execute(
        """SELECT
               CAST(strftime('%H', created_at) AS INTEGER) as hour,
               COUNT(*) as trades,
               SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins,
               SUM(pnl_cents) as total_pnl
           FROM bracket_trade_log
           GROUP BY hour
           ORDER BY hour"""
    ).fetchall()

    by_market = _report_conn.execute(
        """SELECT
               series,
               CAST(strftime('%H', created_at) AS INTEGER) as hour,
               COUNT(*) as trades,
               SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins,
               SUM(pnl_cents) as total_pnl
           FROM bracket_trade_log
           GROUP BY series, hour
           ORDER BY series, hour"""
    ).fetchall()

    return jsonify({
        "by_hour": [
            {
                "hour": r[0],
                "trades": r[1],
                "wins": r[2],
                "win_pct": round(r[2] / r[1] * 100, 1) if r[1] else 0,
                "pnl_cents": r[3] or 0,
            }
            for r in rows
        ],
        "by_market_hour": [
            {
                "series": r[0],
                "hour": r[1],
                "trades": r[2],
                "wins": r[3],
                "win_pct": round(r[3] / r[2] * 100, 1) if r[2] else 0,
                "pnl_cents": r[4] or 0,
            }
            for r in by_market
        ],
    })


@_report_app.route("/report")
def report():
    if not _check_api_key():
        return jsonify({"error": "unauthorized"}), 401
    if _report_conn is None:
        return jsonify({"error": "db not ready"}), 503

    rows_24h = _report_conn.execute(
        """SELECT series, profile, mode, side, entry_cents, exit_cents, exit_reason, pnl_cents, created_at
           FROM bracket_trade_log
           WHERE created_at >= datetime('now', '-24 hours')
           ORDER BY created_at DESC LIMIT 100"""
    ).fetchall()

    by_series = _report_conn.execute(
        """SELECT series, profile,
                  COUNT(*) as trades,
                  SUM(pnl_cents) as total_pnl,
                  SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins
           FROM bracket_trade_log
           WHERE created_at >= datetime('now', '-24 hours')
           GROUP BY series, profile ORDER BY profile, series"""
    ).fetchall()

    by_profile = _report_conn.execute(
        """SELECT profile,
                  COUNT(*) as trades,
                  SUM(pnl_cents) as total_pnl,
                  SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins
           FROM bracket_trade_log
           WHERE created_at >= datetime('now', '-24 hours')
           GROUP BY profile ORDER BY profile"""
    ).fetchall()

    total_pnl    = sum(r[3] or 0 for r in by_series)
    total_trades = sum(r[2] for r in by_series)
    total_wins   = sum(r[4] or 0 for r in by_series)

    return jsonify({
        "uptime_s": int(time.time() - _bot_start_time),
        "summary": {
            "trades_24h": total_trades,
            "wins_24h": total_wins,
            "win_pct": round(total_wins / total_trades * 100, 1) if total_trades else 0,
            "pnl_cents": total_pnl,
            "pnl_usd": round(total_pnl / 100, 2),
        },
        "by_profile": [
            {
                "profile": r[0],
                "trades": r[1],
                "pnl_cents": r[2] or 0,
                "wins": r[3] or 0,
                "win_pct": round((r[3] or 0) / r[1] * 100, 1) if r[1] else 0,
                "pnl_usd": round((r[2] or 0) / 100, 2),
            }
            for r in by_profile
        ],
        "by_series": [
            {
                "series": r[0],
                "profile": r[1],
                "trades": r[2],
                "pnl_cents": r[3] or 0,
                "wins": r[4] or 0,
                "win_pct": round((r[4] or 0) / r[2] * 100, 1) if r[2] else 0,
            }
            for r in by_series
        ],
        "recent_trades": [
            {
                "series": r[0], "profile": r[1], "mode": r[2], "side": r[3],
                "entry": r[4], "exit": r[5], "reason": r[6],
                "pnl_cents": r[7], "time": r[8],
            }
            for r in rows_24h[:20]
        ],
    })


@_report_app.route("/markets")
def markets():
    if _report_auth is None:
        return jsonify({"error": "auth not ready"}), 503
    try:
        client = KalshiClient(_report_auth)
        balance = client.get_balance()
        results = []
        for series in SERIES:
            try:
                mkts = client.get_open_markets(series, limit=5)
                now = time.time()
                candidates = [(m, _parse_iso(m.get("close_time") or m.get("expiration_time") or "")) for m in mkts]
                candidates = [(m, exp) for m, exp in candidates if exp - now > 90]
                if not candidates:
                    results.append({"series": series, "status": "no_contract"})
                    continue
                candidates.sort(key=lambda x: x[1])
                mkt, expiry_ts = candidates[0]
                ticker = mkt["ticker"]
                vol = float(mkt.get("volume_fp", 0) or 0)
                ob = client.get_orderbook(ticker, expiry_ts)
                is_live = LIVE_MODE and (not LIVE_SERIES or series in LIVE_SERIES)
                series_threshold = max(MOMENTUM_THRESHOLD_CENTS, SERIES_THRESHOLD_OVERRIDE.get(series, 0))
                signal = "YES" if ob.yes_bid >= series_threshold else "NO" if ob.no_bid >= series_threshold else "none"
                dollar_vol = round(vol)
                combined = (ob.yes_ask or 0) + (ob.no_ask or 0)
                liquid = combined <= 120
                results.append({
                    "series": series,
                    "ticker": ticker,
                    "yes_bid": ob.yes_bid, "yes_ask": ob.yes_ask,
                    "no_bid": ob.no_bid,   "no_ask": ob.no_ask,
                    "volume_contracts": round(vol),
                    "volume_dollars": dollar_vol,
                    "combined_ask": combined,
                    "liquid_enough": liquid,
                    "signal": signal,
                    "live": is_live,
                    "would_trade": signal != "none" and liquid and is_live,
                })
            except Exception as e:
                results.append({"series": series, "error": str(e)})
        return jsonify({"balance_cents": balance, "threshold": MOMENTUM_THRESHOLD_CENTS, "markets": results})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@_report_app.route("/debug/market")
def debug_market():
    if _report_auth is None:
        return jsonify({"error": "auth not ready"}), 503
    try:
        client = KalshiClient(_report_auth)
        mkts = client.get_open_markets("KXBTC15M", limit=1)
        return jsonify(mkts[0] if mkts else {})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@_report_app.route("/logs")
def logs():
    log_file = f"/opt/kalshi-bot/logs/{ACTIVE_PROFILE}.log"
    try:
        with open(log_file) as f:
            lines = f.readlines()
        return jsonify({"lines": lines[-100:]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@_report_app.route("/status")
def status():
    return jsonify({
        "profile":     ACTIVE_PROFILE,
        "live_mode":   LIVE_MODE,
        "live_series": sorted(LIVE_SERIES) if LIVE_SERIES else "ALL",
        "paper_series": [s for s in SERIES if s not in LIVE_SERIES] if LIVE_SERIES else [],
        "threshold_cents": MOMENTUM_THRESHOLD_CENTS,
        "max_entry_cents": MOMENTUM_MAX_ENTRY_CENTS,
        "uptime_s":    int(time.time() - _bot_start_time),
    })


@_report_app.route("/history")
def history():
    if not _check_api_key():
        return jsonify({"error": "unauthorized"}), 401
    if _report_conn is None:
        return jsonify({"error": "db not ready"}), 503

    days = min(int(request.args.get("days", 7)), 30)
    profile = request.args.get("profile", None)

    group_by_hour = request.args.get("by_hour", "false").lower() == "true"
    where = f"created_at >= datetime('now', '-{days} days')"
    if profile:
        where += f" AND profile = '{profile}'"

    if group_by_hour:
        rows = _report_conn.execute(
            f"""SELECT strftime('%Y-%m-%d %H', created_at) as hour, profile, series,
                      COUNT(*) as trades,
                      SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins,
                      SUM(pnl_cents) as pnl
               FROM bracket_trade_log
               WHERE {where}
               GROUP BY hour, profile, series
               ORDER BY hour DESC, profile, trades DESC"""
        ).fetchall()
        return jsonify([
            {"hour": r[0], "profile": r[1], "series": r[2],
             "trades": r[3], "wins": r[4], "win_pct": round((r[4] or 0) / r[3] * 100, 1) if r[3] else 0,
             "pnl_cents": r[5] or 0}
            for r in rows
        ])

    rows = _report_conn.execute(
        f"""SELECT date(created_at) as day, profile, series,
                  COUNT(*) as trades,
                  SUM(CASE WHEN pnl_cents > 0 THEN 1 ELSE 0 END) as wins,
                  SUM(pnl_cents) as pnl
           FROM bracket_trade_log
           WHERE {where}
           GROUP BY day, profile, series
           ORDER BY day DESC, profile, trades DESC"""
    ).fetchall()

    return jsonify([
        {"day": r[0], "profile": r[1], "series": r[2],
         "trades": r[3], "wins": r[4], "win_pct": round((r[4] or 0) / r[3] * 100, 1) if r[3] else 0,
         "pnl_cents": r[5] or 0}
        for r in rows
    ])


def start_report_server(conn: sqlite3.Connection, port: int, auth: KalshiAuth = None) -> None:
    global _report_conn, _report_auth
    _report_auth = auth
    _report_conn = conn
    import logging as _logging
    _logging.getLogger("werkzeug").setLevel(_logging.ERROR)
    _report_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global _root_log, ACTIVE_PROFILE
    global MOMENTUM_THRESHOLD_CENTS, MOMENTUM_MIN_ENTRY_CENTS, MOMENTUM_MAX_ENTRY_CENTS, ENTRY_WAIT_SECONDS
    global SCAN_WINDOW_SECONDS, TAKE_PROFIT_CENTS, STOP_LOSS_CENTS, SL_OFFSET_CENTS, SL_ALERT_CENTS
    global TIME_STOP_SECONDS, DAILY_LOSS_LIMIT_CENTS, LIVE_MODE, LIVE_SERIES

    parser = argparse.ArgumentParser(description="Kalshi Multi-Market Paper Scalper")
    parser.add_argument(
        "--profile", choices=list(PROFILES.keys()), default="moderate",
        help="Risk profile to run (conservative / moderate / risky)",
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="HTTP report server port (default: 8001/8000/8002 per profile)",
    )
    parser.add_argument(
        "--live", action="store_true", default=False,
        help="Enable live trading (real orders). Paper mode if omitted.",
    )
    parser.add_argument(
        "--live-series", type=str, default="",
        help="Comma-separated list of series to trade live (e.g. KXBNB15M,KXDOGE15M). "
             "All series are live if --live is set and this is omitted.",
    )
    args = parser.parse_args()

    ACTIVE_PROFILE = args.profile
    profile_cfg = PROFILES[ACTIVE_PROFILE]

    # Apply profile params as module globals so all functions pick them up.
    # Bracket profiles only define bracket-specific keys; use .get() with
    # module-level defaults so momentum globals are left untouched for them.
    MOMENTUM_THRESHOLD_CENTS = profile_cfg.get("MOMENTUM_THRESHOLD_CENTS", MOMENTUM_THRESHOLD_CENTS)
    MOMENTUM_MIN_ENTRY_CENTS = profile_cfg.get("MOMENTUM_MIN_ENTRY_CENTS", MOMENTUM_MIN_ENTRY_CENTS)
    MOMENTUM_MAX_ENTRY_CENTS = profile_cfg.get("MOMENTUM_MAX_ENTRY_CENTS", MOMENTUM_MAX_ENTRY_CENTS)
    ENTRY_WAIT_SECONDS       = profile_cfg.get("ENTRY_WAIT_SECONDS",       ENTRY_WAIT_SECONDS)
    SCAN_WINDOW_SECONDS      = profile_cfg.get("SCAN_WINDOW_SECONDS",      SCAN_WINDOW_SECONDS)
    TAKE_PROFIT_CENTS        = profile_cfg.get("TAKE_PROFIT_CENTS",        TAKE_PROFIT_CENTS)
    STOP_LOSS_CENTS          = profile_cfg.get("STOP_LOSS_CENTS",          STOP_LOSS_CENTS)
    SL_OFFSET_CENTS          = profile_cfg.get("SL_OFFSET_CENTS",          SL_OFFSET_CENTS)
    SL_ALERT_CENTS           = profile_cfg.get("SL_ALERT_CENTS",           SL_ALERT_CENTS)
    TIME_STOP_SECONDS        = profile_cfg.get("TIME_STOP_SECONDS",        TIME_STOP_SECONDS)
    DAILY_LOSS_LIMIT_CENTS   = profile_cfg["DAILY_LOSS_LIMIT_CENTS"]
    LIVE_MODE                = args.live or os.environ.get("LIVE_MODE", "false").lower() == "true"
    raw_live_series          = args.live_series or os.environ.get("LIVE_SERIES", "")
    LIVE_SERIES              = {s.strip() for s in raw_live_series.split(",") if s.strip()}

    # Default port per profile so all can run simultaneously
    default_ports = {
        "moderate":          8000,
        "conservative":      8001,
        "moderate-rr12":     8002,
        "og-risky":          8003,
        "risky-low":         8004,
        "risky-mid":         8005,
        "risky-high-early":  8006,
        "risky-high-late":   8007,
    }
    port = args.port if args.port is not None else default_ports.get(ACTIVE_PROFILE, 8000)

    _load_dotenv(".env")

    # Per-profile log file so logs don't interleave.
    # Live service gets its own file so it doesn't double-write with the paper service.
    global LOG_PATH
    suffix = "-live" if LIVE_MODE else ""
    LOG_PATH = f"./logs/{ACTIVE_PROFILE}{suffix}.log"
    _root_log = setup_logging()

    log = get_log("MAIN")
    mode_str = "LIVE TRADING" if LIVE_MODE else "PAPER MODE"
    log.info("=" * 60)
    log.info(f"Multi-Market Scalper  [{mode_str}]  profile={ACTIVE_PROFILE.upper()}")
    if LIVE_MODE and LIVE_SERIES:
        live_mkts  = sorted(LIVE_SERIES)
        paper_mkts = [s for s in SERIES if s not in LIVE_SERIES]
        log.info(f"LIVE markets : {', '.join(live_mkts)}")
        log.info(f"PAPER markets: {', '.join(paper_mkts)}")
    elif LIVE_MODE:
        log.info(f"LIVE markets : ALL")
    log.info(f"Markets  : {', '.join(SERIES)}")
    if ACTIVE_PROFILE in BRACKET_PROFILE_NAMES:
        log.info(f"Strategy : BRACKET  entry={profile_cfg['BRACKET_ENTRY_CENTS']}c  "
                 f"tp_alert={profile_cfg['BRACKET_TP_ALERT_CENTS']}c  "
                 f"sell={profile_cfg['BRACKET_SELL_CENTS']}c  "
                 f"window_start={profile_cfg['BRACKET_WINDOW_START_SECONDS']}s  "
                 f"window_dur={profile_cfg['BRACKET_WINDOW_DURATION_SECONDS']}s")
        log.info(f"Risk     : daily_limit={DAILY_LOSS_LIMIT_CENTS}c  no SL  let expire naturally")
    else:
        log.info(f"Signal   : bid >= {MOMENTUM_THRESHOLD_CENTS}c after {ENTRY_WAIT_SECONDS}s  entry_window={MOMENTUM_MIN_ENTRY_CENTS}–{MOMENTUM_MAX_ENTRY_CENTS}c")
        log.info(f"Exit     : TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c  TimeStop={TIME_STOP_SECONDS}s")
        log.info(f"Risk     : daily_limit={DAILY_LOSS_LIMIT_CENTS}c")
        log.info(f"Orders   : limit@ask-1c  timeout={LIMIT_ORDER_TIMEOUT}s  then market fallback")
    log.info(f"Report   : http://0.0.0.0:{port}/report")
    log.info("=" * 60)

    api_key  = os.environ.get("KALSHI_API_KEY", "b16ab35a-2d26-4bd3-93c6-a1e8fd6c7a95")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)

    try:
        auth = KalshiAuth(api_key, key_path)
        log.info("RSA key loaded OK")
    except FileNotFoundError as e:
        log.error(str(e))
        sys.exit(1)

    try:
        conn = init_db(DB_PATH)
        log.info(f"Database: {DB_PATH}")
    except Exception as e:
        log.error(f"DB error: {e}")
        sys.exit(1)

    stop_event = threading.Event()

    # Start WebSocket ticker feed for live mode
    if LIVE_MODE:
        global _ticker_ws
        _ticker_ws = KalshiTickerWS(auth)
        _ticker_ws.start()

    # Start one worker thread per series
    threads: list[threading.Thread] = []
    for series in SERIES:
        t = threading.Thread(
            target=series_worker,
            args=(series, ACTIVE_PROFILE, auth, conn, stop_event),
            name=f"worker-{series}",
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(0.1)  # stagger startup slightly

    # Start stats reporter
    stats_t = threading.Thread(
        target=stats_reporter,
        args=(conn, stop_event),
        name="stats",
        daemon=True,
    )
    stats_t.start()

    # Start Discord scheduled summary thread
    discord_t = threading.Thread(
        target=discord_summary_scheduler,
        args=(conn, stop_event),
        name="discord-summary",
        daemon=True,
    )
    discord_t.start()

    # Start report HTTP server
    report_t = threading.Thread(
        target=start_report_server,
        args=(conn, port, auth),
        name="report-server",
        daemon=True,
    )
    report_t.start()
    log.info(f"Report server started on port {port}")

    log.info(f"All {len(SERIES)} market workers started. Press Ctrl+C to stop.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down...")
        stop_event.set()
        for t in threads:
            t.join(timeout=10)

    conn.close()
    log.info("Stopped.")


if __name__ == "__main__":
    main()
