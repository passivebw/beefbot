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
import logging
import math
import os
import sqlite3
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
MOMENTUM_MAX_ENTRY_CENTS = 72   # skip if ask already above this (bad R/R)
ENTRY_WAIT_SECONDS       = 180  # wait 3 min for direction to establish
SCAN_WINDOW_SECONDS      = 480  # scan from min 3 to min 11 (8 min window)
TAKE_PROFIT_CENTS        = 85   # exit when bid reaches this
STOP_LOSS_CENTS          = 50   # exit if mid drops back to 50c
SL_ALERT_CENTS           = 55   # switch to fast polling when mid drops here
TIME_STOP_SECONDS        = 120  # exit at whatever price with 2 min left
CONTRACTS                = 1

MARKET_POLL_INTERVAL = 2   # seconds between contract detection polls
ORDER_POLL_INTERVAL  = 5   # seconds between fill/exit checks
SL_POLL_INTERVAL     = 1   # fast polling when near stop loss

# ---------------------------------------------------------------------------
# Risk profiles — loaded at startup via --profile arg
# ---------------------------------------------------------------------------

PROFILES: dict[str, dict] = {
    "conservative": {
        # Fewer trades, higher-quality signals, best R/R (~49% breakeven)
        "MOMENTUM_THRESHOLD_CENTS": 68,   # only very strong Kalshi signal
        "MOMENTUM_MAX_ENTRY_CENTS": 71,   # tight cap → avg entry ~70c
        "ENTRY_WAIT_SECONDS":       240,  # 4 min for direction to establish
        "SCAN_WINDOW_SECONDS":      360,  # stop scanning at minute 10
        "TAKE_PROFIT_CENTS":        88,   # +18c win from avg entry
        "STOP_LOSS_CENTS":          53,   # -17c loss → breakeven ~49%
        "SL_ALERT_CENTS":           57,
        "TIME_STOP_SECONDS":        120,
        "PRICE_MOMENTUM_MIN_PCT":   0.10, # strong Binance confirmation
        "VOLUME_RATIO_MIN":         1.5,  # high volume conviction
        "FUNDING_RATE_MAX":         0.0008,
        "FEAR_GREED_EXTREME":       25,
    },
    "moderate": {
        # Balanced — current live params (~54% breakeven)
        "MOMENTUM_THRESHOLD_CENTS": 65,
        "MOMENTUM_MAX_ENTRY_CENTS": 72,
        "ENTRY_WAIT_SECONDS":       180,
        "SCAN_WINDOW_SECONDS":      480,
        "TAKE_PROFIT_CENTS":        85,
        "STOP_LOSS_CENTS":          50,
        "SL_ALERT_CENTS":           55,
        "TIME_STOP_SECONDS":        120,
        "PRICE_MOMENTUM_MIN_PCT":   0.05,
        "VOLUME_RATIO_MIN":         1.2,
        "FUNDING_RATE_MAX":         0.0010,
        "FEAR_GREED_EXTREME":       20,
    },
    "risky-0m": {
        # Risky params, enter immediately at contract open
        "MOMENTUM_THRESHOLD_CENTS": 60,
        "MOMENTUM_MAX_ENTRY_CENTS": 76,
        "ENTRY_WAIT_SECONDS":       0,    # enter right at open
        "SCAN_WINDOW_SECONDS":      600,
        "TAKE_PROFIT_CENTS":        90,
        "STOP_LOSS_CENTS":          45,
        "SL_ALERT_CENTS":           52,
        "TIME_STOP_SECONDS":        90,
        "PRICE_MOMENTUM_MIN_PCT":   0.02,
        "VOLUME_RATIO_MIN":         1.0,
        "FUNDING_RATE_MAX":         0.0020,
        "FEAR_GREED_EXTREME":       10,
    },
    "risky-1m": {
        # Risky params, enter after 1 minute
        "MOMENTUM_THRESHOLD_CENTS": 60,
        "MOMENTUM_MAX_ENTRY_CENTS": 76,
        "ENTRY_WAIT_SECONDS":       60,   # 1 min wait
        "SCAN_WINDOW_SECONDS":      600,
        "TAKE_PROFIT_CENTS":        90,
        "STOP_LOSS_CENTS":          45,
        "SL_ALERT_CENTS":           52,
        "TIME_STOP_SECONDS":        90,
        "PRICE_MOMENTUM_MIN_PCT":   0.02,
        "VOLUME_RATIO_MIN":         1.0,
        "FUNDING_RATE_MAX":         0.0020,
        "FEAR_GREED_EXTREME":       10,
    },
    "risky-2m": {
        # Risky params, enter after 2 minutes
        "MOMENTUM_THRESHOLD_CENTS": 60,
        "MOMENTUM_MAX_ENTRY_CENTS": 76,
        "ENTRY_WAIT_SECONDS":       120,  # 2 min wait
        "SCAN_WINDOW_SECONDS":      600,
        "TAKE_PROFIT_CENTS":        90,
        "STOP_LOSS_CENTS":          45,
        "SL_ALERT_CENTS":           52,
        "TIME_STOP_SECONDS":        90,
        "PRICE_MOMENTUM_MIN_PCT":   0.02,
        "VOLUME_RATIO_MIN":         1.0,
        "FUNDING_RATE_MAX":         0.0020,
        "FEAR_GREED_EXTREME":       10,
    },
}

# Active profile name — set by --profile arg in main()
ACTIVE_PROFILE = "moderate"

# External confirmation filters
PRICE_MOMENTUM_MIN_PCT   = 0.05  # require at least 0.05% price move in signal direction
VOLUME_RATIO_MIN         = 1.2   # last candle volume must be 1.2x the recent average
FUNDING_RATE_MAX         = 0.0010  # skip if funding rate strongly opposes direction (0.10%)
FEAR_GREED_EXTREME       = 20    # skip if F&G < 20 (extreme fear) on YES, or > 80 on NO

# Binance symbol mapping
BINANCE_SYMBOL = {
    "KXBTC15M":  "BTCUSDT",
    "KXETH15M":  "ETHUSDT",
    "KXSOL15M":  "SOLUSDT",
    "KXXRP15M":  "XRPUSDT",
    "KXDOGE15M": "DOGEUSDT",
    "KXBNB15M":  "BNBUSDT",
    "KXHYPE15M": "HYPEUSDT",
}

# Per-series momentum threshold overrides (added on top of profile threshold)
# XRP and ETH historically weaker signals — require higher conviction to enter
SERIES_THRESHOLD_OVERRIDE = {
    "KXXRP15M": 67,
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

    def close(self) -> None:
        self._http.close()

# ---------------------------------------------------------------------------
# External market data (Binance + Fear & Greed)
# ---------------------------------------------------------------------------

class ExternalData:
    """Fetches Binance price momentum, funding rate, and Fear & Greed index."""

    BINANCE_SPOT    = "https://api.binance.com/api/v3"
    BINANCE_FUTURES = "https://fapi.binance.com/fapi/v1"
    FEAR_GREED_URL  = "https://api.alternative.me/fng/?limit=1"

    def __init__(self) -> None:
        self._http = httpx.Client(timeout=5.0)
        self._fg_cache: tuple[float, int] = (0.0, 50)  # (timestamp, value)

    def price_and_volume(self, symbol: str, lookback: int = 10) -> Optional[dict]:
        """
        Return last-15min momentum + volume ratio vs recent average.
        Uses 15m candles to match Kalshi contract timeframe.
        lookback=10 means compare last candle volume to 10-candle average.
        """
        try:
            r = self._http.get(
                f"{self.BINANCE_SPOT}/klines",
                params={"symbol": symbol, "interval": "15m", "limit": lookback + 1},
            )
            r.raise_for_status()
            candles = r.json()
            if len(candles) < 3:
                return None

            # Last completed candle (index -2), current forming candle (index -1)
            last     = candles[-2]
            baseline = candles[:-1]  # all but the forming candle

            last_open   = float(last[1])
            last_close  = float(last[4])
            last_volume = float(last[5])
            avg_volume  = sum(float(c[5]) for c in baseline) / len(baseline)

            momentum_pct  = (last_close - last_open) / last_open * 100
            volume_ratio  = last_volume / avg_volume if avg_volume > 0 else 1.0

            return {"momentum_pct": momentum_pct, "volume_ratio": volume_ratio}
        except Exception:
            return None

    def funding_rate(self, symbol: str) -> Optional[float]:
        """Return latest perpetual funding rate. Positive = longs pay shorts."""
        try:
            r = self._http.get(
                f"{self.BINANCE_FUTURES}/fundingRate",
                params={"symbol": symbol, "limit": 1},
            )
            r.raise_for_status()
            data = r.json()
            if not data:
                return None
            return float(data[0]["fundingRate"])
        except Exception:
            return None

    def fear_greed(self) -> int:
        """Return Fear & Greed index (0=extreme fear, 100=extreme greed). Cached 10min."""
        now = time.time()
        if now - self._fg_cache[0] < 600:
            return self._fg_cache[1]
        try:
            r = self._http.get(self.FEAR_GREED_URL)
            r.raise_for_status()
            value = int(r.json()["data"][0]["value"])
            self._fg_cache = (now, value)
            return value
        except Exception:
            return self._fg_cache[1]  # return cached on error

    def confirm_entry(self, series: str, side: str, log: logging.LoggerAdapter) -> bool:
        """
        Returns True if external signals confirm the Kalshi momentum signal.
        Checks: price momentum direction, funding rate, fear & greed.
        """
        symbol = BINANCE_SYMBOL.get(series)
        if not symbol:
            return True  # no mapping = no filter

        # 1. Price momentum + volume must confirm direction
        pv = self.price_and_volume(symbol)
        if pv is not None:
            momentum = pv["momentum_pct"]
            vol_ratio = pv["volume_ratio"]
            up = momentum > PRICE_MOMENTUM_MIN_PCT
            dn = momentum < -PRICE_MOMENTUM_MIN_PCT
            if side == "yes" and not up:
                log.info(f"CONFIRM FAIL: {symbol} momentum={momentum:+.3f}% vol={vol_ratio:.2f}x — need up for YES")
                return False
            if side == "no" and not dn:
                log.info(f"CONFIRM FAIL: {symbol} momentum={momentum:+.3f}% vol={vol_ratio:.2f}x — need down for NO")
                return False
            if vol_ratio < VOLUME_RATIO_MIN:
                log.info(f"CONFIRM FAIL: {symbol} volume={vol_ratio:.2f}x < {VOLUME_RATIO_MIN}x avg — low conviction")
                return False
            log.debug(f"CONFIRM OK: {symbol} momentum={momentum:+.3f}% vol={vol_ratio:.2f}x")
        else:
            log.debug(f"CONFIRM: {symbol} price/volume unavailable — skipping check")

        # 2. Funding rate — skip if strongly opposing
        funding = self.funding_rate(symbol)
        if funding is not None:
            if side == "yes" and funding > FUNDING_RATE_MAX:
                log.info(f"CONFIRM FAIL: {symbol} funding={funding:.4f} too high for YES (over-extended long)")
                return False
            if side == "no" and funding < -FUNDING_RATE_MAX:
                log.info(f"CONFIRM FAIL: {symbol} funding={funding:.4f} too negative for NO (over-extended short)")
                return False
            log.debug(f"CONFIRM OK: {symbol} funding={funding:.4f}")

        # 3. Fear & Greed extreme filter
        fg = self.fear_greed()
        if side == "yes" and fg < FEAR_GREED_EXTREME:
            log.info(f"CONFIRM FAIL: Fear&Greed={fg} extreme fear — skip YES")
            return False
        if side == "no" and fg > (100 - FEAR_GREED_EXTREME):
            log.info(f"CONFIRM FAIL: Fear&Greed={fg} extreme greed — skip NO")
            return False
        log.debug(f"CONFIRM OK: Fear&Greed={fg}")

        return True

    def close(self) -> None:
        self._http.close()


# ---------------------------------------------------------------------------
# Strategy helpers
# ---------------------------------------------------------------------------

def find_next_contract(client: KalshiClient, series: str) -> Optional[tuple[str, float]]:
    """Return (ticker, expiry_ts) for the soonest open contract with >90s remaining."""
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
            candidates.append((tte, m["ticker"], exp))

    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    _, ticker, exp = candidates[0]
    return ticker, exp


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
    ext: ExternalData,
    conn: sqlite3.Connection,
    log: logging.LoggerAdapter,
    series: str,
    profile: str,
    ticker: str,
    expiry_ts: float,
) -> None:
    """Momentum-follow cycle: wait 3 min, enter on strong directional signal."""

    tte = expiry_ts - time.time()
    min_required = ENTRY_WAIT_SECONDS + 60 + TIME_STOP_SECONDS + 30
    if tte < min_required:
        log.info(f"[{ticker}] Not enough time (tte={tte:.0f}s < {min_required}s) — skipping")
        return

    # ---- Phase 1: wait for direction to establish ----
    log.info(f"[{ticker}] MOMENTUM mode — waiting {ENTRY_WAIT_SECONDS}s for direction...")
    time.sleep(ENTRY_WAIT_SECONDS)

    # ---- Phase 2: scan for momentum signal ----
    scan_deadline = time.time() + SCAN_WINDOW_SECONDS
    filled_side:  Optional[str] = None
    entry_cents:  Optional[int] = None

    while time.time() < scan_deadline:
        time.sleep(ORDER_POLL_INTERVAL)

        tte = expiry_ts - time.time()
        if tte <= TIME_STOP_SECONDS + 30:
            log.info(f"[{ticker}] Too late to enter (tte={tte:.0f}s) — no trade")
            return

        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error: {e}")
            continue

        log.debug(
            f"[{ticker}] scan: yes_bid={ob.yes_bid} no_bid={ob.no_bid}  tte={tte:.0f}s"
        )

        threshold = max(MOMENTUM_THRESHOLD_CENTS, SERIES_THRESHOLD_OVERRIDE.get(series, 0))

        if ob.yes_bid >= threshold:
            ask = ob.yes_ask
            if ask > MOMENTUM_MAX_ENTRY_CENTS:
                log.debug(f"[{ticker}] YES signal but ask={ask}c > max {MOMENTUM_MAX_ENTRY_CENTS}c — skipping")
                continue
            if not ext.confirm_entry(series, "yes", log):
                continue
            filled_side = "yes"
            entry_cents = ask
            log.info(f"[{ticker}] MOMENTUM ENTRY: YES @ {entry_cents}c  (yes_bid={ob.yes_bid})")
            break
        elif ob.no_bid >= threshold:
            ask = ob.no_ask
            if ask > MOMENTUM_MAX_ENTRY_CENTS:
                log.debug(f"[{ticker}] NO signal but ask={ask}c > max {MOMENTUM_MAX_ENTRY_CENTS}c — skipping")
                continue
            if not ext.confirm_entry(series, "no", log):
                continue
            filled_side = "no"
            entry_cents = ask
            log.info(f"[{ticker}] MOMENTUM ENTRY: NO @ {entry_cents}c  (no_bid={ob.no_bid})")
            break
    else:
        log.info(f"[{ticker}] No momentum signal — skipping")
        return

    # ---- Phase 3: manage position ----
    log.info(
        f"[{ticker}] Position open: LONG {filled_side.upper()} @ {entry_cents}c  "
        f"TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c(mid)"
    )

    exit_reason: Optional[str] = None
    exit_cents:  Optional[int] = None

    while True:
        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error: {e}")
            time.sleep(ORDER_POLL_INTERVAL)
            continue

        tte = expiry_ts - time.time()
        mid = current_mid(ob, filled_side)
        bid = ob.yes_bid if filled_side == "yes" else ob.no_bid

        log.debug(
            f"[{ticker}] pos: side={filled_side} mid={mid}c bid={bid}c  tte={tte:.0f}s"
        )

        if tte <= TIME_STOP_SECONDS:
            exit_reason = "time_stop"
            exit_cents  = mid
            log.info(f"[{ticker}] TIME STOP  tte={tte:.0f}s  exit@{exit_cents}c")
            break

        if bid >= TAKE_PROFIT_CENTS:
            exit_reason = "take_profit"
            exit_cents  = TAKE_PROFIT_CENTS
            log.info(f"[{ticker}] TAKE PROFIT  exit@{exit_cents}c")
            break

        if mid <= STOP_LOSS_CENTS:
            exit_reason = "stop_loss"
            exit_cents  = STOP_LOSS_CENTS  # lock to SL price, not slipped price
            log.info(f"[{ticker}] STOP LOSS  mid={mid}c  exit@{exit_cents}c")
            break

        # Fast polling when near SL to catch it quickly
        time.sleep(SL_POLL_INTERVAL if mid <= SL_ALERT_CENTS else ORDER_POLL_INTERVAL)

        if tte <= 0:
            exit_reason = "expired"
            exit_cents  = mid
            log.info(f"[{ticker}] EXPIRED  exit@{exit_cents}c")
            break

    # ---- Record ----
    assert exit_reason and exit_cents is not None and entry_cents is not None
    pnl_cents = exit_cents - entry_cents
    pnl_usd   = pnl_cents / 100.0 * CONTRACTS
    sign = "+" if pnl_cents >= 0 else ""

    log_trade(
        conn, ticker, series, profile, "momentum", filled_side,
        entry_cents, exit_cents, exit_reason, pnl_cents * CONTRACTS,
    )

    log.info(
        f"[{ticker}] CLOSED  side={filled_side}  entry={entry_cents}c  "
        f"exit={exit_cents}c  reason={exit_reason}  "
        f"PnL={sign}{pnl_cents}c / {sign}${pnl_usd:.4f}"
    )

# ---------------------------------------------------------------------------
# Per-series worker thread
# ---------------------------------------------------------------------------

def series_worker(
    series: str,
    profile: str,
    auth: KalshiAuth,
    conn: sqlite3.Connection,
    stop_event: threading.Event,
) -> None:
    log = get_log(series)
    client = KalshiClient(auth)
    ext    = ExternalData()
    known_ticker: Optional[str] = None

    log.info(f"Worker started — series={series} profile={profile}")

    # Sleep until just before next 15-min boundary
    sleep_until_next_contract(log)

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

        ticker, expiry_ts = result
        tte = expiry_ts - time.time()

        if ticker == known_ticker:
            log.debug(f"Contract unchanged  tte={tte:.0f}s")
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        log.info(f"New contract: {ticker}  tte={tte:.0f}s")
        known_ticker = ticker

        try:
            run_cycle(client, ext, conn, log, series, profile, ticker, expiry_ts)
        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        # Sleep until next boundary
        if not stop_event.is_set():
            sleep_until_next_contract(log)

    client.close()
    ext.close()
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
            log.info(f"  {'TOTAL':>12}                    PnL={total_pnl:>+5}c  ${total_pnl/100:.2f}")
            log.info("─" * 50)
        except Exception as e:
            log.error(f"Stats error: {e}")

# ---------------------------------------------------------------------------
# Report HTTP server
# ---------------------------------------------------------------------------

_report_app = Flask(__name__)
_report_conn: Optional[sqlite3.Connection] = None
_bot_start_time = time.time()


def _check_api_key() -> bool:
    key = os.environ.get("BOT_API_KEY", "")
    if not key:
        return True  # no key configured = open
    return request.headers.get("X-Bot-Api-Key") == key


@_report_app.route("/health")
def health():
    return jsonify({"status": "ok", "uptime_s": int(time.time() - _bot_start_time)})


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


def start_report_server(conn: sqlite3.Connection, port: int) -> None:
    global _report_conn
    _report_conn = conn
    import logging as _logging
    _logging.getLogger("werkzeug").setLevel(_logging.ERROR)
    _report_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global _root_log, ACTIVE_PROFILE
    global MOMENTUM_THRESHOLD_CENTS, MOMENTUM_MAX_ENTRY_CENTS, ENTRY_WAIT_SECONDS
    global SCAN_WINDOW_SECONDS, TAKE_PROFIT_CENTS, STOP_LOSS_CENTS, SL_ALERT_CENTS
    global TIME_STOP_SECONDS, PRICE_MOMENTUM_MIN_PCT, VOLUME_RATIO_MIN
    global FUNDING_RATE_MAX, FEAR_GREED_EXTREME

    parser = argparse.ArgumentParser(description="Kalshi Multi-Market Paper Scalper")
    parser.add_argument(
        "--profile", choices=list(PROFILES.keys()), default="moderate",
        help="Risk profile to run (conservative / moderate / risky)",
    )
    parser.add_argument(
        "--port", type=int, default=None,
        help="HTTP report server port (default: 8001/8000/8002 per profile)",
    )
    args = parser.parse_args()

    ACTIVE_PROFILE = args.profile
    profile_cfg = PROFILES[ACTIVE_PROFILE]

    # Apply profile params as module globals so all functions pick them up
    MOMENTUM_THRESHOLD_CENTS = profile_cfg["MOMENTUM_THRESHOLD_CENTS"]
    MOMENTUM_MAX_ENTRY_CENTS = profile_cfg["MOMENTUM_MAX_ENTRY_CENTS"]
    ENTRY_WAIT_SECONDS       = profile_cfg["ENTRY_WAIT_SECONDS"]
    SCAN_WINDOW_SECONDS      = profile_cfg["SCAN_WINDOW_SECONDS"]
    TAKE_PROFIT_CENTS        = profile_cfg["TAKE_PROFIT_CENTS"]
    STOP_LOSS_CENTS          = profile_cfg["STOP_LOSS_CENTS"]
    SL_ALERT_CENTS           = profile_cfg["SL_ALERT_CENTS"]
    TIME_STOP_SECONDS        = profile_cfg["TIME_STOP_SECONDS"]
    PRICE_MOMENTUM_MIN_PCT   = profile_cfg["PRICE_MOMENTUM_MIN_PCT"]
    VOLUME_RATIO_MIN         = profile_cfg["VOLUME_RATIO_MIN"]
    FUNDING_RATE_MAX         = profile_cfg["FUNDING_RATE_MAX"]
    FEAR_GREED_EXTREME       = profile_cfg["FEAR_GREED_EXTREME"]

    # Default port per profile so all 3 can run simultaneously
    default_ports = {"conservative": 8001, "moderate": 8000, "risky": 8002}
    port = args.port if args.port is not None else default_ports[ACTIVE_PROFILE]

    _load_dotenv(".env")

    # Per-profile log file so logs don't interleave
    global LOG_PATH
    LOG_PATH = f"./logs/{ACTIVE_PROFILE}.log"
    _root_log = setup_logging()

    log = get_log("MAIN")
    log.info("=" * 60)
    log.info(f"Multi-Market Scalper  [PAPER MODE]  profile={ACTIVE_PROFILE.upper()}")
    log.info(f"Markets  : {', '.join(SERIES)}")
    log.info(f"Signal   : bid >= {MOMENTUM_THRESHOLD_CENTS}c after {ENTRY_WAIT_SECONDS}s  max_entry={MOMENTUM_MAX_ENTRY_CENTS}c")
    log.info(f"Exit     : TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c  TimeStop={TIME_STOP_SECONDS}s")
    log.info(f"Filters  : momentum>={PRICE_MOMENTUM_MIN_PCT}%  vol>={VOLUME_RATIO_MIN}x  funding<={FUNDING_RATE_MAX}")
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

    # Start report HTTP server
    report_t = threading.Thread(
        target=start_report_server,
        args=(conn, port),
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
