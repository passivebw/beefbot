"""
multi_scalper.py - Kalshi 7-Market Paper Scalper

Runs bracket + directional strategies across all 7 crypto 15-min markets
simultaneously using one thread per series.

Strategy per market:
  Bracket (all markets, all hours):
    Place YES@ENTRY + NO@ENTRY at contract open.
    First fill cancels the other. TP=56c, SL=20c, time-stop last 3min.

  Directional overlay (bias hours only):
    DOGE/BNB  20:00-24:00 UTC  → place YES only (65%/62% historical win rate)
    HYPE      00:00-04:00 UTC  → place YES only (59% historical win rate)

All trades are PAPER ONLY — no real orders placed.

Run:
    python multi_scalper.py

Output:
    logs/multi_scalper.log
    data/bot.db  (bracket_trade_log table)
"""

from __future__ import annotations

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

# Directional bias windows: (utc_hour_start, utc_hour_end, side)
# During these hours, place ONLY the biased side instead of full bracket
BIAS: dict[str, tuple[int, int, str] | None] = {
    "KXBTC15M":  None,
    "KXETH15M":  None,
    "KXSOL15M":  None,
    "KXXRP15M":  None,
    "KXDOGE15M": (20, 24, "yes"),   # 4-8pm ET → YES 65.6%
    "KXBNB15M":  (20, 24, "yes"),   # 4-8pm ET → YES 62.5%
    "KXHYPE15M": (0,  4,  "yes"),   # 8pm-midnight ET → YES 59.1%
}

# Strategy parameters
ENTRY_CENTS       = 36
TAKE_PROFIT_CENTS = 56
STOP_LOSS_CENTS   = 20
TIME_STOP_SECONDS = 180   # exit at mid if <=3 min remaining
FILL_WINDOW_SECONDS = 60  # cancel if no fill within 1 min
CONTRACTS         = 1

MARKET_POLL_INTERVAL = 2   # seconds between contract detection polls
ORDER_POLL_INTERVAL  = 5   # seconds between fill/exit checks

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
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger = logging.getLogger("multi_scalper")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(fh)
    logger.addHandler(ch)
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
    conn.commit()
    return conn


def log_trade(
    conn: sqlite3.Connection,
    ticker: str,
    series: str,
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
               (kalshi_ticker,series,mode,side,contracts,entry_cents,
                exit_cents,exit_reason,pnl_cents,pnl_usd)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (ticker, series, mode, side, CONTRACTS, entry_cents,
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
        ob = r.json().get("orderbook", {})
        yes_bids = ob.get("yes", [])
        no_bids  = ob.get("no", [])

        yes_bid = yes_bids[0][0] if yes_bids else 0
        no_bid  = no_bids[0][0]  if no_bids  else 0
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


def get_bias_side(series: str) -> Optional[str]:
    """Return 'yes' or 'no' if we're in a bias window, else None (= full bracket)."""
    bias = BIAS.get(series)
    if not bias:
        return None
    start_h, end_h, side = bias
    utc_hour = datetime.now(tz=timezone.utc).hour
    if start_h <= utc_hour < end_h:
        return side
    return None


def check_fill(ob: OrderBook, side: str) -> bool:
    if side == "yes":
        return ob.yes_ask <= ENTRY_CENTS
    else:
        return ob.no_ask <= ENTRY_CENTS


def check_take_profit(ob: OrderBook, side: str) -> bool:
    if side == "yes":
        return ob.yes_bid >= TAKE_PROFIT_CENTS
    else:
        return ob.no_bid >= TAKE_PROFIT_CENTS


def check_stop_loss(ob: OrderBook, side: str) -> bool:
    mid = ob.mid_yes if side == "yes" else (100 - ob.mid_yes)
    return mid <= STOP_LOSS_CENTS


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
    ticker: str,
    expiry_ts: float,
) -> None:
    """Run one full bracket/directional cycle for one contract."""

    tte = expiry_ts - time.time()
    min_required = FILL_WINDOW_SECONDS + TIME_STOP_SECONDS + 30
    if tte < min_required:
        log.info(f"[{ticker}] Not enough time (tte={tte:.0f}s < {min_required}s) — skipping")
        return

    bias_side = get_bias_side(series)
    if bias_side:
        mode = "directional"
        sides_to_try = [bias_side]
        log.info(f"[{ticker}] DIRECTIONAL mode — placing {bias_side.upper()} @ {ENTRY_CENTS}c  (bias window active)")
    else:
        mode = "bracket"
        sides_to_try = ["yes", "no"]
        log.info(f"[{ticker}] BRACKET mode — placing YES @ {ENTRY_CENTS}c + NO @ {ENTRY_CENTS}c")

    placed_at = time.time()
    fill_deadline = placed_at + FILL_WINDOW_SECONDS
    filled_side: Optional[str] = None

    # ---- Phase 1: wait for fill ----
    while time.time() < fill_deadline:
        time.sleep(ORDER_POLL_INTERVAL)

        tte = expiry_ts - time.time()
        if tte <= 0:
            log.info(f"[{ticker}] Expired during fill window — cancelling")
            return

        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error: {e}")
            continue

        log.debug(
            f"[{ticker}] ob: yes_bid={ob.yes_bid} yes_ask={ob.yes_ask} "
            f"no_bid={ob.no_bid} no_ask={ob.no_ask}  tte={tte:.0f}s"
        )

        for side in sides_to_try:
            if check_fill(ob, side):
                filled_side = side
                log.info(
                    f"[{ticker}] FILL: {side.upper()} @ {ENTRY_CENTS}c  "
                    f"({'yes_ask' if side=='yes' else 'no_ask'}="
                    f"{ob.yes_ask if side=='yes' else ob.no_ask})"
                )
                break

        if filled_side:
            break
    else:
        elapsed = time.time() - placed_at
        log.info(f"[{ticker}] Fill window expired ({elapsed:.0f}s) — no fill")
        return

    # ---- Phase 2: manage position ----
    log.info(
        f"[{ticker}] Position open: LONG {filled_side.upper()} @ {ENTRY_CENTS}c  "
        f"TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c"
    )

    exit_reason: Optional[str] = None
    exit_cents:  Optional[int] = None

    while True:
        time.sleep(ORDER_POLL_INTERVAL)

        tte = expiry_ts - time.time()

        try:
            ob = client.get_orderbook(ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{ticker}] Orderbook error: {e}")
            continue

        mid = current_mid(ob, filled_side)
        log.debug(
            f"[{ticker}] pos: side={filled_side} mid={mid}c  "
            f"yes_bid={ob.yes_bid} yes_ask={ob.yes_ask}  tte={tte:.0f}s"
        )

        if tte <= TIME_STOP_SECONDS:
            exit_reason = "time_stop"
            exit_cents  = mid
            log.info(f"[{ticker}] TIME STOP  tte={tte:.0f}s  exit@{exit_cents}c")
            break

        if check_take_profit(ob, filled_side):
            exit_reason = "take_profit"
            exit_cents  = TAKE_PROFIT_CENTS
            log.info(f"[{ticker}] TAKE PROFIT  exit@{exit_cents}c")
            break

        if check_stop_loss(ob, filled_side):
            exit_reason = "stop_loss"
            exit_cents  = mid
            log.info(f"[{ticker}] STOP LOSS  mid={mid}c  exit@{exit_cents}c")
            break

        if tte <= 0:
            exit_reason = "expired"
            exit_cents  = mid
            log.info(f"[{ticker}] EXPIRED  exit@{exit_cents}c")
            break

    # ---- Record ----
    assert exit_reason and exit_cents is not None
    pnl_cents = exit_cents - ENTRY_CENTS
    pnl_usd   = pnl_cents / 100.0 * CONTRACTS
    sign = "+" if pnl_cents >= 0 else ""

    log_trade(
        conn, ticker, series, mode, filled_side,
        ENTRY_CENTS, exit_cents, exit_reason, pnl_cents * CONTRACTS,
    )

    log.info(
        f"[{ticker}] CLOSED  side={filled_side}  entry={ENTRY_CENTS}c  "
        f"exit={exit_cents}c  reason={exit_reason}  "
        f"PnL={sign}{pnl_cents}c / {sign}${pnl_usd:.4f}"
    )
    print(
        f"\n{'='*60}\n"
        f"  [{series}] TRADE CLOSED\n"
        f"  Ticker  : {ticker}\n"
        f"  Mode    : {mode}\n"
        f"  Side    : {filled_side.upper()}\n"
        f"  Entry   : {ENTRY_CENTS}c\n"
        f"  Exit    : {exit_cents}c  ({exit_reason})\n"
        f"  PnL     : {sign}{pnl_cents}c  ({sign}${pnl_usd:.4f})\n"
        f"{'='*60}\n",
        flush=True,
    )

# ---------------------------------------------------------------------------
# Per-series worker thread
# ---------------------------------------------------------------------------

def series_worker(
    series: str,
    auth: KalshiAuth,
    conn: sqlite3.Connection,
    stop_event: threading.Event,
) -> None:
    log = get_log(series)
    client = KalshiClient(auth)
    known_ticker: Optional[str] = None

    log.info(f"Worker started — series={series}")

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
            run_cycle(client, conn, log, series, ticker, expiry_ts)
        except Exception as e:
            log.error(f"Cycle error: {e}", exc_info=True)

        # Sleep until next boundary
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
            log.info(f"  {'TOTAL':>12}                    PnL={total_pnl:>+5}c  ${total_pnl/100:.2f}")
            log.info("─" * 50)
        except Exception as e:
            log.error(f"Stats error: {e}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global _root_log
    _load_dotenv(".env")
    _root_log = setup_logging()

    log = get_log("MAIN")
    log.info("=" * 60)
    log.info("Multi-Market Scalper  [PAPER MODE]")
    log.info(f"Markets : {', '.join(SERIES)}")
    log.info(f"Strategy: bracket @ {ENTRY_CENTS}c  TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c")
    log.info("Bias    : DOGE/BNB 20-24 UTC YES | HYPE 00-04 UTC YES")
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
            args=(series, auth, conn, stop_event),
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
