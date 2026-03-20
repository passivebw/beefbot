"""
bracket_scalper.py - Kalshi KXBTC15M Bracket Scalper (Paper Trading)

Strategy:
  - At the open of each new KXBTC15M 15-min contract, place YES @ 36c AND NO @ 36c.
  - 1-minute fill window: if neither fills, cancel both and wait for next contract.
  - If one fills, cancel the other immediately.
  - Take profit at 56c (+20c).
  - Stop loss if price drops to 20c (-16c).
  - Time stop: exit at market mid if <=3 minutes remain.
  - Position size: 1 contract per trade.

All orders are PAPER ONLY — no real Kalshi orders are submitted.

Run:
    python bracket_scalper.py

Dependencies: httpx, cryptography (standard library + those two third-party packages)
"""

from __future__ import annotations

import base64
import logging
import os
import sqlite3
import sys
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional
import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"
SERIES_TICKER = "KXBTC15M"

ENTRY_CENTS = 36          # limit price for both legs
TAKE_PROFIT_CENTS = 56    # exit when bid >= this
STOP_LOSS_CENTS = 20      # exit when mid <= this
TIME_STOP_SECONDS = 180   # exit at market when time-to-expiry <= 3 min
FILL_WINDOW_SECONDS = 60  # cancel if neither fills within 1 min

CONTRACTS = 1             # position size

MARKET_POLL_INTERVAL = 2    # seconds between contract-detection polls
ORDER_POLL_INTERVAL = 5     # seconds between fill / exit polls

DEFAULT_KEY_PATH = "/opt/kalshi-bot/kalshi_private_key.pem"
DB_PATH = "./data/bot.db"
LOG_PATH = "./logs/bracket.log"


# ---------------------------------------------------------------------------
# .env loader (no python-dotenv dependency)
# ---------------------------------------------------------------------------

def _load_dotenv(path: str = ".env") -> None:
    """Parse KEY=VALUE lines from a .env file and set them as environment variables."""
    p = Path(path)
    if not p.exists():
        return
    with open(p) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    log_dir = Path(LOG_PATH).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(fmt)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)

    logger = logging.getLogger("bracket_scalper")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


log: logging.Logger  # assigned in main()


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS bracket_trade_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at   TEXT    NOT NULL DEFAULT (datetime('now','utc')),
    kalshi_ticker TEXT   NOT NULL,
    side         TEXT    NOT NULL,
    contracts    INTEGER NOT NULL,
    entry_cents  INTEGER NOT NULL,
    exit_cents   INTEGER,
    exit_reason  TEXT,
    pnl_cents    INTEGER,
    pnl_usd      REAL
);
"""


def init_db(db_path: str) -> sqlite3.Connection:
    """Open (or create) the shared bot.db and ensure bracket_trade_log exists."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn


def log_trade(
    conn: sqlite3.Connection,
    ticker: str,
    side: str,
    contracts: int,
    entry_cents: int,
    exit_cents: int,
    exit_reason: str,
    pnl_cents: int,
) -> None:
    pnl_usd = pnl_cents / 100.0
    conn.execute(
        """
        INSERT INTO bracket_trade_log
            (kalshi_ticker, side, contracts, entry_cents, exit_cents,
             exit_reason, pnl_cents, pnl_usd)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (ticker, side, contracts, entry_cents, exit_cents,
         exit_reason, pnl_cents, pnl_usd),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Kalshi auth + HTTP
# ---------------------------------------------------------------------------

class KalshiAuth:
    """Generates RSA-PSS signed request headers for the Kalshi API."""

    def __init__(self, api_key: str, private_key_path: str) -> None:
        self._api_key = api_key
        self._private_key = self._load_key(private_key_path)

    @staticmethod
    def _load_key(path: str):
        resolved = Path(path)
        if not resolved.exists():
            raise FileNotFoundError(f"Kalshi private key not found: {path}")
        with open(resolved, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    def headers(self, method: str, path: str) -> dict[str, str]:
        """Return auth headers for a Kalshi API request."""
        ts = str(int(time.time() * 1000))
        msg = (ts + method.upper() + path).encode()
        sig = self._private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }


# ---------------------------------------------------------------------------
# Market data structures
# ---------------------------------------------------------------------------

@dataclass
class OrderBook:
    """Snapshot of the best yes/no bid and ask prices in cents."""
    ticker: str
    yes_bid: int   # cents
    yes_ask: int   # cents
    no_bid: int    # cents
    no_ask: int    # cents
    expiry_ts: float

    @property
    def mid_yes(self) -> int:
        """Mid-price of the YES contract in cents (rounded)."""
        return round((self.yes_bid + self.yes_ask) / 2)

    @property
    def seconds_to_expiry(self) -> float:
        return max(0.0, self.expiry_ts - time.time())


# ---------------------------------------------------------------------------
# Kalshi HTTP client (synchronous, paper-only)
# ---------------------------------------------------------------------------

class KalshiClient:
    def __init__(self, auth: KalshiAuth) -> None:
        self._auth = auth

    # ------------------------------------------------------------------
    # Market discovery
    # ------------------------------------------------------------------

    def get_open_markets(self, series_ticker: str, limit: int = 5) -> list[dict]:
        """Return raw market dicts from the Kalshi API."""
        path = "/trade-api/v2/markets"
        url = KALSHI_BASE_URL + "/markets"
        params = {"series_ticker": series_ticker, "status": "open", "limit": limit}
        headers = self._auth.headers("GET", path)
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url, headers=headers, params=params)
            r.raise_for_status()
        return r.json().get("markets", [])

    # ------------------------------------------------------------------
    # Orderbook / quotes
    # ------------------------------------------------------------------

    def get_orderbook(self, ticker: str) -> OrderBook:
        """Fetch the current orderbook for a market and return an OrderBook."""
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        url = KALSHI_BASE_URL + f"/markets/{ticker}/orderbook"
        headers = self._auth.headers("GET", path)
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url, headers=headers)
            r.raise_for_status()
            body = r.json()

        yes_bid_c, no_bid_c = self._parse_orderbook(body)
        yes_ask_c = 100 - no_bid_c
        no_ask_c = 100 - yes_bid_c

        # Fetch expiry from market metadata if we don't have it yet
        expiry_ts = self._get_expiry(ticker)

        return OrderBook(
            ticker=ticker,
            yes_bid=yes_bid_c,
            yes_ask=yes_ask_c,
            no_bid=no_bid_c,
            no_ask=no_ask_c,
            expiry_ts=expiry_ts,
        )

    def get_orderbook_with_expiry(self, ticker: str, expiry_ts: float) -> OrderBook:
        """Fetch orderbook for a ticker when we already know the expiry timestamp."""
        path = f"/trade-api/v2/markets/{ticker}/orderbook"
        url = KALSHI_BASE_URL + f"/markets/{ticker}/orderbook"
        headers = self._auth.headers("GET", path)
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url, headers=headers)
            r.raise_for_status()
            body = r.json()

        yes_bid_c, no_bid_c = self._parse_orderbook(body)
        yes_ask_c = 100 - no_bid_c
        no_ask_c = 100 - yes_bid_c

        return OrderBook(
            ticker=ticker,
            yes_bid=yes_bid_c,
            yes_ask=yes_ask_c,
            no_bid=no_bid_c,
            no_ask=no_ask_c,
            expiry_ts=expiry_ts,
        )

    @staticmethod
    def _parse_orderbook(body: dict) -> tuple[int, int]:
        """Return (yes_bid_cents, no_bid_cents) from raw orderbook response."""
        ob_fp = body.get("orderbook_fp", {})
        if ob_fp:
            yes_bids_fp = ob_fp.get("yes_dollars", [])
            no_bids_fp = ob_fp.get("no_dollars", [])
            yes_bid_c = round(float(yes_bids_fp[-1][0]) * 100) if yes_bids_fp else 0
            no_bid_c = round(float(no_bids_fp[-1][0]) * 100) if no_bids_fp else 0
        else:
            ob = body.get("orderbook", {})
            yes_bids = ob.get("yes", [])
            no_bids = ob.get("no", [])
            yes_bid_c = yes_bids[0][0] if yes_bids else 0
            no_bid_c = no_bids[0][0] if no_bids else 0
        return int(yes_bid_c), int(no_bid_c)

    def _get_expiry(self, ticker: str) -> float:
        """Fetch expiry timestamp for a single market."""
        path = f"/trade-api/v2/markets/{ticker}"
        url = KALSHI_BASE_URL + f"/markets/{ticker}"
        headers = self._auth.headers("GET", path)
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url, headers=headers)
            r.raise_for_status()
            m = r.json().get("market", {})
        return _parse_iso(m.get("close_time", ""))


# ---------------------------------------------------------------------------
# ISO timestamp parser (no dateutil)
# ---------------------------------------------------------------------------

def _parse_iso(s: str) -> float:
    """Parse an ISO-8601 UTC timestamp string to a POSIX float (best-effort)."""
    if not s:
        return 0.0
    # Strip trailing Z / +00:00 and try common formats
    s = s.replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f+00:00",
        "%Y-%m-%dT%H:%M:%S+00:00",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            import datetime
            dt = datetime.datetime.strptime(s.replace("+00:00", ""), fmt.replace("+00:00", "").replace("Z", ""))
            return dt.replace(tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            continue
    return 0.0


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class Phase(Enum):
    WAITING_FOR_CONTRACT = auto()   # no active contract detected yet
    WAITING_FOR_FILL = auto()       # bracket placed, within 1-min window
    IN_POSITION = auto()            # one leg filled, managing exit


@dataclass
class BracketState:
    """All mutable state for the current bracket cycle."""
    ticker: str = ""
    expiry_ts: float = 0.0

    # Bracket placement time
    placed_at: float = 0.0

    # Which side got filled (None until filled)
    filled_side: Optional[str] = None   # "yes" or "no"
    entry_cents: int = 0

    # Entry time (for logging)
    filled_at: float = 0.0

    phase: Phase = Phase.WAITING_FOR_CONTRACT

    def reset(self) -> None:
        self.ticker = ""
        self.expiry_ts = 0.0
        self.placed_at = 0.0
        self.filled_side = None
        self.entry_cents = 0
        self.filled_at = 0.0
        self.phase = Phase.WAITING_FOR_CONTRACT


# ---------------------------------------------------------------------------
# Paper fill simulation
# ---------------------------------------------------------------------------

def check_entry_fill(ob: OrderBook, side: str) -> bool:
    """
    Simulate a limit-buy fill.

    YES @ 36c fills when yes_ask <= 36 (someone is willing to sell YES at/below our bid).
    NO  @ 36c fills when no_ask  <= 36.
    """
    if side == "yes":
        return ob.yes_ask <= ENTRY_CENTS
    else:
        return ob.no_ask <= ENTRY_CENTS


def check_take_profit(ob: OrderBook, side: str) -> bool:
    """
    Take profit at 56c.

    Long YES: yes_bid >= 56 (we can sell into the bid at 56+).
    Long NO:  no_bid  >= 56.
    """
    if side == "yes":
        return ob.yes_bid >= TAKE_PROFIT_CENTS
    else:
        return ob.no_bid >= TAKE_PROFIT_CENTS


def check_stop_loss(ob: OrderBook, side: str) -> bool:
    """
    Stop loss when mid price of our position drops to 20c.
    """
    if side == "yes":
        mid = ob.mid_yes
    else:
        mid = 100 - ob.mid_yes  # mid of NO contract
    return mid <= STOP_LOSS_CENTS


def current_mid(ob: OrderBook, side: str) -> int:
    """Current mid price (in cents) of the held side."""
    if side == "yes":
        return ob.mid_yes
    else:
        return 100 - ob.mid_yes


# ---------------------------------------------------------------------------
# Contract detection helpers
# ---------------------------------------------------------------------------

def next_quarter_hour_ts() -> float:
    """Return the Unix timestamp of the next :00/:15/:30/:45 boundary."""
    import math
    now = time.time()
    quarter = 15 * 60  # 900 seconds
    return math.ceil(now / quarter) * quarter


def sleep_until_next_contract(buffer_s: float = 1.5) -> None:
    """
    Sleep until `buffer_s` seconds before the next 15-min boundary.
    Logs how long we're waiting so the user can see we're alive.
    """
    target = next_quarter_hour_ts() - buffer_s
    wait = target - time.time()
    if wait > 5:
        log.info(f"Sleeping {wait:.0f}s until next contract open (~{wait/60:.1f} min) ...")
        time.sleep(wait)
    # If < 5s away, just let the poll loop handle it immediately


def find_nearest_open_market(client: KalshiClient) -> Optional[tuple[str, float]]:
    """
    Return (ticker, expiry_ts) for the open KXBTC15M contract with the
    soonest expiry that still has meaningful time remaining (>90 s).

    Returns None if no suitable market is found.
    """
    try:
        markets = client.get_open_markets(SERIES_TICKER, limit=5)
    except Exception as e:
        log.warning(f"market poll error: {e}")
        return None

    now = time.time()
    candidates: list[tuple[float, str, float]] = []
    for m in markets:
        exp = _parse_iso(m.get("close_time", ""))
        tte = exp - now
        if tte > 90:
            candidates.append((tte, m["ticker"], exp))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])   # soonest expiry first
    _, ticker, exp = candidates[0]
    return ticker, exp


# ---------------------------------------------------------------------------
# Core strategy loop
# ---------------------------------------------------------------------------

def run_bracket_cycle(
    client: KalshiClient,
    conn: sqlite3.Connection,
    state: BracketState,
    current_ticker: str,
    expiry_ts: float,
) -> None:
    """
    Execute the full bracket lifecycle for one contract:
      1. Place YES and NO paper limits @ 36c.
      2. Poll every 5 s for up to 60 s; cancel if neither fills.
      3. If one fills: cancel the other, monitor for exit conditions.
      4. On exit: record to DB, print summary.
    """
    log.info(
        f"[{current_ticker}] New contract detected — "
        f"tte={expiry_ts - time.time():.0f}s  Placing bracket @ {ENTRY_CENTS}c each"
    )

    state.ticker = current_ticker
    state.expiry_ts = expiry_ts
    state.placed_at = time.time()
    state.phase = Phase.WAITING_FOR_FILL

    # Log simulated order placement
    log.info(
        f"[{current_ticker}] PAPER ORDER: BUY YES @ {ENTRY_CENTS}c  |  "
        f"PAPER ORDER: BUY NO @ {ENTRY_CENTS}c"
    )

    # -----------------------------------------------------------------------
    # Phase 1 — Wait for a fill (up to FILL_WINDOW_SECONDS)
    # -----------------------------------------------------------------------
    fill_deadline = state.placed_at + FILL_WINDOW_SECONDS

    while time.time() < fill_deadline:
        time.sleep(ORDER_POLL_INTERVAL)

        # Safety: if the contract has expired while waiting, bail out
        tte = expiry_ts - time.time()
        if tte <= 0:
            log.info(f"[{current_ticker}] Contract expired during fill window — cancelling bracket")
            state.reset()
            return

        try:
            ob = client.get_orderbook_with_expiry(current_ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{current_ticker}] orderbook fetch error (fill window): {e}")
            continue

        log.debug(
            f"[{current_ticker}] ob: yes_bid={ob.yes_bid} yes_ask={ob.yes_ask} "
            f"no_bid={ob.no_bid} no_ask={ob.no_ask}  tte={tte:.0f}s"
        )

        # Check YES fill first
        if check_entry_fill(ob, "yes"):
            state.filled_side = "yes"
            state.entry_cents = ENTRY_CENTS
            state.filled_at = time.time()
            log.info(
                f"[{current_ticker}] FILL SIMULATED: YES @ {ENTRY_CENTS}c  "
                f"(yes_ask={ob.yes_ask} <= {ENTRY_CENTS})  — cancelling NO leg"
            )
            break

        # Check NO fill
        if check_entry_fill(ob, "no"):
            state.filled_side = "no"
            state.entry_cents = ENTRY_CENTS
            state.filled_at = time.time()
            log.info(
                f"[{current_ticker}] FILL SIMULATED: NO @ {ENTRY_CENTS}c  "
                f"(no_ask={ob.no_ask} <= {ENTRY_CENTS})  — cancelling YES leg"
            )
            break
    else:
        # Fill window expired with no fill
        elapsed = time.time() - state.placed_at
        log.info(
            f"[{current_ticker}] Fill window expired ({elapsed:.0f}s) — "
            "no fill; cancelling both legs. Waiting for next contract."
        )
        state.reset()
        return

    # -----------------------------------------------------------------------
    # Phase 2 — Manage open position
    # -----------------------------------------------------------------------
    state.phase = Phase.IN_POSITION
    filled_side = state.filled_side
    assert filled_side is not None

    log.info(
        f"[{current_ticker}] Position open: LONG {filled_side.upper()} @ {state.entry_cents}c  "
        f"| TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c  time_stop=last {TIME_STOP_SECONDS}s"
    )

    exit_reason: Optional[str] = None
    exit_cents: Optional[int] = None

    while True:
        time.sleep(ORDER_POLL_INTERVAL)

        tte = expiry_ts - time.time()

        # Fetch current quotes
        try:
            ob = client.get_orderbook_with_expiry(current_ticker, expiry_ts)
        except Exception as e:
            log.warning(f"[{current_ticker}] orderbook fetch error (position): {e}")
            # Try again next poll rather than force-exiting on a transient error
            continue

        mid = current_mid(ob, filled_side)
        log.debug(
            f"[{current_ticker}] position check: side={filled_side} mid={mid}c  "
            f"yes_bid={ob.yes_bid} yes_ask={ob.yes_ask} "
            f"no_bid={ob.no_bid} no_ask={ob.no_ask}  tte={tte:.0f}s"
        )

        # Time stop — exit at market mid with <=3 min remaining
        if tte <= TIME_STOP_SECONDS:
            exit_reason = "time_stop"
            exit_cents = mid
            log.info(
                f"[{current_ticker}] TIME STOP triggered — tte={tte:.0f}s <= {TIME_STOP_SECONDS}s  "
                f"exit @ mid {exit_cents}c"
            )
            break

        # Take profit
        if check_take_profit(ob, filled_side):
            exit_reason = "take_profit"
            exit_cents = TAKE_PROFIT_CENTS
            if filled_side == "yes":
                log.info(
                    f"[{current_ticker}] TAKE PROFIT — yes_bid={ob.yes_bid} >= {TAKE_PROFIT_CENTS}c  "
                    f"exit @ {exit_cents}c"
                )
            else:
                log.info(
                    f"[{current_ticker}] TAKE PROFIT — no_bid={ob.no_bid} >= {TAKE_PROFIT_CENTS}c  "
                    f"exit @ {exit_cents}c"
                )
            break

        # Stop loss
        if check_stop_loss(ob, filled_side):
            exit_reason = "stop_loss"
            exit_cents = mid
            log.info(
                f"[{current_ticker}] STOP LOSS triggered — mid={mid}c <= {STOP_LOSS_CENTS}c  "
                f"exit @ {exit_cents}c"
            )
            break

        # Contract expired without hitting any exit condition
        if tte <= 0:
            exit_reason = "expired"
            exit_cents = mid
            log.info(
                f"[{current_ticker}] Contract expired — exit @ mid {exit_cents}c"
            )
            break

    # -----------------------------------------------------------------------
    # Record and report
    # -----------------------------------------------------------------------
    assert exit_reason is not None and exit_cents is not None
    pnl_cents = exit_cents - state.entry_cents   # e.g. +20 for TP, -16 for SL
    pnl_usd = pnl_cents / 100.0 * CONTRACTS

    log_trade(
        conn=conn,
        ticker=current_ticker,
        side=filled_side,
        contracts=CONTRACTS,
        entry_cents=state.entry_cents,
        exit_cents=exit_cents,
        exit_reason=exit_reason,
        pnl_cents=pnl_cents * CONTRACTS,
    )

    sign = "+" if pnl_cents >= 0 else ""
    log.info(
        f"[{current_ticker}] TRADE CLOSED  "
        f"side={filled_side}  entry={state.entry_cents}c  exit={exit_cents}c  "
        f"reason={exit_reason}  PnL={sign}{pnl_cents}c / {sign}${pnl_usd:.2f}"
    )
    print(
        f"\n{'='*60}\n"
        f"  TRADE CLOSED  [{current_ticker}]\n"
        f"  Side    : {filled_side.upper()}\n"
        f"  Entry   : {state.entry_cents}c\n"
        f"  Exit    : {exit_cents}c\n"
        f"  Reason  : {exit_reason}\n"
        f"  PnL     : {sign}{pnl_cents}c  ({sign}${pnl_usd:.4f})\n"
        f"{'='*60}\n",
        flush=True,
    )

    state.reset()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    global log

    # Load environment from .env before anything else
    _load_dotenv(".env")

    log = _setup_logging()
    log.info("=" * 60)
    log.info("Bracket Scalper starting  [PAPER MODE]")
    log.info(f"Strategy: bracket @ {ENTRY_CENTS}c  TP={TAKE_PROFIT_CENTS}c  SL={STOP_LOSS_CENTS}c")
    log.info(f"Fill window: {FILL_WINDOW_SECONDS}s  Time stop: last {TIME_STOP_SECONDS}s")
    log.info("=" * 60)

    # Configuration from environment
    api_key = os.environ.get("KALSHI_API_KEY", "b16ab35a-2d26-4bd3-93c6-a1e8fd6c7a95")
    private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)

    log.info(f"API key  : {api_key}")
    log.info(f"Key path : {private_key_path}")
    log.info(f"DB path  : {DB_PATH}")

    # Auth
    try:
        auth = KalshiAuth(api_key=api_key, private_key_path=private_key_path)
        log.info("RSA private key loaded OK")
    except FileNotFoundError as e:
        log.error(f"Cannot start: {e}")
        sys.exit(1)
    except Exception as e:
        log.error(f"Key load error: {e}")
        sys.exit(1)

    # Database
    try:
        conn = init_db(DB_PATH)
        log.info(f"Database ready: {DB_PATH}")
    except Exception as e:
        log.error(f"DB init failed: {e}")
        sys.exit(1)

    client = KalshiClient(auth)
    state = BracketState()

    known_ticker: Optional[str] = None   # last contract we acted on

    log.info(f"Starting contract detection loop (poll every {MARKET_POLL_INTERVAL}s)...")

    # Sleep until just before the next 15-min boundary for immediate entry
    sleep_until_next_contract()

    while True:
        # ----------------------------------------------------------------
        # Contract detection
        # ----------------------------------------------------------------
        try:
            result = find_nearest_open_market(client)
        except Exception as e:
            log.error(f"Unexpected error in contract detection: {e}", exc_info=True)
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        if result is None:
            log.debug("No suitable open contract found — retrying...")
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        ticker, expiry_ts = result
        tte = expiry_ts - time.time()

        if ticker == known_ticker:
            # Same contract — nothing to do; just wait for next boundary
            log.debug(f"Contract {ticker} unchanged  tte={tte:.0f}s")
            time.sleep(MARKET_POLL_INTERVAL)
            continue

        # New contract detected
        log.info(
            f"New contract: {ticker}  tte={tte:.0f}s  expiry={expiry_ts:.0f}"
        )
        known_ticker = ticker

        # Sanity check: only enter if we have a meaningful amount of time left
        # (need at least the fill window + some position management time)
        min_required = FILL_WINDOW_SECONDS + TIME_STOP_SECONDS + 30
        if tte < min_required:
            log.info(
                f"[{ticker}] Not enough time to bracket "
                f"(tte={tte:.0f}s < {min_required}s required) — skipping"
            )
            sleep_until_next_contract()
            continue

        # Run the full bracket cycle (blocking until exit or cancellation)
        try:
            run_bracket_cycle(client, conn, state, ticker, expiry_ts)
        except KeyboardInterrupt:
            log.info("Keyboard interrupt — shutting down")
            break
        except Exception as e:
            log.error(f"[{ticker}] Unhandled error in bracket cycle: {e}", exc_info=True)
            state.reset()

        # After a cycle completes, sleep until the next contract boundary
        sleep_until_next_contract()

    log.info("Bracket scalper stopped.")
    conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")
