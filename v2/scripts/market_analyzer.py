"""
market_analyzer.py - Multi-Market Kalshi 15-min Crypto Edge Analysis

Fetches historical resolved contracts for BTC/ETH/XRP/SOL 15-min markets,
analyzes patterns, and produces a comprehensive edge report.

Runtime: ~1-2 hours (rate-limited API fetching), then instant analysis.
Output:  analysis/edge_report.txt  +  analysis/market_data.db

Run: python market_analyzer.py
"""

from __future__ import annotations

import base64
import json
import logging
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

SERIES_TO_ANALYZE = ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M", "KXDOGE15M", "KXBNB15M", "KXHYPE15M"]

MAX_CONTRACTS_PER_SERIES = 500
MAX_TRADES_PER_CONTRACT  = 100   # up to 100 trades per contract for price path
RATE_LIMIT_DELAY         = 0.40  # seconds between API calls

OUTPUT_DIR   = Path("./analysis")
DB_PATH      = OUTPUT_DIR / "market_data.db"
REPORT_PATH  = OUTPUT_DIR / "edge_report.txt"
LOG_PATH     = OUTPUT_DIR / "analyzer.log"

DEFAULT_KEY_PATH = "/opt/kalshi-bot/kalshi_private_key.pem"

# Bracket strategy parameters mirroring bracket_scalper.py
TAKE_PROFIT_CENTS = 56
STOP_LOSS_CENTS   = 20

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_dotenv(path: str = ".env") -> None:
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
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")

    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)

    logger = logging.getLogger("analyzer")
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    return logger


log: logging.Logger  # assigned in main()

# ---------------------------------------------------------------------------
# Kalshi Auth
# ---------------------------------------------------------------------------

class KalshiAuth:
    def __init__(self, api_key: str, private_key_path: str) -> None:
        self._api_key = api_key
        with open(private_key_path, "rb") as f:
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
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }

# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

class KalshiClient:
    def __init__(self, auth: KalshiAuth) -> None:
        self._auth = auth
        self._http = httpx.Client(timeout=15.0)

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = KALSHI_BASE_URL + path
        headers = self._auth.headers("GET", "/trade-api/v2" + path)
        r = self._http.get(url, headers=headers, params=params or {})
        r.raise_for_status()
        return r.json()

    def get_resolved_markets(
        self, series_ticker: str, cursor: str | None = None, limit: int = 100
    ) -> tuple[list[dict], str | None]:
        # No status filter — get all markets and filter by result in Python
        params: dict = {"series_ticker": series_ticker, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        data = self._get("/markets", params)
        return data.get("markets", []), data.get("cursor")

    def get_trades(
        self, ticker: str, limit: int = 100, cursor: str | None = None
    ) -> tuple[list[dict], str | None]:
        params: dict = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        try:
            data = self._get(f"/markets/{ticker}/trades", params)
        except httpx.HTTPStatusError:
            return [], None
        return data.get("trades", []), data.get("cursor")

    def close(self) -> None:
        self._http.close()

# ---------------------------------------------------------------------------
# SQLite Database
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS contracts (
    ticker          TEXT PRIMARY KEY,
    series          TEXT NOT NULL,
    open_ts         REAL,
    close_ts        REAL,
    result          TEXT,
    volume          INTEGER,
    open_interest   INTEGER,
    open_price      REAL,
    min_price       REAL,
    max_price       REAL,
    price_path_json TEXT,
    trades_fetched  INTEGER DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);
"""

def init_db() -> sqlite3.Connection:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def _parse_iso(s: str) -> float | None:
    if not s:
        return None
    try:
        s = s.rstrip("Z")
        if "+" in s:
            s = s.split("+")[0]
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


def upsert_contract(conn: sqlite3.Connection, series: str, market: dict) -> bool:
    """Insert contract if new. Returns True if inserted."""
    ticker = market.get("ticker", "")
    if not ticker:
        return False

    close_ts = _parse_iso(market.get("close_time") or market.get("expiration_time") or "")
    open_ts = (close_ts - 900) if close_ts else None
    result = (market.get("result") or "").lower()
    volume = market.get("volume") or 0
    oi = market.get("open_interest") or 0

    if conn.execute("SELECT 1 FROM contracts WHERE ticker=?", (ticker,)).fetchone():
        return False

    conn.execute(
        "INSERT OR IGNORE INTO contracts (ticker,series,open_ts,close_ts,result,volume,open_interest) "
        "VALUES (?,?,?,?,?,?,?)",
        (ticker, series, open_ts, close_ts, result, volume, oi),
    )
    conn.commit()
    return True


def save_price_path(
    conn: sqlite3.Connection,
    ticker: str,
    open_price: float | None,
    min_price: float | None,
    max_price: float | None,
    price_path: list[float],
) -> None:
    conn.execute(
        "UPDATE contracts SET open_price=?, min_price=?, max_price=?, price_path_json=?, trades_fetched=1 WHERE ticker=?",
        (open_price, min_price, max_price, json.dumps(price_path), ticker),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Phase 1 — Collect market metadata
# ---------------------------------------------------------------------------

def collect_markets(client: KalshiClient, conn: sqlite3.Connection) -> None:
    for series in SERIES_TO_ANALYZE:
        log.info(f"Fetching resolved markets for {series}...")
        cursor: str | None = None
        total = 0
        new = 0

        while total < MAX_CONTRACTS_PER_SERIES:
            try:
                markets, cursor = client.get_resolved_markets(series, cursor=cursor)
            except httpx.HTTPStatusError as e:
                code = e.response.status_code
                if code in (400, 404):
                    log.warning(f"  {series}: not found on Kalshi ({code}) — skipping")
                else:
                    log.error(f"  {series}: HTTP {code}")
                break
            except Exception as e:
                log.error(f"  {series}: {e}")
                break

            if not markets:
                break

            for m in markets:
                if upsert_contract(conn, series, m):
                    new += 1
                total += 1

            if not cursor or len(markets) < 100:
                break

            time.sleep(RATE_LIMIT_DELAY)

        log.info(f"  {series}: {total} contracts pulled, {new} new")
        time.sleep(RATE_LIMIT_DELAY)


# ---------------------------------------------------------------------------
# Phase 2 — Fetch trade price paths
# ---------------------------------------------------------------------------

def fetch_price_paths(client: KalshiClient, conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT ticker, open_ts FROM contracts WHERE trades_fetched=0 ORDER BY close_ts DESC"
    ).fetchall()

    total = len(rows)
    log.info(f"Fetching trade price paths for {total} contracts...")

    for i, (ticker, open_ts) in enumerate(rows, 1):
        if i % 100 == 0 or i == 1:
            log.info(f"  Price path progress: {i}/{total}")

        try:
            trades, _ = client.get_trades(ticker, limit=MAX_TRADES_PER_CONTRACT)
        except Exception as e:
            log.debug(f"  {ticker}: {e}")
            conn.execute("UPDATE contracts SET trades_fetched=1 WHERE ticker=?", (ticker,))
            conn.commit()
            time.sleep(RATE_LIMIT_DELAY)
            continue

        if not trades:
            conn.execute("UPDATE contracts SET trades_fetched=1 WHERE ticker=?", (ticker,))
            conn.commit()
            time.sleep(RATE_LIMIT_DELAY)
            continue

        # Extract yes_price values; Kalshi returns newest-first
        prices: list[float] = []
        timed: list[tuple[float, float]] = []  # (ts, price)

        for t in trades:
            p = t.get("yes_price")
            if p is None:
                continue
            p = float(p)
            prices.append(p)

            ts_raw = t.get("created_time") or ""
            ts = _parse_iso(ts_raw) or 0.0
            timed.append((ts, p))

        if not prices:
            conn.execute("UPDATE contracts SET trades_fetched=1 WHERE ticker=?", (ticker,))
            conn.commit()
            time.sleep(RATE_LIMIT_DELAY)
            continue

        # Sort ascending by timestamp to get chronological path
        timed.sort(key=lambda x: x[0])
        price_path = [p for _, p in timed]

        open_price = price_path[0] if price_path else None
        min_price  = min(prices)
        max_price  = max(prices)

        save_price_path(conn, ticker, open_price, min_price, max_price, price_path)
        time.sleep(RATE_LIMIT_DELAY)

    log.info("Price path fetch complete.")


# ---------------------------------------------------------------------------
# Phase 3 — Analysis
# ---------------------------------------------------------------------------

def bucket_label(price: float) -> str:
    b = int(price // 10) * 10
    b = max(0, min(90, b))
    return f"{b:02d}-{b+10:02d}"


def analyze_series(conn: sqlite3.Connection, series: str) -> dict:
    rows = conn.execute(
        """SELECT ticker, open_ts, close_ts, result, volume,
                  open_price, min_price, max_price, price_path_json
           FROM contracts
           WHERE series=? AND result IN ('yes','no')
           ORDER BY close_ts DESC""",
        (series,),
    ).fetchall()

    if not rows:
        return {"series": series, "count": 0}

    contracts = []
    for ticker, open_ts, close_ts, result, volume, open_price, min_price, max_price, path_json in rows:
        dt = datetime.fromtimestamp(open_ts, tz=timezone.utc) if open_ts else None

        # Parse price path
        path: list[float] = []
        if path_json:
            try:
                path = json.loads(path_json)
            except Exception:
                pass

        contracts.append({
            "ticker":      ticker,
            "result":      result,
            "volume":      volume or 0,
            "open_price":  open_price,
            "min_price":   min_price,
            "max_price":   max_price,
            "path":        path,
            "hour":        dt.hour if dt else None,
            "weekday":     dt.weekday() if dt else None,
            "swing":       (max_price - min_price) if (max_price is not None and min_price is not None) else None,
        })

    n = len(contracts)
    yes_wins = sum(1 for c in contracts if c["result"] == "yes")

    # ---- 1. Price bucket analysis ----
    # Bucket by opening price; compute YES win rate and EV per bucket
    bucket_raw: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "yes": 0, "volume": 0, "swings": [],
        "hit_tp": 0, "hit_sl": 0,
    })

    for c in contracts:
        if c["open_price"] is None:
            continue
        b = bucket_label(c["open_price"])
        bucket_raw[b]["count"] += 1
        bucket_raw[b]["volume"] += c["volume"]
        if c["result"] == "yes":
            bucket_raw[b]["yes"] += 1
        if c["swing"] is not None:
            bucket_raw[b]["swings"].append(c["swing"])
        if c["max_price"] is not None and c["max_price"] >= TAKE_PROFIT_CENTS:
            bucket_raw[b]["hit_tp"] += 1
        if c["min_price"] is not None and c["min_price"] <= STOP_LOSS_CENTS:
            bucket_raw[b]["hit_sl"] += 1

    price_buckets: dict[str, dict] = {}
    for b in sorted(bucket_raw.keys()):
        d = bucket_raw[b]
        cnt = d["count"]
        if cnt == 0:
            continue
        yes_rate = d["yes"] / cnt
        mid_cents = int(b.split("-")[0]) + 5
        no_rate = 1 - yes_rate

        # EV in cents: positive means you have an edge
        ev_yes = yes_rate * 100 - mid_cents
        ev_no  = no_rate  * 100 - (100 - mid_cents)

        avg_swing = sum(d["swings"]) / len(d["swings"]) if d["swings"] else 0.0
        tp_rate   = d["hit_tp"] / cnt
        sl_rate   = d["hit_sl"] / cnt
        # Bracket EV: +20c on TP, -16c on SL, 0 on time-stop (simplified)
        bracket_ev = tp_rate * 20 - sl_rate * 16

        price_buckets[b] = {
            "count":       cnt,
            "yes_rate":    round(yes_rate, 3),
            "ev_yes":      round(ev_yes, 2),
            "ev_no":       round(ev_no, 2),
            "avg_volume":  round(d["volume"] / cnt, 1),
            "avg_swing":   round(avg_swing, 1),
            "tp_rate":     round(tp_rate, 3),
            "sl_rate":     round(sl_rate, 3),
            "bracket_ev":  round(bracket_ev, 2),
        }

    # ---- 2. Bracket opportunity by entry price window ----
    # Look at specific entry prices (not just buckets) with ±5c window
    bracket_entries = [25, 30, 35, 36, 40, 45, 50, 55, 60, 65, 70]
    bracket_by_entry: dict[int, dict] = {}
    for entry in bracket_entries:
        lo, hi = entry - 5, entry + 5
        cands = [
            c for c in contracts
            if c["open_price"] is not None and lo <= c["open_price"] <= hi
        ]
        cnt = len(cands)
        if cnt < 5:
            bracket_by_entry[entry] = {"count": cnt}
            continue

        hit_tp = sum(1 for c in cands if c["max_price"] is not None and c["max_price"] >= TAKE_PROFIT_CENTS)
        hit_sl = sum(1 for c in cands if c["min_price"] is not None and c["min_price"] <= STOP_LOSS_CENTS)
        tp_rate = hit_tp / cnt
        sl_rate = hit_sl / cnt
        ev = round(tp_rate * 20 - sl_rate * 16, 2)

        # Also check: do markets that open near entry tend to swing toward TP or SL more?
        # If open < 50: TP means UP move (+20c swing needed)
        # Momentum: % that moved UP (YES won) from near-entry opening
        yes_rate_cands = sum(1 for c in cands if c["result"] == "yes") / cnt

        bracket_by_entry[entry] = {
            "count":    cnt,
            "tp_rate":  round(tp_rate, 3),
            "sl_rate":  round(sl_rate, 3),
            "ev_cents": ev,
            "yes_rate": round(yes_rate_cands, 3),
        }

    # ---- 3. Time-of-day analysis (UTC) ----
    hour_raw: dict[int, dict] = defaultdict(lambda: {"count": 0, "yes": 0, "volume": 0})
    for c in contracts:
        h = c["hour"]
        if h is None:
            continue
        hour_raw[h]["count"] += 1
        hour_raw[h]["volume"] += c["volume"]
        if c["result"] == "yes":
            hour_raw[h]["yes"] += 1

    hour_analysis: dict[int, dict] = {}
    for h in sorted(hour_raw.keys()):
        d = hour_raw[h]
        cnt = d["count"]
        yes_rate = d["yes"] / cnt if cnt else 0
        hour_analysis[h] = {
            "count":      cnt,
            "yes_rate":   round(yes_rate, 3),
            "avg_volume": round(d["volume"] / cnt, 1) if cnt else 0,
        }

    # ---- 4. Volume-as-signal ----
    # Do high-volume contracts have a different YES rate? (crowd wisdom)
    if contracts:
        vols = sorted(c["volume"] for c in contracts if c["volume"])
        if len(vols) >= 10:
            median_vol = vols[len(vols) // 2]
            hi_vol = [c for c in contracts if c["volume"] >= median_vol]
            lo_vol = [c for c in contracts if c["volume"] < median_vol]
            vol_signal = {
                "median_volume":  median_vol,
                "hi_vol_yes_rate": round(sum(1 for c in hi_vol if c["result"] == "yes") / len(hi_vol), 3) if hi_vol else None,
                "lo_vol_yes_rate": round(sum(1 for c in lo_vol if c["result"] == "yes") / len(lo_vol), 3) if lo_vol else None,
            }
        else:
            vol_signal = {}
    else:
        vol_signal = {}

    # ---- 5. Mean reversion vs momentum ----
    # For contracts opening >50¢ (YES favored), does YES win more often? (momentum)
    # Or does it mean-revert to ~50%?
    above_50 = [c for c in contracts if c["open_price"] is not None and c["open_price"] > 55]
    below_45 = [c for c in contracts if c["open_price"] is not None and c["open_price"] < 45]
    momentum_signal = {}
    if len(above_50) >= 10:
        yes_rate_above = sum(1 for c in above_50 if c["result"] == "yes") / len(above_50)
        momentum_signal["above_55_yes_rate"] = round(yes_rate_above, 3)
        momentum_signal["above_55_count"] = len(above_50)
        # If yes_rate > 0.55 → momentum; if ~0.50 → random; if < 0.45 → mean-reversion
    if len(below_45) >= 10:
        yes_rate_below = sum(1 for c in below_45 if c["result"] == "yes") / len(below_45)
        momentum_signal["below_45_yes_rate"] = round(yes_rate_below, 3)
        momentum_signal["below_45_count"] = len(below_45)

    # ---- 6. Swing / volatility summary ----
    swings = [c["swing"] for c in contracts if c["swing"] is not None]
    volatility = {
        "avg_swing":    round(sum(swings) / len(swings), 1) if swings else 0,
        "pct_swing_20": round(sum(1 for s in swings if s >= 20) / len(swings), 3) if swings else 0,
        "pct_swing_30": round(sum(1 for s in swings if s >= 30) / len(swings), 3) if swings else 0,
    }

    return {
        "series":           series,
        "count":            n,
        "overall_yes_rate": round(yes_wins / n, 3),
        "price_buckets":    price_buckets,
        "bracket_by_entry": bracket_by_entry,
        "hour_analysis":    hour_analysis,
        "vol_signal":       vol_signal,
        "momentum_signal":  momentum_signal,
        "volatility":       volatility,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(results: list[dict]) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: list[str] = []

    lines += [
        "=" * 72,
        "  KALSHI 15-MIN CRYPTO MARKETS — EDGE ANALYSIS REPORT",
        f"  Generated: {now}",
        "=" * 72,
        "",
        "  METHODOLOGY:",
        "  - Historical resolved contracts pulled from Kalshi API",
        "  - Opening price = first trade after contract open",
        "  - Price path = up to 100 trades sampled during contract life",
        "  - EV = expected value in cents per contract (positive = edge)",
        "  - Bracket EV = TP rate × +20c  −  SL rate × 16c  (TP=56c, SL=20c)",
        "",
    ]

    active = [r for r in results if r.get("count", 0) >= 10]

    for r in results:
        series = r["series"]
        lines += [f"\n{'─'*72}", f"  {series}", f"{'─'*72}"]

        if r.get("count", 0) < 10:
            lines.append(f"  Insufficient data ({r.get('count',0)} contracts)")
            continue

        lines += [
            f"  Contracts analyzed   : {r['count']}",
            f"  Overall YES rate     : {r['overall_yes_rate']:.1%}  (50% = fair)",
            f"  Avg price swing      : {r['volatility']['avg_swing']:.1f}c",
            f"  Contracts swing ≥20c : {r['volatility']['pct_swing_20']:.1%}",
            f"  Contracts swing ≥30c : {r['volatility']['pct_swing_30']:.1%}",
            "",
        ]

        # Price bucket table
        lines.append("  OPENING PRICE vs OUTCOME + BRACKET EV:")
        lines.append(
            f"  {'Bucket':>8}  {'N':>5}  {'YES%':>6}  {'EV(YES)':>8}  {'EV(NO)':>8}  {'AvgSwing':>9}  {'BracketEV':>10}"
        )
        lines.append(f"  {'─'*8}  {'─'*5}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*9}  {'─'*10}")

        for b, d in sorted(r["price_buckets"].items()):
            flag = ""
            if abs(d["ev_yes"]) >= 3 and d["count"] >= 20:
                flag = " ◄ YES EDGE" if d["ev_yes"] > 0 else " ◄ NO EDGE"
            elif d["bracket_ev"] >= 2 and d["count"] >= 10:
                flag = " ◄ BRACKET"
            lines.append(
                f"  {b:>8}  {d['count']:>5}  {d['yes_rate']:>5.1%}  "
                f"  {d['ev_yes']:>+6.1f}c  {d['ev_no']:>+6.1f}c  "
                f"{d['avg_swing']:>8.1f}c  {d['bracket_ev']:>+9.2f}c{flag}"
            )

        lines.append("")
        lines.append("  BRACKET STRATEGY (YES @ entry ±5c, TP=56c, SL=20c):")
        lines.append(
            f"  {'Entry':>6}  {'N':>5}  {'TP%':>6}  {'SL%':>6}  {'EV':>8}  {'YES%':>6}"
        )
        lines.append(f"  {'─'*6}  {'─'*5}  {'─'*6}  {'─'*6}  {'─'*8}  {'─'*6}")

        for entry, d in sorted(r["bracket_by_entry"].items()):
            if d.get("count", 0) < 5:
                lines.append(f"  {entry:>5}c  {'<5':>5}  {'—':>6}  {'—':>6}  {'—':>8}  {'—':>6}")
            else:
                flag = " ◄ EDGE" if d["ev_cents"] > 2 else ""
                lines.append(
                    f"  {entry:>5}c  {d['count']:>5}  {d['tp_rate']:>5.1%}  "
                    f"{d['sl_rate']:>5.1%}  {d['ev_cents']:>+7.2f}c  "
                    f"{d['yes_rate']:>5.1%}{flag}"
                )

        # Momentum / mean-reversion
        ms = r.get("momentum_signal", {})
        if ms:
            lines.append("")
            lines.append("  MOMENTUM vs MEAN-REVERSION:")
            if "above_55_yes_rate" in ms:
                yr = ms["above_55_yes_rate"]
                label = "MOMENTUM" if yr > 0.55 else ("MEAN-REVERT" if yr < 0.45 else "RANDOM")
                lines.append(
                    f"  Contracts opening >55c: YES wins {yr:.1%} of time  "
                    f"(n={ms['above_55_count']}) → {label}"
                )
            if "below_45_yes_rate" in ms:
                yr = ms["below_45_yes_rate"]
                label = "MEAN-REVERT" if yr > 0.55 else ("MOMENTUM" if yr < 0.45 else "RANDOM")
                lines.append(
                    f"  Contracts opening <45c: YES wins {yr:.1%} of time  "
                    f"(n={ms['below_45_count']}) → {label}"
                )

        # Volume signal
        vs = r.get("vol_signal", {})
        if vs and vs.get("hi_vol_yes_rate") is not None:
            lines.append("")
            lines.append("  VOLUME SIGNAL (crowd wisdom):")
            lines.append(f"  Median volume          : {vs['median_volume']}")
            lines.append(f"  High-vol YES rate      : {vs['hi_vol_yes_rate']:.1%}")
            lines.append(f"  Low-vol  YES rate      : {vs['lo_vol_yes_rate']:.1%}")

        # Time of day (grouped into 4-hour blocks)
        ha = r.get("hour_analysis", {})
        if ha:
            lines.append("")
            lines.append("  TIME-OF-DAY YES RATE (UTC, 4-hour blocks):")
            blocks: dict[int, dict] = defaultdict(lambda: {"count": 0, "yes_sum": 0})
            for h, d in ha.items():
                bl = (h // 4) * 4
                blocks[bl]["count"] += d["count"]
                blocks[bl]["yes_sum"] += round(d["yes_rate"] * d["count"])
            for bl in sorted(blocks.keys()):
                d = blocks[bl]
                cnt = d["count"]
                if cnt == 0:
                    continue
                yr = d["yes_sum"] / cnt
                flag = "  ← edge?" if abs(yr - 0.5) > 0.05 and cnt >= 15 else ""
                lines.append(
                    f"  {bl:02d}:00-{bl+4:02d}:00 UTC:  {yr:.1%} YES  (n={cnt}){flag}"
                )

    # Cross-asset summary
    lines += [f"\n{'─'*72}", "  CROSS-ASSET SUMMARY", f"{'─'*72}"]
    if active:
        lines.append(
            f"  {'Series':>12}  {'N':>5}  {'YES%':>6}  {'AvgSwing':>9}  {'Swing≥20':>9}"
        )
        for r in sorted(active, key=lambda x: -x["volatility"]["avg_swing"]):
            lines.append(
                f"  {r['series']:>12}  {r['count']:>5}  "
                f"{r['overall_yes_rate']:>5.1%}  "
                f"{r['volatility']['avg_swing']:>8.1f}c  "
                f"{r['volatility']['pct_swing_20']:>8.1%}"
            )
        best = max(active, key=lambda x: x["volatility"]["avg_swing"])
        lines.append(f"\n  Most volatile (best for bracket strategy): {best['series']}")

        # Best EV plays across all series
        lines.append("\n  TOP EV PLAYS FOUND:")
        found_any = False
        for r in active:
            for b, d in r["price_buckets"].items():
                if abs(d["ev_yes"]) >= 3 and d["count"] >= 20:
                    side = "BUY YES" if d["ev_yes"] > 0 else "BUY NO"
                    ev = d["ev_yes"] if d["ev_yes"] > 0 else d["ev_no"]
                    lines.append(
                        f"  {r['series']} opening {b}c → {side}  "
                        f"EV={ev:+.1f}c/trade  (n={d['count']})"
                    )
                    found_any = True
            for entry, d in r["bracket_by_entry"].items():
                if d.get("count", 0) >= 10 and d.get("ev_cents", 0) > 2:
                    lines.append(
                        f"  {r['series']} bracket @ {entry}c  "
                        f"EV={d['ev_cents']:+.2f}c  TP={d['tp_rate']:.1%}  SL={d['sl_rate']:.1%}  (n={d['count']})"
                    )
                    found_any = True
        if not found_any:
            lines.append("  None found with n≥20 and EV≥3c. See per-series tables above for smaller samples.")
    else:
        lines.append("  No series with sufficient data.")

    lines += ["", "=" * 72, "  END OF REPORT", "=" * 72]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global log
    _load_dotenv(".env")
    log = setup_logging()

    log.info("=" * 60)
    log.info("Market Analyzer — Kalshi 15-min Crypto  [4 series]")
    log.info("=" * 60)

    api_key  = os.environ.get("KALSHI_API_KEY", "b16ab35a-2d26-4bd3-93c6-a1e8fd6c7a95")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)

    try:
        auth = KalshiAuth(api_key, key_path)
        log.info("RSA private key loaded OK")
    except FileNotFoundError as e:
        log.error(str(e))
        sys.exit(1)

    client = KalshiClient(auth)
    conn   = init_db()
    log.info(f"Database: {DB_PATH}")

    # Phase 1 — market metadata
    log.info("─" * 40)
    log.info("Phase 1: Collecting resolved market metadata...")
    collect_markets(client, conn)

    # Phase 2 — analysis + report
    log.info("─" * 40)
    log.info("Phase 2: Running analysis...")
    results = [analyze_series(conn, s) for s in SERIES_TO_ANALYZE]
    for r in results:
        log.info(f"  {r['series']}: {r['count']} contracts analyzed")

    report = generate_report(results)
    print("\n" + report)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    log.info(f"Report saved → {REPORT_PATH}")

    # Also dump raw JSON for deeper inspection
    json_path = OUTPUT_DIR / "results.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    log.info(f"Raw JSON   → {json_path}")

    conn.close()
    client.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
