"""
historical_analysis.py - Kalshi 15-min Crypto Historical Edge Analysis

Fully automated. Run once and leave it overnight:
    python historical_analysis.py

What it does:
  Phase 1 - Pulls up to 3000 resolved contracts per series from Kalshi API
             (YES/NO results + timestamps)
  Phase 2 - Downloads 6 months of Kalshi public daily dump files,
             extracts high/low price range per contract, caches filtered data
  Phase 3 - Joins API results with dump price ranges
  Phase 4 - Full analysis: time-of-day edges, bracket EV, momentum, cross-asset
  Phase 5 - Writes edge_report.txt

Output folder: analysis/
  analysis/edge_report.txt      <- main report
  analysis/market_data.db       <- SQLite database
  analysis/dumps/               <- cached filtered dump data (small files)
  analysis/analyzer.log         <- progress log
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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SERIES = [
    "KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M",
    "KXDOGE15M", "KXBNB15M", "KXHYPE15M",
]

MAX_CONTRACTS_PER_SERIES = 3000   # ~31 days of data
DUMP_DAYS                = 180    # how many days of dump files to scan
RATE_LIMIT_DELAY         = 0.35   # seconds between Kalshi API calls

DUMP_URL  = "https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_{date}.json"

OUTPUT_DIR = Path("./analysis")
DUMPS_DIR  = OUTPUT_DIR / "dumps"
DB_PATH    = OUTPUT_DIR / "market_data.db"
REPORT_PATH= OUTPUT_DIR / "edge_report.txt"
LOG_PATH   = OUTPUT_DIR / "analyzer.log"

TAKE_PROFIT = 56
STOP_LOSS   = 20

DEFAULT_KEY_PATH = "./kalshi_private_key.pem"

# ---------------------------------------------------------------------------
# Setup
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
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def setup_logging() -> logging.Logger:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DUMPS_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger = logging.getLogger("hist")
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    return logger


log: logging.Logger

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
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS contracts (
    ticker       TEXT PRIMARY KEY,
    series       TEXT NOT NULL,
    open_ts      REAL,
    close_ts     REAL,
    result       TEXT,
    volume       INTEGER,
    open_price   REAL,
    high_price   REAL,
    low_price    REAL,
    api_fetched  INTEGER DEFAULT 0,
    dump_fetched INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS dump_progress (
    date_str TEXT PRIMARY KEY,
    records  INTEGER,
    done     INTEGER DEFAULT 1
);
"""

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


def _parse_ts(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.rstrip("Z").split("+")[0]).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Phase 1 — API: pull resolved contracts
# ---------------------------------------------------------------------------

def phase1_api(auth: KalshiAuth, conn: sqlite3.Connection) -> None:
    log.info("=" * 60)
    log.info("Phase 1: Pulling resolved contracts from Kalshi API")
    log.info("=" * 60)

    http = httpx.Client(timeout=15.0)
    base = "https://api.elections.kalshi.com/trade-api/v2"

    for series in SERIES:
        existing = conn.execute(
            "SELECT COUNT(*) FROM contracts WHERE series=? AND api_fetched=1", (series,)
        ).fetchone()[0]
        log.info(f"  {series}: {existing} already in DB, pulling up to {MAX_CONTRACTS_PER_SERIES}...")

        cursor: str | None = None
        total = 0
        new = 0

        while total < MAX_CONTRACTS_PER_SERIES:
            path = "/trade-api/v2/markets"
            params: dict = {"series_ticker": series, "limit": 100}
            if cursor:
                params["cursor"] = cursor

            try:
                r = http.get(base + "/markets", headers=auth.headers("GET", path), params=params)
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                log.warning(f"    HTTP {e.response.status_code} — stopping pagination for {series}")
                break
            except Exception as e:
                log.warning(f"    Error: {e} — stopping")
                break

            data = r.json()
            markets = data.get("markets", [])
            cursor = data.get("cursor")

            if not markets:
                break

            for m in markets:
                ticker = m.get("ticker", "")
                result = (m.get("result") or "").lower()
                if result not in ("yes", "no"):
                    continue

                close_ts = _parse_ts(m.get("close_time") or m.get("expiration_time"))
                open_ts = (close_ts - 900) if close_ts else None
                volume = m.get("volume") or 0

                exists = conn.execute("SELECT 1 FROM contracts WHERE ticker=?", (ticker,)).fetchone()
                if not exists:
                    conn.execute(
                        "INSERT OR IGNORE INTO contracts "
                        "(ticker,series,open_ts,close_ts,result,volume,api_fetched) "
                        "VALUES (?,?,?,?,?,?,1)",
                        (ticker, series, open_ts, close_ts, result, volume, ),
                    )
                    new += 1
                total += 1

            conn.commit()

            if not cursor or len(markets) < 100:
                break

            time.sleep(RATE_LIMIT_DELAY)

        settled = conn.execute(
            "SELECT COUNT(*) FROM contracts WHERE series=? AND result IN ('yes','no')", (series,)
        ).fetchone()[0]
        log.info(f"    {series}: {new} new contracts added, {settled} total settled")
        time.sleep(RATE_LIMIT_DELAY)

    http.close()
    total_settled = conn.execute(
        "SELECT COUNT(*) FROM contracts WHERE result IN ('yes','no')"
    ).fetchone()[0]
    log.info(f"Phase 1 complete. Total settled contracts: {total_settled}")


# ---------------------------------------------------------------------------
# Phase 2 — Dump files: extract high/low price ranges
# ---------------------------------------------------------------------------

def phase2_dumps(conn: sqlite3.Connection) -> None:
    log.info("=" * 60)
    log.info("Phase 2: Downloading Kalshi daily dump files for price ranges")
    log.info(f"         Scanning {DUMP_DAYS} days back")
    log.info("=" * 60)

    # Build set of tickers we need price data for
    need_tickers: dict[str, str] = {}  # ticker -> series
    rows = conn.execute(
        "SELECT ticker, series FROM contracts WHERE result IN ('yes','no') AND dump_fetched=0"
    ).fetchall()
    for ticker, series in rows:
        need_tickers[ticker] = series

    log.info(f"  Need price data for {len(need_tickers)} contracts")
    if not need_tickers:
        log.info("  All contracts already have dump data — skipping")
        return

    today = datetime.now(tz=timezone.utc).date()
    found_total = 0

    with httpx.Client(timeout=120.0, follow_redirects=True) as http:
        for i in range(DUMP_DAYS):
            date = today - timedelta(days=i)
            date_str = date.strftime("%Y-%m-%d")

            # Skip if already processed
            done = conn.execute(
                "SELECT done FROM dump_progress WHERE date_str=?", (date_str,)
            ).fetchone()
            if done:
                continue

            # Check if we still need contracts from this date range
            # Contracts from this date would have close_ts on this day
            day_start = datetime(date.year, date.month, date.day, tzinfo=timezone.utc).timestamp()
            day_end   = day_start + 86400
            needed_today = conn.execute(
                "SELECT COUNT(*) FROM contracts WHERE dump_fetched=0 "
                "AND close_ts >= ? AND close_ts < ?", (day_start, day_end)
            ).fetchone()[0]

            if needed_today == 0:
                # Mark done even if no contracts needed
                conn.execute(
                    "INSERT OR REPLACE INTO dump_progress (date_str, records) VALUES (?,0)",
                    (date_str,)
                )
                conn.commit()
                continue

            # Check cached filtered file first
            cache_file = DUMPS_DIR / f"{date_str}_crypto.json"
            if cache_file.exists():
                log.info(f"  {date_str}: using cached filtered data")
                with open(cache_file) as f:
                    crypto_records = json.load(f)
            else:
                url = DUMP_URL.format(date=date_str)
                log.info(f"  {date_str}: downloading dump ({needed_today} contracts needed)...")
                try:
                    r = http.get(url)
                    if r.status_code == 404:
                        log.info(f"  {date_str}: not available (404)")
                        conn.execute(
                            "INSERT OR REPLACE INTO dump_progress (date_str, records) VALUES (?,0)",
                            (date_str,)
                        )
                        conn.commit()
                        continue
                    r.raise_for_status()
                except Exception as e:
                    log.warning(f"  {date_str}: download error — {e}")
                    continue

                log.info(f"  {date_str}: parsing {len(r.content)/1024/1024:.0f}MB...")
                try:
                    all_records = r.json()
                except Exception as e:
                    log.warning(f"  {date_str}: JSON parse error — {e}")
                    continue

                # Filter to crypto 15m only
                crypto_records = []
                for rec in all_records:
                    tn = str(rec.get("ticker_name") or rec.get("ticker") or "")
                    rt = str(rec.get("report_ticker") or "")
                    if any(tn.startswith(s) or rt == s for s in SERIES):
                        crypto_records.append(rec)

                # Also check looser match in case naming differs
                if not crypto_records:
                    for rec in all_records:
                        tn = str(rec.get("ticker_name") or rec.get("ticker") or "")
                        rt = str(rec.get("report_ticker") or "")
                        combined = tn + rt
                        if any(s[:6] in combined for s in SERIES):
                            crypto_records.append(rec)

                del all_records  # free RAM

                # Cache the filtered result
                with open(cache_file, "w") as f:
                    json.dump(crypto_records, f)

                log.info(f"  {date_str}: found {len(crypto_records)} crypto 15m records")

            # Match dump records to DB contracts
            found_today = 0
            for rec in crypto_records:
                tn = str(rec.get("ticker_name") or rec.get("ticker") or "")
                high = rec.get("high")
                low  = rec.get("low")

                if tn in need_tickers and high is not None and low is not None:
                    conn.execute(
                        "UPDATE contracts SET high_price=?, low_price=?, dump_fetched=1 WHERE ticker=?",
                        (float(high), float(low), tn)
                    )
                    found_today += 1
                    found_total += 1

            conn.execute(
                "INSERT OR REPLACE INTO dump_progress (date_str, records) VALUES (?,?)",
                (date_str, len(crypto_records))
            )
            conn.commit()

            if found_today > 0:
                log.info(f"  {date_str}: matched {found_today} contracts with price ranges")

    log.info(f"Phase 2 complete. Total price ranges added: {found_total}")

    # Report coverage
    with_prices = conn.execute(
        "SELECT COUNT(*) FROM contracts WHERE result IN ('yes','no') AND high_price IS NOT NULL"
    ).fetchone()[0]
    total = conn.execute(
        "SELECT COUNT(*) FROM contracts WHERE result IN ('yes','no')"
    ).fetchone()[0]
    log.info(f"Price range coverage: {with_prices}/{total} contracts ({with_prices/total*100:.0f}%)")


# ---------------------------------------------------------------------------
# Phase 3 — Analysis
# ---------------------------------------------------------------------------

def analyze_series(conn: sqlite3.Connection, series: str) -> dict:
    rows = conn.execute(
        """SELECT ticker, open_ts, result, volume, open_price, high_price, low_price
           FROM contracts
           WHERE series=? AND result IN ('yes','no')
           ORDER BY close_ts DESC""",
        (series,),
    ).fetchall()

    if len(rows) < 10:
        return {"series": series, "count": len(rows)}

    contracts = []
    for ticker, open_ts, result, volume, open_price, high_price, low_price in rows:
        dt = datetime.fromtimestamp(open_ts, tz=timezone.utc) if open_ts else None
        swing = (high_price - low_price) if (high_price is not None and low_price is not None) else None
        contracts.append({
            "ticker":     ticker,
            "result":     result,
            "volume":     volume or 0,
            "open_price": open_price,
            "high":       high_price,
            "low":        low_price,
            "swing":      swing,
            "hour":       dt.hour if dt else None,
            "weekday":    dt.weekday() if dt else None,
        })

    n = len(contracts)
    yes_wins = sum(1 for c in contracts if c["result"] == "yes")

    # ---- Time of day ----
    hour_raw: dict[int, dict] = defaultdict(lambda: {"count": 0, "yes": 0})
    for c in contracts:
        h = c["hour"]
        if h is None:
            continue
        hour_raw[h]["count"] += 1
        if c["result"] == "yes":
            hour_raw[h]["yes"] += 1

    hour_analysis = {
        h: {"count": d["count"], "yes_rate": round(d["yes"] / d["count"], 3)}
        for h, d in hour_raw.items() if d["count"] > 0
    }

    # ---- Price bucket analysis ----
    contracts_with_open = [c for c in contracts if c["open_price"] is not None]
    bucket_raw: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "yes": 0, "hit_tp": 0, "hit_sl": 0, "swings": []
    })
    for c in contracts_with_open:
        b_floor = int(c["open_price"] // 10) * 10
        b = f"{b_floor:02d}-{b_floor+10:02d}"
        bucket_raw[b]["count"] += 1
        if c["result"] == "yes":
            bucket_raw[b]["yes"] += 1
        if c["high"] is not None and c["high"] >= TAKE_PROFIT:
            bucket_raw[b]["hit_tp"] += 1
        if c["low"] is not None and c["low"] <= STOP_LOSS:
            bucket_raw[b]["hit_sl"] += 1
        if c["swing"] is not None:
            bucket_raw[b]["swings"].append(c["swing"])

    price_buckets: dict[str, dict] = {}
    for b in sorted(bucket_raw.keys()):
        d = bucket_raw[b]
        cnt = d["count"]
        if cnt < 5:
            continue
        yes_rate  = d["yes"] / cnt
        mid       = int(b.split("-")[0]) + 5
        ev_yes    = yes_rate * 100 - mid
        ev_no     = (1 - yes_rate) * 100 - (100 - mid)
        tp_rate   = d["hit_tp"] / cnt
        sl_rate   = d["hit_sl"] / cnt
        avg_swing = sum(d["swings"]) / len(d["swings"]) if d["swings"] else 0
        price_buckets[b] = {
            "count":      cnt,
            "yes_rate":   round(yes_rate, 3),
            "ev_yes":     round(ev_yes, 2),
            "ev_no":      round(ev_no, 2),
            "tp_rate":    round(tp_rate, 3),
            "sl_rate":    round(sl_rate, 3),
            "bracket_ev": round(tp_rate * 20 - sl_rate * 16, 2),
            "avg_swing":  round(avg_swing, 1),
        }

    # ---- Bracket by entry ----
    bracket_by_entry: dict[int, dict] = {}
    for entry in [25, 30, 35, 36, 40, 45, 50, 55, 60, 65, 70]:
        cands = [c for c in contracts_with_open if abs(c["open_price"] - entry) <= 5]
        cnt = len(cands)
        if cnt < 5:
            bracket_by_entry[entry] = {"count": cnt}
            continue
        hit_tp = sum(1 for c in cands if c["high"] and c["high"] >= TAKE_PROFIT)
        hit_sl = sum(1 for c in cands if c["low"] and c["low"] <= STOP_LOSS)
        tp_rate = hit_tp / cnt
        sl_rate = hit_sl / cnt
        bracket_by_entry[entry] = {
            "count":    cnt,
            "tp_rate":  round(tp_rate, 3),
            "sl_rate":  round(sl_rate, 3),
            "ev_cents": round(tp_rate * 20 - sl_rate * 16, 2),
            "yes_rate": round(sum(1 for c in cands if c["result"] == "yes") / cnt, 3),
        }

    # ---- Momentum ----
    above_55 = [c for c in contracts_with_open if c["open_price"] > 55]
    below_45 = [c for c in contracts_with_open if c["open_price"] < 45]
    momentum: dict[str, dict] = {}
    if len(above_55) >= 10:
        yr = sum(1 for c in above_55 if c["result"] == "yes") / len(above_55)
        momentum["above_55"] = {
            "count": len(above_55), "yes_rate": round(yr, 3),
            "signal": "MOMENTUM" if yr > 0.55 else ("MEAN-REVERT" if yr < 0.45 else "RANDOM"),
        }
    if len(below_45) >= 10:
        yr = sum(1 for c in below_45 if c["result"] == "yes") / len(below_45)
        momentum["below_45"] = {
            "count": len(below_45), "yes_rate": round(yr, 3),
            "signal": "MEAN-REVERT" if yr > 0.55 else ("MOMENTUM" if yr < 0.45 else "RANDOM"),
        }

    # ---- Volatility ----
    swings = [c["swing"] for c in contracts if c["swing"] is not None]
    volatility = {
        "avg_swing":      round(sum(swings) / len(swings), 1) if swings else 0,
        "pct_swing_20":   round(sum(1 for s in swings if s >= 20) / len(swings), 3) if swings else 0,
        "price_coverage": f"{len(contracts_with_open)}/{n}",
    }

    return {
        "series":           series,
        "count":            n,
        "overall_yes_rate": round(yes_wins / n, 3),
        "hour_analysis":    hour_analysis,
        "price_buckets":    price_buckets,
        "bracket_by_entry": bracket_by_entry,
        "momentum":         momentum,
        "volatility":       volatility,
    }


# ---------------------------------------------------------------------------
# Phase 4 — Report
# ---------------------------------------------------------------------------

def generate_report(results: list[dict]) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    L: list[str] = []

    L += [
        "=" * 72,
        "  KALSHI 15-MIN CRYPTO — EDGE ANALYSIS REPORT",
        f"  Generated : {now}",
        f"  Series    : {', '.join(SERIES)}",
        f"  API depth : up to {MAX_CONTRACTS_PER_SERIES} contracts per series (~{MAX_CONTRACTS_PER_SERIES//96} days)",
        f"  Dump depth: {DUMP_DAYS} days of daily files for price ranges",
        "=" * 72,
        "",
    ]

    active = [r for r in results if r.get("count", 0) >= 10]

    for r in results:
        series = r["series"]
        L += [f"\n{'─'*72}", f"  {series}", f"{'─'*72}"]

        if r.get("count", 0) < 10:
            L.append(f"  Insufficient data ({r.get('count',0)} contracts)")
            continue

        v = r["volatility"]
        L += [
            f"  Contracts analyzed : {r['count']}",
            f"  Overall YES rate   : {r['overall_yes_rate']:.1%}  (50.0% = fair)",
            f"  Price coverage     : {v['price_coverage']} contracts have high/low data",
            f"  Avg price swing    : {v['avg_swing']:.1f}c  |  Swing ≥20c: {v['pct_swing_20']:.1%}",
            "",
        ]

        # Momentum
        m = r.get("momentum", {})
        if m:
            L.append("  MOMENTUM / MEAN-REVERSION:")
            for key, d in m.items():
                label = ">55c" if key == "above_55" else "<45c"
                L.append(f"  Open {label} → YES wins {d['yes_rate']:.1%}  (n={d['count']})  [{d['signal']}]")
            L.append("")

        # Time of day
        ha = r.get("hour_analysis", {})
        if ha:
            L.append("  TIME-OF-DAY  (UTC 4-hour blocks):")
            blocks: dict[int, dict] = defaultdict(lambda: {"count": 0, "yes": 0})
            for h, d in ha.items():
                bl = (h // 4) * 4
                blocks[bl]["count"] += d["count"]
                blocks[bl]["yes"] += round(d["yes_rate"] * d["count"])
            for bl in sorted(blocks.keys()):
                d = blocks[bl]
                cnt = d["count"]
                if cnt == 0:
                    continue
                yr = d["yes"] / cnt
                bar = "█" * int(yr * 20)
                flag = "  ◄ YES EDGE" if yr > 0.56 and cnt >= 20 else \
                       "  ◄ NO EDGE"  if yr < 0.44 and cnt >= 20 else ""
                L.append(
                    f"  {bl:02d}:00-{bl+4:02d}:00 UTC  "
                    f"{yr:.1%} YES  (n={cnt:>4})  {bar}{flag}"
                )
            L.append("")

        # Price bucket table
        if r["price_buckets"]:
            L.append("  OPENING PRICE vs OUTCOME:")
            L.append(f"  {'Bucket':>8}  {'N':>5}  {'YES%':>6}  {'EV(YES)':>8}  {'EV(NO)':>8}  {'Swing':>7}  {'BracketEV':>10}")
            L.append(f"  {'─'*8}  {'─'*5}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*7}  {'─'*10}")
            for b, d in sorted(r["price_buckets"].items()):
                flag = ""
                if d["count"] >= 15 and d["ev_yes"] >= 3:
                    flag = "  ◄ YES EDGE"
                elif d["count"] >= 15 and d["ev_no"] >= 3:
                    flag = "  ◄ NO EDGE"
                elif d["count"] >= 10 and d["bracket_ev"] >= 2:
                    flag = "  ◄ BRACKET"
                L.append(
                    f"  {b:>8}  {d['count']:>5}  {d['yes_rate']:>5.1%}  "
                    f"  {d['ev_yes']:>+6.1f}c  {d['ev_no']:>+6.1f}c  "
                    f"{d['avg_swing']:>6.1f}c  {d['bracket_ev']:>+9.2f}c{flag}"
                )
            L.append("")

        # Bracket analysis
        has_bracket = any(d.get("count", 0) >= 5 for d in r["bracket_by_entry"].values())
        if has_bracket:
            L.append("  BRACKET STRATEGY  (TP=56c, SL=20c):")
            L.append(f"  {'Entry':>6}  {'N':>5}  {'TP%':>6}  {'SL%':>6}  {'EV/trade':>9}  {'YES%':>6}")
            L.append(f"  {'─'*6}  {'─'*5}  {'─'*6}  {'─'*6}  {'─'*9}  {'─'*6}")
            for entry, d in sorted(r["bracket_by_entry"].items()):
                if d.get("count", 0) < 5:
                    continue
                flag = "  ◄ POSITIVE EV" if d.get("ev_cents", 0) > 2 else ""
                L.append(
                    f"  {entry:>5}c  {d['count']:>5}  {d['tp_rate']:>5.1%}  "
                    f"{d['sl_rate']:>5.1%}  {d['ev_cents']:>+8.2f}c  {d['yes_rate']:>5.1%}{flag}"
                )
            L.append("")

    # Cross-asset summary
    L += [f"\n{'─'*72}", "  CROSS-ASSET SUMMARY", f"{'─'*72}"]
    if active:
        L.append(f"  {'Series':>12}  {'N':>5}  {'YES%':>6}  {'AvgSwing':>9}  {'Swing≥20':>9}")
        for r in sorted(active, key=lambda x: -x["volatility"]["avg_swing"]):
            L.append(
                f"  {r['series']:>12}  {r['count']:>5}  {r['overall_yes_rate']:>5.1%}  "
                f"{r['volatility']['avg_swing']:>8.1f}c  {r['volatility']['pct_swing_20']:>8.1%}"
            )

        # Top edge plays
        L.append("\n  TOP EV PLAYS  (n≥15, |EV|≥3c):")
        found = False
        for r in active:
            for b, d in r.get("price_buckets", {}).items():
                if d["count"] >= 15:
                    if d["ev_yes"] >= 3:
                        L.append(f"  {r['series']:>12}  open {b}c → BUY YES  EV={d['ev_yes']:+.1f}c  (n={d['count']})")
                        found = True
                    elif d["ev_no"] >= 3:
                        L.append(f"  {r['series']:>12}  open {b}c → BUY NO   EV={d['ev_no']:+.1f}c  (n={d['count']})")
                        found = True
            for entry, d in r.get("bracket_by_entry", {}).items():
                if d.get("count", 0) >= 15 and d.get("ev_cents", 0) > 2:
                    L.append(
                        f"  {r['series']:>12}  bracket@{entry}c  "
                        f"EV={d['ev_cents']:+.2f}c  TP={d['tp_rate']:.1%} SL={d['sl_rate']:.1%}  (n={d['count']})"
                    )
                    found = True
        if not found:
            L.append("  None found with sufficient sample yet — check per-series tables above.")

        L.append("\n  TIME-OF-DAY EDGE SUMMARY  (n≥20, deviation >6% from 50%):")
        found_tod = False
        for r in active:
            ha = r.get("hour_analysis", {})
            blocks: dict[int, dict] = defaultdict(lambda: {"count": 0, "yes": 0})
            for h, d in ha.items():
                bl = (h // 4) * 4
                blocks[bl]["count"] += d["count"]
                blocks[bl]["yes"] += round(d["yes_rate"] * d["count"])
            for bl in sorted(blocks.keys()):
                d = blocks[bl]
                cnt = d["count"]
                if cnt < 20:
                    continue
                yr = d["yes"] / cnt
                if yr > 0.56:
                    L.append(f"  {r['series']:>12}  {bl:02d}:00-{bl+4:02d}:00 UTC → BUY YES  {yr:.1%}  (n={cnt})")
                    found_tod = True
                elif yr < 0.44:
                    L.append(f"  {r['series']:>12}  {bl:02d}:00-{bl+4:02d}:00 UTC → BUY NO   {yr:.1%}  (n={cnt})")
                    found_tod = True
        if not found_tod:
            L.append("  None with n≥20 yet.")

    L += ["", "=" * 72, "  END OF REPORT", "=" * 72]
    return "\n".join(L)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global log
    _load_dotenv(".env")
    log = setup_logging()

    log.info("=" * 60)
    log.info("Kalshi Historical Analysis — Starting")
    log.info(f"Series: {', '.join(SERIES)}")
    log.info("=" * 60)

    api_key  = os.environ.get("KALSHI_API_KEY", "b16ab35a-2d26-4bd3-93c6-a1e8fd6c7a95")
    key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH", DEFAULT_KEY_PATH)

    try:
        auth = KalshiAuth(api_key, key_path)
        log.info("RSA key loaded OK")
    except Exception as e:
        log.error(f"Key error: {e}")
        sys.exit(1)

    conn = init_db()
    log.info(f"Database: {DB_PATH}")

    # Phase 1 — API contracts
    phase1_api(auth, conn)

    # Phase 2 — Dump price ranges
    phase2_dumps(conn)

    # Phase 3 — Analyze
    log.info("=" * 60)
    log.info("Phase 3: Running analysis...")
    log.info("=" * 60)
    results = []
    for series in SERIES:
        r = analyze_series(conn, series)
        log.info(f"  {series}: {r.get('count', 0)} contracts, "
                 f"price coverage={r.get('volatility', {}).get('price_coverage', 'N/A')}")
        results.append(r)

    # Phase 4 — Report
    report = generate_report(results)
    print("\n" + report)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    log.info(f"\nReport saved → {REPORT_PATH}")

    with open(OUTPUT_DIR / "results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    conn.close()
    log.info("Done.")


if __name__ == "__main__":
    main()
