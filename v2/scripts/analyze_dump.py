"""
analyze_dump.py - Download Kalshi daily market dumps and analyze crypto 15-min contracts.

Kalshi publishes daily JSON dumps at:
  https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_YYYY-MM-DD.json

No auth required. Downloads last N days, extracts all crypto 15-min contracts,
analyzes price paths, and writes edge_report.txt.

Run: python analyze_dump.py
"""

from __future__ import annotations

import json
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DUMP_BASE_URL  = "https://kalshi-public-docs.s3.amazonaws.com/reporting/market_data_{date}.json"
DAYS_TO_FETCH  = 5           # go back this many days
OUTPUT_DIR     = Path("./analysis")
REPORT_PATH    = OUTPUT_DIR / "edge_report.txt"
RAW_DIR        = OUTPUT_DIR / "dumps"       # cache downloaded files
LOG_PATH       = OUTPUT_DIR / "analyzer.log"

# All crypto 15-min series to look for (we'll discover which exist)
CRYPTO_PREFIXES = ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M",
                   "KXDOGE15M", "KXBNB15M", "KXHYPE15M"]

TAKE_PROFIT_CENTS = 56
STOP_LOSS_CENTS   = 20

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging() -> logging.Logger:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger = logging.getLogger("dump_analyzer")
    logger.setLevel(logging.INFO)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    return logger

log: logging.Logger

# ---------------------------------------------------------------------------
# Download dumps
# ---------------------------------------------------------------------------

def download_dump(date_str: str) -> list[dict] | None:
    """Download and cache one day's dump. Returns list of market dicts or None."""
    cache_path = RAW_DIR / f"{date_str}.json"

    url = DUMP_BASE_URL.format(date=date_str)
    try:
        r = httpx.get(url, timeout=120.0, follow_redirects=True)
        if r.status_code == 404:
            log.info(f"  {date_str}: no dump available (404)")
            return None
        r.raise_for_status()
    except Exception as e:
        log.warning(f"  {date_str}: download failed — {e}")
        return None

    data = r.json()
    if isinstance(data, list):
        markets = data
    elif isinstance(data, dict):
        markets = (data.get("markets") or data.get("data") or
                   data.get("market_data") or list(data.values())[0] if data else [])
        if not isinstance(markets, list):
            markets = [data]
    else:
        markets = []

    log.info(f"  {date_str}: downloaded {len(markets)} markets")
    return markets


def fetch_all_dumps() -> list[dict]:
    """Download last DAYS_TO_FETCH days, extract only crypto contracts, discard raw data."""
    all_markets: list[dict] = []
    today = datetime.now(tz=timezone.utc).date()

    log.info(f"Downloading {DAYS_TO_FETCH} days of Kalshi market dumps...")
    for i in range(DAYS_TO_FETCH):
        date = today - timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        markets = download_dump(date_str)
        if markets:
            # Only keep crypto 15-min markets to save memory
            crypto = [
                m for m in markets
                if any(
                    (m.get("ticker") or m.get("market_ticker") or "").startswith(p)
                    for p in CRYPTO_PREFIXES
                )
            ]
            log.info(f"  {date_str}: {len(markets)} total, {len(crypto)} crypto 15m")
            all_markets.extend(crypto)
            del markets  # free RAM immediately
        time.sleep(0.1)

    log.info(f"Total crypto records: {len(all_markets)}")
    return all_markets

# ---------------------------------------------------------------------------
# Parse + filter
# ---------------------------------------------------------------------------

def _parse_ts(s: str | None) -> float | None:
    if not s:
        return None
    try:
        s = s.rstrip("Z").split("+")[0]
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


def extract_crypto_contracts(all_markets: list[dict]) -> dict[str, list[dict]]:
    """
    Filter to crypto 15-min contracts with settled results.
    Returns {series_ticker: [contract_dict, ...]}
    """
    # First pass: print all unique series tickers we find to help discovery
    all_tickers = set()
    for m in all_markets:
        t = m.get("ticker") or m.get("market_ticker") or ""
        if t:
            # Extract series prefix (everything before the date part)
            parts = t.split("-")
            if parts:
                all_tickers.add(parts[0])

    crypto_tickers = [t for t in sorted(all_tickers)
                      if any(c in t for c in ["BTC", "ETH", "SOL", "XRP", "DOGE", "BNB", "HYPE"])]
    log.info(f"Crypto series found in dumps: {crypto_tickers}")

    by_series: dict[str, list[dict]] = defaultdict(list)

    for m in all_markets:
        ticker = m.get("ticker") or m.get("market_ticker") or ""
        if not ticker:
            continue

        # Match against crypto prefixes
        series = None
        for prefix in CRYPTO_PREFIXES + crypto_tickers:
            if ticker.startswith(prefix):
                series = prefix
                break
        if not series:
            continue

        result = (m.get("result") or m.get("outcome") or "").lower()
        if result not in ("yes", "no"):
            continue

        # Parse timestamps
        close_ts = _parse_ts(m.get("close_time") or m.get("expiration_time") or m.get("close_date"))
        open_ts  = (close_ts - 900) if close_ts else None

        # Extract price path
        # Dump format varies — look for common fields
        price_history = (m.get("price_history") or m.get("prices") or
                         m.get("history") or m.get("orderbook_history") or [])

        # Extract yes prices from history
        yes_prices: list[float] = []
        for entry in price_history:
            if isinstance(entry, dict):
                p = (entry.get("yes_price") or entry.get("yes_bid") or
                     entry.get("price") or entry.get("yes") or entry.get("mid"))
                if p is not None:
                    yes_prices.append(float(p))
            elif isinstance(entry, (int, float)):
                yes_prices.append(float(entry))

        # Also check for open/close price fields
        open_price = (m.get("open_price") or m.get("opening_price") or
                      m.get("yes_open") or (yes_prices[0] if yes_prices else None))
        last_price = (m.get("last_price") or m.get("last_yes_price") or
                      m.get("close_price") or (yes_prices[-1] if yes_prices else None))

        # Volume
        volume = m.get("volume") or m.get("yes_volume") or 0

        by_series[series].append({
            "ticker":      ticker,
            "series":      series,
            "result":      result,
            "open_ts":     open_ts,
            "close_ts":    close_ts,
            "open_price":  float(open_price) if open_price is not None else None,
            "last_price":  float(last_price) if last_price is not None else None,
            "min_price":   min(yes_prices) if yes_prices else None,
            "max_price":   max(yes_prices) if yes_prices else None,
            "price_count": len(yes_prices),
            "volume":      int(volume) if volume else 0,
            "hour":        datetime.fromtimestamp(open_ts, tz=timezone.utc).hour if open_ts else None,
            "weekday":     datetime.fromtimestamp(open_ts, tz=timezone.utc).weekday() if open_ts else None,
            # raw market for field discovery
            "_raw_keys":   list(m.keys()),
        })

    for series, contracts in by_series.items():
        log.info(f"  {series}: {len(contracts)} settled contracts")

    return dict(by_series)

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def bucket_label(price: float) -> str:
    b = int(price // 10) * 10
    b = max(0, min(90, b))
    return f"{b:02d}-{b+10:02d}"


def analyze_series(series: str, contracts: list[dict]) -> dict:
    n = len(contracts)
    if n < 10:
        return {"series": series, "count": n}

    yes_wins = sum(1 for c in contracts if c["result"] == "yes")

    # ---- Price bucket analysis ----
    contracts_with_open = [c for c in contracts if c["open_price"] is not None]
    bucket_raw: dict[str, dict] = defaultdict(lambda: {
        "count": 0, "yes": 0, "volume": 0,
        "hit_tp": 0, "hit_sl": 0, "swings": [],
    })
    for c in contracts_with_open:
        b = bucket_label(c["open_price"])
        bucket_raw[b]["count"] += 1
        bucket_raw[b]["volume"] += c["volume"]
        if c["result"] == "yes":
            bucket_raw[b]["yes"] += 1
        if c["max_price"] is not None and c["max_price"] >= TAKE_PROFIT_CENTS:
            bucket_raw[b]["hit_tp"] += 1
        if c["min_price"] is not None and c["min_price"] <= STOP_LOSS_CENTS:
            bucket_raw[b]["hit_sl"] += 1
        swing = (c["max_price"] - c["min_price"]) if (c["max_price"] and c["min_price"]) else None
        if swing is not None:
            bucket_raw[b]["swings"].append(swing)

    price_buckets: dict[str, dict] = {}
    for b in sorted(bucket_raw.keys()):
        d = bucket_raw[b]
        cnt = d["count"]
        if cnt < 3:
            continue
        yes_rate  = d["yes"] / cnt
        mid_cents = int(b.split("-")[0]) + 5
        ev_yes    = yes_rate * 100 - mid_cents
        ev_no     = (1 - yes_rate) * 100 - (100 - mid_cents)
        avg_swing = sum(d["swings"]) / len(d["swings"]) if d["swings"] else 0.0
        tp_rate   = d["hit_tp"] / cnt
        sl_rate   = d["hit_sl"] / cnt
        bracket_ev = tp_rate * 20 - sl_rate * 16
        price_buckets[b] = {
            "count": cnt, "yes_rate": round(yes_rate, 3),
            "ev_yes": round(ev_yes, 2), "ev_no": round(ev_no, 2),
            "avg_volume": round(d["volume"] / cnt, 1),
            "avg_swing": round(avg_swing, 1),
            "tp_rate": round(tp_rate, 3), "sl_rate": round(sl_rate, 3),
            "bracket_ev": round(bracket_ev, 2),
        }

    # ---- Bracket by entry price ----
    bracket_entries = [25, 30, 35, 36, 40, 45, 50, 55, 60, 65, 70]
    bracket_by_entry: dict[int, dict] = {}
    for entry in bracket_entries:
        lo, hi = entry - 5, entry + 5
        cands = [c for c in contracts_with_open if lo <= c["open_price"] <= hi]
        cnt = len(cands)
        if cnt < 5:
            bracket_by_entry[entry] = {"count": cnt}
            continue
        hit_tp = sum(1 for c in cands if c["max_price"] and c["max_price"] >= TAKE_PROFIT_CENTS)
        hit_sl = sum(1 for c in cands if c["min_price"] and c["min_price"] <= STOP_LOSS_CENTS)
        tp_rate = hit_tp / cnt
        sl_rate = hit_sl / cnt
        yes_rate = sum(1 for c in cands if c["result"] == "yes") / cnt
        bracket_by_entry[entry] = {
            "count": cnt, "tp_rate": round(tp_rate, 3), "sl_rate": round(sl_rate, 3),
            "ev_cents": round(tp_rate * 20 - sl_rate * 16, 2),
            "yes_rate": round(yes_rate, 3),
        }

    # ---- Time of day ----
    hour_raw: dict[int, dict] = defaultdict(lambda: {"count": 0, "yes": 0, "volume": 0})
    for c in contracts:
        h = c["hour"]
        if h is None:
            continue
        hour_raw[h]["count"] += 1
        hour_raw[h]["volume"] += c["volume"]
        if c["result"] == "yes":
            hour_raw[h]["yes"] += 1

    hour_analysis = {
        h: {
            "count": d["count"],
            "yes_rate": round(d["yes"] / d["count"], 3) if d["count"] else 0,
        }
        for h, d in hour_raw.items()
    }

    # ---- Momentum vs mean reversion ----
    above_55 = [c for c in contracts_with_open if c["open_price"] > 55]
    below_45 = [c for c in contracts_with_open if c["open_price"] < 45]
    momentum = {}
    if len(above_55) >= 5:
        yr = sum(1 for c in above_55 if c["result"] == "yes") / len(above_55)
        momentum["above_55"] = {"count": len(above_55), "yes_rate": round(yr, 3),
                                 "signal": "MOMENTUM" if yr > 0.55 else ("MEAN-REVERT" if yr < 0.45 else "RANDOM")}
    if len(below_45) >= 5:
        yr = sum(1 for c in below_45 if c["result"] == "yes") / len(below_45)
        momentum["below_45"] = {"count": len(below_45), "yes_rate": round(yr, 3),
                                  "signal": "MEAN-REVERT" if yr > 0.55 else ("MOMENTUM" if yr < 0.45 else "RANDOM")}

    # ---- Volatility ----
    swings = [(c["max_price"] - c["min_price"]) for c in contracts if c["max_price"] and c["min_price"]]
    volatility = {
        "avg_swing":    round(sum(swings) / len(swings), 1) if swings else 0,
        "pct_swing_20": round(sum(1 for s in swings if s >= 20) / len(swings), 3) if swings else 0,
        "data_coverage": f"{len(contracts_with_open)}/{n} have opening price",
        "price_path_coverage": f"{sum(1 for c in contracts if c['price_count'] > 0)}/{n} have price path",
    }

    # ---- Raw field discovery (show what data IS in the dump) ----
    raw_keys: set[str] = set()
    for c in contracts[:5]:
        raw_keys.update(c.get("_raw_keys", []))

    return {
        "series": series, "count": n,
        "overall_yes_rate": round(yes_wins / n, 3),
        "price_buckets": price_buckets,
        "bracket_by_entry": bracket_by_entry,
        "hour_analysis": hour_analysis,
        "momentum": momentum,
        "volatility": volatility,
        "available_dump_fields": sorted(raw_keys),
    }

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def generate_report(results: list[dict], dump_fields_sample: list[str]) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    L: list[str] = []

    L += [
        "=" * 72,
        "  KALSHI 15-MIN CRYPTO MARKETS — EDGE ANALYSIS (PUBLIC DUMP DATA)",
        f"  Generated: {now}  |  Days analyzed: {DAYS_TO_FETCH}",
        "=" * 72,
        "",
        f"  Dump fields available: {', '.join(dump_fields_sample[:20])}",
        "",
    ]

    active = [r for r in results if r.get("count", 0) >= 10]

    for r in results:
        series = r["series"]
        L += [f"\n{'─'*72}", f"  {series}", f"{'─'*72}"]

        if r.get("count", 0) < 10:
            L.append(f"  Insufficient data ({r.get('count', 0)} contracts)")
            continue

        v = r["volatility"]
        L += [
            f"  Contracts analyzed   : {r['count']}",
            f"  Overall YES rate     : {r['overall_yes_rate']:.1%}  (50% = fair)",
            f"  Data coverage        : {v['data_coverage']}",
            f"  Price path coverage  : {v['price_path_coverage']}",
            f"  Avg price swing      : {v['avg_swing']:.1f}c",
            f"  Contracts swing ≥20c : {v['pct_swing_20']:.1%}",
            "",
        ]

        # Momentum
        m = r.get("momentum", {})
        if m:
            L.append("  MOMENTUM / MEAN-REVERSION:")
            if "above_55" in m:
                d = m["above_55"]
                L.append(f"  Open >55c → YES wins {d['yes_rate']:.1%}  (n={d['count']})  → {d['signal']}")
            if "below_45" in m:
                d = m["below_45"]
                L.append(f"  Open <45c → YES wins {d['yes_rate']:.1%}  (n={d['count']})  → {d['signal']}")
            L.append("")

        # Price buckets
        if r["price_buckets"]:
            L.append("  OPENING PRICE vs OUTCOME:")
            L.append(f"  {'Bucket':>8}  {'N':>5}  {'YES%':>6}  {'EV(YES)':>8}  {'EV(NO)':>8}  {'Swing':>7}  {'BracketEV':>10}")
            L.append(f"  {'─'*8}  {'─'*5}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*7}  {'─'*10}")
            for b, d in sorted(r["price_buckets"].items()):
                flag = ""
                if d["count"] >= 10 and abs(d["ev_yes"]) >= 3:
                    flag = "  ◄ YES EDGE" if d["ev_yes"] > 0 else "  ◄ NO EDGE"
                elif d.get("bracket_ev", 0) >= 2 and d["count"] >= 10:
                    flag = "  ◄ BRACKET"
                L.append(
                    f"  {b:>8}  {d['count']:>5}  {d['yes_rate']:>5.1%}  "
                    f"  {d['ev_yes']:>+6.1f}c  {d['ev_no']:>+6.1f}c  "
                    f"{d['avg_swing']:>6.1f}c  {d['bracket_ev']:>+9.2f}c{flag}"
                )
            L.append("")

        # Bracket analysis
        if any(d.get("count", 0) >= 5 for d in r["bracket_by_entry"].values()):
            L.append("  BRACKET STRATEGY (YES @ entry ±5c, TP=56c, SL=20c):")
            L.append(f"  {'Entry':>6}  {'N':>5}  {'TP%':>6}  {'SL%':>6}  {'EV':>8}  {'YES%':>6}")
            L.append(f"  {'─'*6}  {'─'*5}  {'─'*6}  {'─'*6}  {'─'*8}  {'─'*6}")
            for entry, d in sorted(r["bracket_by_entry"].items()):
                if d.get("count", 0) < 5:
                    L.append(f"  {entry:>5}c  {'<5':>5}")
                else:
                    flag = "  ◄ EDGE" if d.get("ev_cents", 0) > 2 else ""
                    L.append(
                        f"  {entry:>5}c  {d['count']:>5}  {d['tp_rate']:>5.1%}  "
                        f"{d['sl_rate']:>5.1%}  {d['ev_cents']:>+7.2f}c  {d['yes_rate']:>5.1%}{flag}"
                    )
            L.append("")

        # Time of day
        ha = r.get("hour_analysis", {})
        if ha:
            L.append("  TIME-OF-DAY (UTC 4-hour blocks):")
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
                L.append(f"  {bl:02d}:00-{bl+4:02d}:00 UTC:  {yr:.1%} YES  (n={cnt}){flag}")
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

        L.append("\n  TOP EV PLAYS (n≥10, |EV|≥3c):")
        found = False
        for r in active:
            for b, d in r["price_buckets"].items():
                if d["count"] >= 10 and abs(d["ev_yes"]) >= 3:
                    side = "BUY YES" if d["ev_yes"] > 0 else "BUY NO"
                    ev = max(d["ev_yes"], d["ev_no"])
                    L.append(f"  {r['series']} open {b}c → {side}  EV={ev:+.1f}c  (n={d['count']})")
                    found = True
            for entry, d in r["bracket_by_entry"].items():
                if d.get("count", 0) >= 10 and d.get("ev_cents", 0) > 2:
                    L.append(
                        f"  {r['series']} bracket@{entry}c  EV={d['ev_cents']:+.2f}c  "
                        f"TP={d['tp_rate']:.1%} SL={d['sl_rate']:.1%}  (n={d['count']})"
                    )
                    found = True
        if not found:
            L.append("  None yet with sufficient sample size.")
    else:
        L.append("  No series with ≥10 contracts.")

    L += ["", "=" * 72, "  END OF REPORT", "=" * 72]
    return "\n".join(L)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global log
    log = setup_logging()
    log.info("=" * 60)
    log.info("Kalshi Dump Analyzer — 15-min Crypto Markets")
    log.info("=" * 60)

    # Download dumps
    all_markets = fetch_all_dumps()
    if not all_markets:
        log.error("No market data downloaded. Check URL or internet connection.")
        sys.exit(1)

    # Show a sample of raw keys to understand dump structure
    sample_keys: list[str] = []
    if all_markets:
        sample_keys = sorted(all_markets[0].keys())
        log.info(f"Sample dump fields: {sample_keys}")

    # Extract crypto contracts
    log.info("Extracting crypto 15-min contracts...")
    by_series = extract_crypto_contracts(all_markets)

    if not by_series:
        log.warning("No crypto 15-min contracts found in dumps.")
        log.info("Dump sample record:")
        for k, v in list(all_markets[0].items())[:10]:
            log.info(f"  {k}: {str(v)[:80]}")
        sys.exit(0)

    # Analyze each series
    log.info("Running analysis...")
    results = [analyze_series(series, contracts) for series, contracts in by_series.items()]
    for r in results:
        log.info(f"  {r['series']}: {r['count']} contracts")

    # Generate report
    report = generate_report(results, sample_keys)
    print("\n" + report)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)
    log.info(f"Report → {REPORT_PATH}")

    with open(OUTPUT_DIR / "results_dump.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    log.info("Done.")


if __name__ == "__main__":
    main()
