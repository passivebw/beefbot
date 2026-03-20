"""test_features.py - Unit tests using synthetic data only."""
from __future__ import annotations
import time
import pytest
from app.data.market_state import (
    MarketStateSnapshot, Trade, OrderBook, BookLevel, LiquidationCluster
)
from app.features.order_flow import compute_order_flow_imbalance
from app.features.taker_flow import compute_taker_flow
from app.features.liquidation import compute_liquidation_signal
from app.features.volatility import compute_volatility
from app.features.volume import compute_volume_spike


def snap(trades=None, book=None, clusters=None, now=None):
    now = now or time.time()
    return MarketStateSnapshot(
        now=now, last_trade=trades[-1] if trades else None,
        trade_tape=trades or [], order_book=book,
        oi_current=None, oi_history=[], liquidation_clusters=clusters or [],
        kalshi_market=None, binance_last_update=now, bybit_last_update=now,
        coinglass_last_update=now, kalshi_last_update=now,
    )

def trade(price=50000, qty=1.0, buy=True, age=0):
    return Trade(time.time()-age, price, qty, not buy)

def book(bid_d=1.0, ask_d=1.0, mid=50000.0):
    bids = [BookLevel(mid - 5 - i*10, bid_d) for i in range(10)]
    asks = [BookLevel(mid + 5 + i*10, ask_d) for i in range(10)]
    return OrderBook(time.time(), bids, asks)


def test_ofi_balanced():
    r = compute_order_flow_imbalance(snap(book=book(), trades=[trade()]))
    assert r.is_valid and abs(r.ofi_top10) < 0.05

def test_ofi_bid_heavy():
    r = compute_order_flow_imbalance(snap(book=book(bid_d=5.0, ask_d=1.0), trades=[trade()]))
    assert r.is_valid and r.ofi_top10 > 0.3

def test_ofi_no_book():
    assert not compute_order_flow_imbalance(snap()).is_valid

def test_taker_all_buys():
    now = time.time()
    trades = [Trade(now-i, 50000, 1.0, False) for i in range(20)]
    r = compute_taker_flow(snap(trades=trades, now=now))
    assert r.is_valid and r.buy_ratio_60s > 0.95

def test_taker_all_sells():
    now = time.time()
    trades = [Trade(now-i, 50000, 1.0, True) for i in range(20)]
    r = compute_taker_flow(snap(trades=trades, now=now))
    assert r.buy_ratio_60s < 0.05

def test_taker_no_trades():
    assert not compute_taker_flow(snap()).is_valid

def test_liq_upside_pull():
    now = time.time()
    clusters = [LiquidationCluster(50200, 50_000_000, "short"), LiquidationCluster(49500, 2_000_000, "long")]
    r = compute_liquidation_signal(snap(trades=[trade(50000)], clusters=clusters, now=now), cluster_distance_bps=100.0)
    assert r.is_valid and r.net_pull_signal > 0

def test_liq_no_clusters():
    assert not compute_liquidation_signal(snap(trades=[trade()])).is_valid

def test_vol_flat():
    now = time.time()
    trades = [Trade(now-i, 50000.0, 1.0, False) for i in range(30)]
    r = compute_volatility(snap(trades=trades, now=now))
    assert r.is_valid and r.realized_vol_1m_bps < 0.5

def test_volume_spike():
    now = time.time()
    base = [Trade(now-240+i*4, 50000, 0.1, False) for i in range(60)]
    burst = [Trade(now-5+i*0.5, 50000, 3.0, False) for i in range(10)]
    r = compute_volume_spike(snap(trades=base+burst, now=now), spike_multiplier=2.0)
    assert r.is_valid and r.is_spike
