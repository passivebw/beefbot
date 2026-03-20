"""volatility.py - Realized volatility and regime classification."""
from __future__ import annotations
import math
from dataclasses import dataclass
from app.data.market_state import MarketStateSnapshot

REGIME_QUIET = "quiet"
REGIME_NORMAL = "normal"
REGIME_EXPANSION = "expansion"


@dataclass
class VolatilityFeatures:
    realized_vol_1m_bps: float
    realized_vol_5m_bps: float
    high_1m: float
    low_1m: float
    range_1m_bps: float
    vol_ratio: float
    regime: str
    is_valid: bool


def compute_volatility(snapshot: MarketStateSnapshot,
                        short_window_s: float = 60.0,
                        long_window_s: float = 300.0,
                        quiet_threshold: float = 0.5,
                        expansion_threshold: float = 2.0) -> VolatilityFeatures:
    trades = snapshot.trade_tape
    now = snapshot.now
    if not trades or len(trades) < 5:
        return VolatilityFeatures(0.0, 0.0, 0.0, 0.0, 0.0, 1.0, REGIME_NORMAL, False)

    def rvol(window_s):
        cutoff = now - window_s
        prices = [t.price for t in trades if t.timestamp >= cutoff]
        if len(prices) < 3:
            return 0.0, prices
        rets = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices))]
        if not rets:
            return 0.0, prices
        mean = sum(rets) / len(rets)
        var = sum((r - mean)**2 for r in rets) / len(rets)
        vol = math.sqrt(var) * 10_000
        per_s = vol / (window_s / max(len(rets), 1))
        return per_s, prices

    v1, p1 = rvol(short_window_s)
    v5, _ = rvol(long_window_s)

    high = max(p1) if p1 else 0.0
    low = min(p1) if p1 else 0.0
    mid = snapshot.current_price or ((high + low) / 2 if p1 else 1.0)
    range_bps = (high - low) / mid * 10_000 if mid else 0.0
    ratio = v1 / v5 if v5 > 0 else 1.0

    if v1 < quiet_threshold:
        regime = REGIME_QUIET
    elif v1 > expansion_threshold or ratio > 1.5:
        regime = REGIME_EXPANSION
    else:
        regime = REGIME_NORMAL

    return VolatilityFeatures(v1, v5, high, low, range_bps, ratio, regime, True)
