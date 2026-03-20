"""open_interest.py - Open interest change signals."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from app.data.market_state import MarketStateSnapshot, OISnapshot


@dataclass
class OIFeatures:
    oi_delta_1m_pct: float
    oi_delta_5m_pct: float
    oi_signal: float
    regime: str
    is_valid: bool


def compute_open_interest_signal(snapshot: MarketStateSnapshot,
                                  window_1m_s: float = 60.0,
                                  window_5m_s: float = 300.0,
                                  significant_delta_pct: float = 0.003) -> OIFeatures:
    current = snapshot.oi_current
    history = snapshot.oi_history
    if not current or len(history) < 2:
        return OIFeatures(0.0, 0.0, 0.0, "unknown", False)

    now = snapshot.now

    def past(window_s) -> Optional[OISnapshot]:
        target = now - window_s
        best = None
        for s in history:
            if s.timestamp <= target:
                if best is None or s.timestamp > best.timestamp:
                    best = s
        return best

    def pct(ref):
        if ref is None or ref.oi_contracts == 0:
            return 0.0
        return (current.oi_contracts - ref.oi_contracts) / ref.oi_contracts

    d1 = pct(past(window_1m_s))
    d5 = pct(past(window_5m_s))

    if d5 > significant_delta_pct:
        regime, signal = "trend_continuation", 0.3
    elif d5 < -significant_delta_pct:
        regime, signal = "deleveraging", -0.2
    else:
        regime, signal = "stable", 0.0

    return OIFeatures(oi_delta_1m_pct=d1, oi_delta_5m_pct=d5, oi_signal=signal, regime=regime, is_valid=True)
