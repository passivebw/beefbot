"""taker_flow.py - Taker buy/sell pressure from trade tape."""
from __future__ import annotations
from dataclasses import dataclass
from app.data.market_state import MarketStateSnapshot


@dataclass
class TakerFlowFeatures:
    buy_ratio_15s: float
    volume_15s: float
    buy_ratio_60s: float
    volume_60s: float
    buy_ratio_180s: float
    volume_180s: float
    flow_acceleration: float
    trade_count_60s: int
    is_valid: bool


def compute_taker_flow(snapshot: MarketStateSnapshot,
                       short_window_s: float = 15.0,
                       medium_window_s: float = 60.0,
                       long_window_s: float = 180.0) -> TakerFlowFeatures:
    trades = snapshot.trade_tape
    now = snapshot.now
    if not trades:
        return TakerFlowFeatures(0.5, 0.0, 0.5, 0.0, 0.5, 0.0, 0.0, 0, False)

    def window(w):
        cutoff = now - w
        bv = sv = count = 0
        for t in reversed(trades):
            if t.timestamp < cutoff:
                break
            if t.is_buyer_maker:
                sv += t.qty
            else:
                bv += t.qty
            count += 1
        total = bv + sv
        return (bv / total if total > 0 else 0.5), bv + sv, count

    r15, v15, _ = window(short_window_s)
    r60, v60, c60 = window(medium_window_s)
    r180, v180, _ = window(long_window_s)

    return TakerFlowFeatures(
        buy_ratio_15s=r15, volume_15s=v15,
        buy_ratio_60s=r60, volume_60s=v60,
        buy_ratio_180s=r180, volume_180s=v180,
        flow_acceleration=r15 - r180,
        trade_count_60s=c60,
        is_valid=c60 >= 3,
    )
