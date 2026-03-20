"""order_flow.py - Order book imbalance features."""
from __future__ import annotations
from dataclasses import dataclass
from app.data.market_state import MarketStateSnapshot


@dataclass
class OrderFlowFeatures:
    ofi_top5: float
    ofi_top10: float
    depth_weighted_imbalance: float
    bid_depth_usd: float
    ask_depth_usd: float
    spread_bps: float
    is_valid: bool


def compute_order_flow_imbalance(snapshot: MarketStateSnapshot, n_levels: int = 10) -> OrderFlowFeatures:
    book = snapshot.order_book
    price = snapshot.current_price
    if not book or not price or not book.bids or not book.asks:
        return OrderFlowFeatures(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, False)

    def ofi(bq, aq):
        t = bq + aq
        return (bq - aq) / t if t > 0 else 0.0

    bids = book.bids[:n_levels]
    asks = book.asks[:n_levels]
    bq5 = sum(b.qty for b in bids[:5])
    aq5 = sum(a.qty for a in asks[:5])
    bq10 = sum(b.qty for b in bids)
    aq10 = sum(a.qty for a in asks)
    bw = sum(b.qty * b.price for b in bids)
    aw = sum(a.qty * a.price for a in asks)
    mid = book.mid or price
    spread_bps = (book.best_ask - book.best_bid) / mid * 10_000 if book.best_ask and book.best_bid else 0.0

    return OrderFlowFeatures(
        ofi_top5=ofi(bq5, aq5),
        ofi_top10=ofi(bq10, aq10),
        depth_weighted_imbalance=ofi(bw, aw),
        bid_depth_usd=bw,
        ask_depth_usd=aw,
        spread_bps=spread_bps,
        is_valid=True,
    )
