"""liquidation.py - Liquidation cluster proximity scoring."""
from __future__ import annotations
from dataclasses import dataclass
from app.data.market_state import MarketStateSnapshot


@dataclass
class LiquidationFeatures:
    nearest_up_cluster_dist_bps: float
    nearest_up_cluster_usd: float
    nearest_down_cluster_dist_bps: float
    nearest_down_cluster_usd: float
    net_pull_signal: float
    is_valid: bool


def compute_liquidation_signal(snapshot: MarketStateSnapshot,
                                cluster_distance_bps: float = 50.0,
                                min_cluster_usd: float = 5_000_000) -> LiquidationFeatures:
    price = snapshot.current_price
    clusters = snapshot.liquidation_clusters
    if not price or not clusters:
        return LiquidationFeatures(9999.0, 0.0, 9999.0, 0.0, 0.0, False)

    up, down = [], []
    for c in clusters:
        if c.usd_size < min_cluster_usd:
            continue
        if c.price_level > price:
            up.append(c)
        else:
            down.append(c)

    def nearest(lst):
        if not lst:
            return 9999.0, 0.0
        s = sorted(lst, key=lambda c: abs(c.price_level - price))
        dist = abs(s[0].price_level - price) / price * 10_000
        nearby = sum(c.usd_size for c in lst if abs(c.price_level - price) / price * 10_000 <= cluster_distance_bps)
        return dist, nearby

    ud, uu = nearest(up)
    dd, du = nearest(down)

    def pull(dist, usd):
        if dist >= cluster_distance_bps or usd == 0:
            return 0.0
        return (1.0 - dist / cluster_distance_bps) * usd

    up_p, down_p = pull(ud, uu), pull(dd, du)
    total = up_p + down_p
    signal = (up_p - down_p) / total if total > 0 else 0.0

    return LiquidationFeatures(ud, uu, dd, du, signal, True)
