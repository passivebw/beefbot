"""signal_engine.py - Assembles features and produces trade recommendation."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
from app.config import AppConfig
from app.data.market_state import MarketStateSnapshot
from app.features.order_flow import compute_order_flow_imbalance
from app.features.taker_flow import compute_taker_flow
from app.features.open_interest import compute_open_interest_signal
from app.features.liquidation import compute_liquidation_signal
from app.features.volatility import compute_volatility
from app.features.volume import compute_volume_spike
from app.features.probability_model import compute_probability, AllFeatures, ProbabilityOutput
from app.logger import get_logger

log = get_logger(__name__)
Action = Literal["BUY_YES", "BUY_NO", "HOLD"]


@dataclass
class SignalResult:
    prob_up: float
    prob_down: float
    confidence: float
    composite_score: float
    recommended_action: Action
    features: AllFeatures
    probability_output: ProbabilityOutput
    reason: str


def run_signal_engine(snapshot: MarketStateSnapshot, cfg: AppConfig) -> SignalResult:
    s = cfg.strategy
    features = AllFeatures(
        order_flow=compute_order_flow_imbalance(snapshot, n_levels=s.book_imbalance_levels),
        taker_flow=compute_taker_flow(snapshot, s.taker_short_window_s, s.taker_medium_window_s, s.taker_long_window_s),
        oi=compute_open_interest_signal(snapshot, significant_delta_pct=s.oi_significant_delta_pct),
        liquidation=compute_liquidation_signal(snapshot, cluster_distance_bps=s.liq_cluster_distance_bps),
        volatility=compute_volatility(snapshot),
        volume=compute_volume_spike(snapshot, spike_multiplier=s.volume_spike_multiplier),
    )
    prob = compute_probability(snapshot, features, cfg)
    action, reason = _action(prob, snapshot, cfg)
    log.info(f"signal | action={action} prob_up={prob.prob_up:.3f} prob_down={prob.prob_down:.3f} conf={prob.confidence:.3f} | {reason}")
    return SignalResult(prob.prob_up, prob.prob_down, prob.confidence, prob.composite_score, action, features, prob, reason)


def _action(prob: ProbabilityOutput, snapshot: MarketStateSnapshot, cfg: AppConfig):
    s = cfg.strategy
    if prob.confidence < s.min_confidence:
        return "HOLD", f"low confidence {prob.confidence:.2f}"
    if not snapshot.kalshi_market:
        return "HOLD", "no Kalshi market"
    tte = snapshot.kalshi_market.seconds_to_expiry
    if tte < cfg.kalshi.min_time_to_expiry_s:
        return "HOLD", f"expiry too close ({tte:.0f}s)"
    if prob.prob_up > 0.5:
        return "BUY_YES", f"prob_up={prob.prob_up:.3f}"
    return "BUY_NO", f"prob_down={prob.prob_down:.3f}"
