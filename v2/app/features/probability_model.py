"""probability_model.py - Weighted sigmoid probability model."""
from __future__ import annotations
import math
from dataclasses import dataclass
from app.data.market_state import MarketStateSnapshot
from app.features.order_flow import OrderFlowFeatures
from app.features.taker_flow import TakerFlowFeatures
from app.features.open_interest import OIFeatures
from app.features.liquidation import LiquidationFeatures
from app.features.volatility import VolatilityFeatures, REGIME_EXPANSION
from app.features.volume import VolumeFeatures
from app.logger import get_logger

log = get_logger(__name__)


@dataclass
class AllFeatures:
    order_flow: OrderFlowFeatures
    taker_flow: TakerFlowFeatures
    oi: OIFeatures
    liquidation: LiquidationFeatures
    volatility: VolatilityFeatures
    volume: VolumeFeatures


@dataclass
class ProbabilityOutput:
    prob_up: float
    prob_down: float
    composite_score: float
    confidence: float
    signal_breakdown: dict
    features: AllFeatures


def _sigmoid(x: float, k: float = 4.0) -> float:
    try:
        return 1.0 / (1.0 + math.exp(-k * x))
    except OverflowError:
        return 0.0 if x < 0 else 1.0


def compute_probability(snapshot: MarketStateSnapshot, features: AllFeatures, cfg) -> ProbabilityOutput:
    s = cfg.strategy
    f = features

    s_ofi = (0.6 * f.order_flow.depth_weighted_imbalance + 0.4 * f.order_flow.ofi_top10) if f.order_flow.is_valid else 0.0
    s_taker = max(-1.0, min(1.0, (f.taker_flow.buy_ratio_60s - 0.5) * 2.0 + f.taker_flow.flow_acceleration * 0.6)) if f.taker_flow.is_valid else 0.0
    s_oi = max(-1.0, min(1.0, f.oi.oi_signal)) if f.oi.is_valid else 0.0
    s_liq = max(-1.0, min(1.0, f.liquidation.net_pull_signal)) if f.liquidation.is_valid else 0.0
    s_vol = -0.15 if (f.volatility.is_valid and f.volatility.regime == REGIME_EXPANSION) else 0.0
    s_volume = (f.volume.spike_strength * (f.taker_flow.buy_ratio_60s - 0.5) * 2.0
                if f.volume.is_valid and f.volume.is_spike and f.taker_flow.is_valid else 0.0)
    # Funding rate: positive funding = longs paying = bearish lean, negative = bullish lean
    # Scale so that 0.01% per hour (0.0001) maps to ~1.0 signal strength
    s_funding = 0.0
    if snapshot.oi_current is not None:
        s_funding = max(-1.0, min(1.0, -snapshot.oi_current.funding_rate * 10000.0))

    breakdown = {
        "order_flow": s_ofi, "taker_pressure": s_taker, "open_interest": s_oi,
        "liquidation": s_liq, "volatility": s_vol, "volume": s_volume,
        "funding": s_funding,
    }

    w_sum = s.w_order_flow + s.w_taker_pressure + s.w_open_interest + s.w_liquidation + s.w_volatility + s.w_volume + s.w_funding
    composite = (
        s.w_order_flow * s_ofi + s.w_taker_pressure * s_taker +
        s.w_open_interest * s_oi + s.w_liquidation * s_liq +
        s.w_volatility * s_vol + s.w_volume * s_volume +
        s.w_funding * s_funding
    ) / (w_sum or 1.0)

    prob_up = _sigmoid(composite, k=s.logistic_k)

    # Confidence scoring
    confidence = 1.0
    now = snapshot.now
    def age_penalty(last, thresh):
        age = now - last
        return min(0.3, max(0.0, (age - thresh) / thresh * 0.3)) if age >= thresh else 0.0

    def age_penalty_if_used(last, thresh):
        if last == 0.0:
            return 0.0  # source never connected — don't penalize unconfigured sources
        return age_penalty(last, thresh)

    confidence -= age_penalty(snapshot.binance_last_update, cfg.binance.staleness_threshold_s)
    confidence -= age_penalty_if_used(snapshot.hyperliquid_last_update, cfg.hyperliquid.staleness_threshold_s)
    confidence -= age_penalty_if_used(snapshot.coinglass_last_update, cfg.coinglass.staleness_threshold_s)
    confidence -= age_penalty(snapshot.kalshi_last_update, cfg.kalshi.staleness_threshold_s)
    if not f.liquidation.is_valid:
        confidence -= 0.15
    if not f.taker_flow.is_valid:
        confidence -= 0.20
    vals = [v for v in breakdown.values() if v != 0.0]
    if vals:
        bull = sum(1 for v in vals if v > 0)
        bear = sum(1 for v in vals if v < 0)
        confidence -= (min(bull, bear) / len(vals)) * 0.3
    if snapshot.kalshi_market:
        tte = snapshot.kalshi_market.seconds_to_expiry
        if tte < 60:
            confidence = 0.0
        elif tte < 180:
            confidence -= 0.2

    confidence = max(0.0, min(1.0, confidence))
    log.debug(f"prob_up={prob_up:.3f} composite={composite:.3f} confidence={confidence:.3f}")

    return ProbabilityOutput(
        prob_up=prob_up, prob_down=1.0 - prob_up,
        composite_score=composite, confidence=confidence,
        signal_breakdown=breakdown, features=features,
    )
