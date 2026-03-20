"""edge_engine.py - Compares model probability to Kalshi market price."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from app.config import AppConfig
from app.data.market_state import MarketStateSnapshot
from app.strategy.signal_engine import SignalResult
from app.logger import get_logger

log = get_logger(__name__)


@dataclass
class EdgeResult:
    action: str
    raw_edge: float
    net_edge: float
    model_prob: float
    market_implied_prob: float
    target_price_cents: int
    passes_threshold: bool
    reject_reason: Optional[str]


def _btc_momentum(snapshot: MarketStateSnapshot, window_s: float) -> float:
    """Returns abs BTC price change over the last window_s seconds. 0 if insufficient data."""
    if not snapshot.current_price or not snapshot.trade_tape:
        return 0.0
    cutoff = snapshot.now - window_s
    old_trades = [t for t in snapshot.trade_tape if t.timestamp <= cutoff]
    if not old_trades:
        return 0.0
    old_price = old_trades[-1].price
    return abs(snapshot.current_price - old_price)


def compute_edge(signal: SignalResult, snapshot: MarketStateSnapshot, cfg: AppConfig) -> EdgeResult:
    s = cfg.strategy
    market = snapshot.kalshi_market

    if signal.recommended_action == "HOLD":
        return EdgeResult("HOLD", 0.0, 0.0, signal.prob_up, 0.5, 50, False, signal.reason)

    if not market:
        return EdgeResult("HOLD", 0.0, 0.0, signal.prob_up, 0.5, 50, False, "no market")

    if market.yes_spread > s.max_spread_pct:
        reason = f"spread {market.yes_spread:.3f} > max {s.max_spread_pct}"
        log.warning(f"edge: {reason}")
        return EdgeResult("HOLD", 0.0, 0.0, signal.prob_up, market.implied_prob_yes,
                          int(market.yes_ask * 100), False, reason)

    # Require at least 2 active signals — don't trade on order flow alone
    active_signals = sum(1 for v in signal.probability_output.signal_breakdown.values() if v != 0.0)
    if active_signals < 2:
        reason = f"only {active_signals} active signal(s) — insufficient data"
        log.warning(f"edge: {reason}")
        return EdgeResult("HOLD", 0.0, 0.0, signal.prob_up, market.implied_prob_yes,
                          int(market.yes_ask * 100), False, reason)

    if signal.recommended_action == "BUY_YES":
        model_prob = signal.prob_up
        market_implied = market.implied_prob_yes
        target_cents = int(round(market.yes_ask * 100))
    else:
        model_prob = signal.prob_down
        market_implied = market.implied_prob_no
        target_cents = int(round(market.no_ask * 100))

    raw_edge = model_prob - market_implied
    net_edge = raw_edge - s.fee_per_trade - s.slippage_buffer
    max_edge = getattr(s, 'max_edge_pct', 0.30)
    passes = (net_edge >= s.min_edge_pct and net_edge <= max_edge
              and signal.confidence >= s.min_confidence)

    reject_reason = None
    if not passes:
        if net_edge < s.min_edge_pct:
            reject_reason = f"net_edge {net_edge:.3f} < min {s.min_edge_pct}"
        elif net_edge > max_edge:
            reject_reason = f"net_edge {net_edge:.3f} > max {max_edge} (longshot trap)"
        else:
            reject_reason = f"confidence {signal.confidence:.2f} < min {s.min_confidence}"

    log.info(f"edge | action={signal.recommended_action} model={model_prob:.3f} market={market_implied:.3f} "
             f"raw={raw_edge:.3f} net={net_edge:.3f} passes={passes}")

    return EdgeResult(
        action=signal.recommended_action if passes else "HOLD",
        raw_edge=raw_edge, net_edge=net_edge,
        model_prob=model_prob, market_implied_prob=market_implied,
        target_price_cents=target_cents,
        passes_threshold=passes, reject_reason=reject_reason,
    )
