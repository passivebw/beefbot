"""exit_engine.py - Evaluates whether an open position should be exited early."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from app.config import AppConfig
from app.data.market_state import MarketStateSnapshot
from app.strategy.signal_engine import SignalResult
from app.strategy.risk_manager import OpenPosition
from app.logger import get_logger

log = get_logger(__name__)

ET = ZoneInfo("America/New_York")


@dataclass
class ExitDecision:
    should_exit: bool
    reason: str
    position: OpenPosition
    exit_price_cents: int
    is_active_mode: bool


def evaluate_exit(
    position: OpenPosition,
    signal: SignalResult,
    snapshot: MarketStateSnapshot,
    cfg: AppConfig,
) -> ExitDecision:
    s = cfg.strategy
    market = snapshot.kalshi_market

    def no_exit(reason: str) -> ExitDecision:
        return ExitDecision(False, reason, position, 0, False)

    # Check active mode window (10am-4pm ET)
    hour_et = datetime.now(ET).hour
    in_active_mode = s.active_mode_start_hour_et <= hour_et < s.active_mode_end_hour_et
    if not in_active_mode:
        return no_exit("passive mode — outside active hours")

    if not market:
        return no_exit("no market data")

    # Don't exit if too close to expiry
    if market.seconds_to_expiry < s.exit_min_time_to_expiry_s:
        return no_exit(f"too close to expiry ({market.seconds_to_expiry:.0f}s)")

    # Don't exit if spread is too wide (no liquidity)
    spread = market.yes_ask - market.yes_bid
    if spread > s.exit_max_spread_pct:
        return no_exit(f"spread too wide ({spread:.3f})")

    # Check if signal has flipped against our position with high confidence
    if signal.confidence < s.exit_confidence_threshold:
        return no_exit(f"signal confidence too low to exit ({signal.confidence:.2f} < {s.exit_confidence_threshold:.2f})")

    flipped = False
    if position.side == "yes" and signal.prob_down > 0.55:
        flipped = True
    elif position.side == "no" and signal.prob_up > 0.55:
        flipped = True

    if not flipped:
        return no_exit("signal consistent with position")

    # Calculate exit price — we sell at the bid
    if position.side == "yes":
        exit_price_cents = max(1, int(round(market.yes_bid * 100)))
    else:
        exit_price_cents = max(1, int(round(market.no_bid * 100)))

    # Don't exit if we'd lose more than just holding (e.g. bid collapsed)
    if exit_price_cents <= 1:
        return no_exit("bid too low — not worth exiting")

    direction = "DOWN" if position.side == "yes" else "UP"
    log.info(
        f"EXIT SIGNAL: held {position.side} @ {position.entry_price_cents}c | "
        f"signal flipped {direction} | conf={signal.confidence:.2f} | "
        f"exit @ {exit_price_cents}c"
    )

    return ExitDecision(
        should_exit=True,
        reason=f"signal flipped against {position.side} position (conf={signal.confidence:.2f})",
        position=position,
        exit_price_cents=exit_price_cents,
        is_active_mode=True,
    )
