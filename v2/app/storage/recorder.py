"""recorder.py - Writes decisions and trades to SQLite."""
from __future__ import annotations
import time
from datetime import datetime
from typing import Optional
from app.storage.db import get_session
from app.storage.models import DecisionLog, TradeLog, MarketSnapshot, ExitLog
from app.data.market_state import MarketStateSnapshot
from app.logger import get_logger

log = get_logger(__name__)


async def record_decision(snapshot: MarketStateSnapshot, signal, edge) -> None:
    try:
        market = snapshot.kalshi_market
        now = snapshot.now
        f = signal.features

        funding_rate = snapshot.oi_current.funding_rate if snapshot.oi_current else None
        momentum_30m = None
        if snapshot.trade_tape:
            now_ts = snapshot.now
            cutoff = now_ts - 1800.0
            old = next((t for t in snapshot.trade_tape if t.timestamp >= cutoff), None)
            if old:
                momentum_30m = round(snapshot.current_price - old.price, 2)

        features_json = {
            "ofi": round(f.order_flow.depth_weighted_imbalance, 4) if f.order_flow.is_valid else None,
            "taker_60s": round(f.taker_flow.buy_ratio_60s, 4) if f.taker_flow.is_valid else None,
            "taker_accel": round(f.taker_flow.flow_acceleration, 4) if f.taker_flow.is_valid else None,
            "oi_delta_5m": round(f.oi.oi_delta_5m_pct, 5) if f.oi.is_valid else None,
            "liq_signal": round(f.liquidation.net_pull_signal, 4) if f.liquidation.is_valid else None,
            "vol_regime": f.volatility.regime if f.volatility.is_valid else None,
            "vol_spike": f.volume.is_spike if f.volume.is_valid else None,
            "spread_bps": round(f.order_flow.spread_bps, 2) if f.order_flow.is_valid else None,
            "funding_rate": round(funding_rate, 8) if funding_rate is not None else None,
            "momentum_30m": momentum_30m,
        }

        row = DecisionLog(
            created_at=datetime.utcnow(),
            btc_price=snapshot.current_price,
            kalshi_ticker=market.ticker if market else None,
            kalshi_yes_bid=market.yes_bid if market else None,
            kalshi_yes_ask=market.yes_ask if market else None,
            kalshi_implied_prob=market.implied_prob_yes if market else None,
            kalshi_seconds_to_expiry=market.seconds_to_expiry if market else None,
            model_prob_up=signal.prob_up,
            model_prob_down=signal.prob_down,
            composite_score=signal.composite_score,
            confidence=signal.confidence,
            raw_edge=edge.raw_edge,
            net_edge=edge.net_edge,
            signal_breakdown=signal.probability_output.signal_breakdown,
            features_json=features_json,
            recommended_action=edge.action,
            action_reason=edge.reject_reason or signal.reason,
            binance_age_s=now - snapshot.binance_last_update,
            kalshi_age_s=now - snapshot.kalshi_last_update,
        )
        async with get_session() as session:
            session.add(row)
            await session.commit()
    except Exception as e:
        log.error(f"record_decision failed: {e}")


async def record_trade(execution, snapshot: MarketStateSnapshot, signal) -> None:
    if not execution.attempted or not execution.order:
        return
    try:
        o = execution.order
        row = TradeLog(
            created_at=datetime.utcnow(),
            order_id=o.order_id,
            kalshi_ticker=o.ticker,
            side=o.side,
            count=o.count,
            price_cents=o.price_cents,
            entry_cost_usd=execution.risk_usd,
            dry_run=execution.dry_run,
            status=o.status,
            model_prob=execution.edge.model_prob,
            market_implied_prob=execution.edge.market_implied_prob,
            net_edge=execution.edge.net_edge,
            confidence=signal.confidence,
            btc_price_at_entry=snapshot.current_price,
        )
        async with get_session() as session:
            session.add(row)
            await session.commit()
        log.info(f"trade recorded: {o.order_id}")
    except Exception as e:
        log.error(f"record_trade failed: {e}")


async def record_exit(exit_result, exit_decision, snapshot: MarketStateSnapshot, signal) -> None:
    try:
        pos = exit_result.position
        row = ExitLog(
            created_at=datetime.utcnow(),
            entry_order_id=pos.order_id,
            exit_order_id=exit_result.order.order_id if exit_result.order else None,
            kalshi_ticker=pos.ticker,
            side=pos.side,
            count=pos.count,
            entry_price_cents=pos.entry_price_cents,
            exit_price_cents=exit_result.exit_price_cents,
            realized_pnl_usd=exit_result.realized_pnl,
            entry_btc_price=pos.entry_btc_price,
            exit_btc_price=snapshot.current_price or 0.0,
            hold_duration_s=time.time() - pos.entry_time,
            exit_reason=exit_decision.reason,
            dry_run=exit_result.dry_run,
            is_active_mode=exit_decision.is_active_mode,
            exit_confidence=signal.confidence,
        )
        async with get_session() as session:
            session.add(row)
            await session.commit()
        log.info(f"exit recorded: pnl=${exit_result.realized_pnl:.2f} | held {row.hold_duration_s:.0f}s")
    except Exception as e:
        log.error(f"record_exit failed: {e}")


async def record_market_snapshot(snapshot: MarketStateSnapshot) -> None:
    try:
        market = snapshot.kalshi_market
        if not market:
            return
        row = MarketSnapshot(
            created_at=datetime.utcnow(),
            kalshi_ticker=market.ticker,
            yes_bid=market.yes_bid,
            yes_ask=market.yes_ask,
            implied_prob=market.implied_prob_yes,
            btc_price=snapshot.current_price,
        )
        async with get_session() as session:
            session.add(row)
            await session.commit()
    except Exception as e:
        log.error(f"record_snapshot failed: {e}")
