"""execution_engine.py - Executes or simulates trades."""
from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Optional
from app.config import AppConfig
from app.data.market_state import MarketStateSnapshot
from app.data.kalshi_client import KalshiClient, KalshiOrder
from app.strategy.signal_engine import SignalResult
from app.strategy.edge_engine import EdgeResult
from app.strategy.exit_engine import ExitDecision
from app.strategy.risk_manager import RiskManager, OpenPosition
from app.logger import get_logger

log = get_logger(__name__)


@dataclass
class ExecutionResult:
    attempted: bool
    order: Optional[KalshiOrder]
    dry_run: bool
    edge: EdgeResult
    risk_contracts: int
    risk_usd: float
    timestamp: float
    note: str


@dataclass
class ExitResult:
    attempted: bool
    order: Optional[KalshiOrder]
    dry_run: bool
    position: OpenPosition
    exit_price_cents: int
    realized_pnl: float
    timestamp: float
    note: str


class ExecutionEngine:
    def __init__(self, cfg: AppConfig, kalshi: KalshiClient, risk: RiskManager) -> None:
        self._cfg = cfg
        self._kalshi = kalshi
        self._risk = risk
        self._dry_run = cfg.bot_mode == "paper"
        self._placed: dict[str, float] = {}  # ticker -> timestamp
        self._just_exited: set[str] = set()  # tickers to bypass dedup after exit

    async def execute_exit(self, exit_decision: ExitDecision, snapshot: MarketStateSnapshot) -> ExitResult:
        now = time.time()
        pos = exit_decision.position

        log.info(
            f"EXIT {'[PAPER]' if self._dry_run else '[LIVE]'}: "
            f"sell {pos.side} x{pos.count} @ {exit_decision.exit_price_cents}c "
            f"on {pos.ticker} | reason={exit_decision.reason}"
        )

        try:
            order = await self._kalshi.place_order(
                ticker=pos.ticker,
                side=pos.side,
                action="sell",
                count=pos.count,
                price_cents=exit_decision.exit_price_cents,
                dry_run=self._dry_run,
            )
        except Exception as e:
            log.error(f"exit order failed: {e}")
            return ExitResult(True, None, self._dry_run, pos,
                              exit_decision.exit_price_cents, 0.0, now, f"exception: {e}")

        pnl = self._risk.record_exit(pos.ticker, exit_decision.exit_price_cents)
        self._just_exited.add(pos.ticker)

        return ExitResult(
            attempted=True, order=order, dry_run=self._dry_run,
            position=pos, exit_price_cents=exit_decision.exit_price_cents,
            realized_pnl=pnl, timestamp=now,
            note="ok" if order else "no order returned",
        )

    async def execute(self, signal: SignalResult, edge: EdgeResult, snapshot: MarketStateSnapshot) -> ExecutionResult:
        now = time.time()

        if not edge.passes_threshold or edge.action == "HOLD":
            return ExecutionResult(False, None, self._dry_run, edge, 0, 0.0, now, f"HOLD: {edge.reject_reason}")

        market = snapshot.kalshi_market
        if not market:
            return ExecutionResult(False, None, self._dry_run, edge, 0, 0.0, now, "no market")

        # Dedup — bypass if we just exited this ticker (allow immediate re-entry)
        if market.ticker in self._just_exited:
            self._just_exited.discard(market.ticker)
        else:
            last = self._placed.get(market.ticker, 0.0)
            if (now - last) < 30.0:
                return ExecutionResult(False, None, self._dry_run, edge, 0, 0.0, now,
                                       f"dedup: {now - last:.0f}s since last order")

        risk = self._risk.check(snapshot, edge, self._dry_run)
        if not risk.approved:
            return ExecutionResult(False, None, self._dry_run, edge, 0, 0.0, now,
                                   f"risk rejected: {risk.reject_reason}")

        side = "yes" if edge.action == "BUY_YES" else "no"
        log.info(f"executing {'PAPER' if self._dry_run else 'LIVE'}: {side} x{risk.position_size_contracts} @ {edge.target_price_cents}c on {market.ticker}")

        try:
            order = await self._kalshi.place_order(
                ticker=market.ticker, side=side, action="buy",
                count=risk.position_size_contracts,
                price_cents=edge.target_price_cents,
                dry_run=self._dry_run,
            )
        except Exception as e:
            log.error(f"order placement failed: {e}")
            return ExecutionResult(True, None, self._dry_run, edge,
                                   risk.position_size_contracts, risk.position_size_usd, now, f"exception: {e}")

        if order:
            self._placed[market.ticker] = now
            self._risk.record_trade_open(
                ticker=market.ticker,
                order_id=order.order_id,
                side=side,
                count=risk.position_size_contracts,
                entry_price_cents=edge.target_price_cents,
                btc_price=snapshot.current_price or 0.0,
            )

        return ExecutionResult(
            attempted=True, order=order, dry_run=self._dry_run, edge=edge,
            risk_contracts=risk.position_size_contracts, risk_usd=risk.position_size_usd,
            timestamp=now, note="ok" if order else "no order returned",
        )
