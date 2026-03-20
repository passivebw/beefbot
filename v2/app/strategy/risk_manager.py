"""risk_manager.py - Position sizing and safety gates."""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from typing import Optional
from app.config import AppConfig
from app.data.market_state import MarketStateSnapshot
from app.logger import get_logger

log = get_logger(__name__)

MACRO_NEWS_UTC_HOURS = {8, 13, 14, 19, 20}


@dataclass
class RiskCheckResult:
    approved: bool
    reject_reason: Optional[str]
    position_size_contracts: int
    position_size_usd: float


@dataclass
class OpenPosition:
    ticker: str
    order_id: str
    side: str
    count: int
    entry_price_cents: int
    entry_time: float
    entry_btc_price: float


@dataclass
class SessionState:
    starting_balance: float = 1000.0
    current_balance: float = 1000.0
    daily_pnl: float = 0.0
    consecutive_losses: int = 0
    cooldown_until: float = 0.0
    open_positions: dict = field(default_factory=dict)  # ticker -> OpenPosition
    trade_count_today: int = 0


class RiskManager:
    def __init__(self, cfg: AppConfig) -> None:
        self._cfg = cfg
        self._r = cfg.risk
        self._state = SessionState()

    def set_balance(self, balance: float) -> None:
        self._state.starting_balance = balance
        self._state.current_balance = balance
        log.info(f"risk: balance set to ${balance:.2f}")

    def check(self, snapshot: MarketStateSnapshot, edge_result, dry_run: bool = True) -> RiskCheckResult:
        now = time.time()
        r = self._r

        if now < self._state.cooldown_until:
            return self._reject(f"cooldown for {self._state.cooldown_until - now:.0f}s")

        if not snapshot.binance_last_update or (now - snapshot.binance_last_update) > r.max_daily_loss_pct * 2:
            pass  # handled by state_is_minimally_ready in main

        loss_pct = -self._state.daily_pnl / max(self._state.starting_balance, 1.0)
        if loss_pct >= r.max_daily_loss_pct:
            return self._reject(f"daily loss limit {loss_pct*100:.1f}% >= {r.max_daily_loss_pct*100:.1f}%")

        if self._state.consecutive_losses >= r.max_consecutive_losses:
            self._state.cooldown_until = now + r.loss_cooldown_s
            return self._reject(f"consecutive losses {self._state.consecutive_losses} — entering cooldown")

        if r.session_start_utc_hour is not None and r.session_end_utc_hour is not None:
            from datetime import datetime, timezone
            h = datetime.now(timezone.utc).hour
            start, end = r.session_start_utc_hour, r.session_end_utc_hour
            in_session = start <= h < end if start < end else (h >= start or h < end)
            if not in_session:
                return self._reject(f"outside session window {start}-{end} UTC")

        if r.avoid_macro_news:
            from datetime import datetime, timezone
            if datetime.now(timezone.utc).hour in MACRO_NEWS_UTC_HOURS:
                return self._reject("macro news window")

        market = snapshot.kalshi_market
        if market and market.ticker in self._state.open_positions:
            return self._reject(f"already positioned on {market.ticker}")

        contracts, usd = self._size(edge_result)
        if contracts <= 0:
            return self._reject("position sizing = 0 contracts")

        return RiskCheckResult(True, None, contracts, usd)

    def _size(self, edge_result) -> tuple[int, float]:
        r = self._r
        balance = self._state.current_balance or 1000.0
        max_risk = balance * r.max_position_pct

        if r.use_kelly and edge_result.net_edge > 0:
            price = edge_result.target_price_cents / 100.0
            if 0 < price < 1:
                odds = (1.0 - price) / price
                kelly_f = edge_result.net_edge / odds
                capped = min(kelly_f * r.kelly_fraction, r.max_position_pct)
                max_risk = balance * capped

        cost = edge_result.target_price_cents / 100.0
        if cost <= 0:
            return 0, 0.0
        contracts = min(int(max_risk / cost), r.max_contracts)
        return max(0, contracts), contracts * cost

    def record_trade_open(self, ticker: str, order_id: str, side: str = "",
                          count: int = 0, entry_price_cents: int = 0,
                          btc_price: float = 0.0) -> None:
        self._state.open_positions[ticker] = OpenPosition(
            ticker=ticker, order_id=order_id, side=side, count=count,
            entry_price_cents=entry_price_cents, entry_time=time.time(),
            entry_btc_price=btc_price,
        )
        self._state.trade_count_today += 1

    def get_position(self, ticker: str) -> Optional[OpenPosition]:
        return self._state.open_positions.get(ticker)

    def record_exit(self, ticker: str, exit_price_cents: int) -> float:
        pos = self._state.open_positions.pop(ticker, None)
        if not pos:
            return 0.0
        pnl = (exit_price_cents - pos.entry_price_cents) * pos.count / 100.0
        self._state.daily_pnl += pnl
        self._state.current_balance += pnl
        if pnl < 0:
            self._state.consecutive_losses += 1
            log.warning(f"risk: early exit loss ${pnl:.2f} | daily_pnl=${self._state.daily_pnl:.2f}")
        else:
            self._state.consecutive_losses = 0
            log.info(f"risk: early exit profit ${pnl:.2f} | daily_pnl=${self._state.daily_pnl:.2f}")
        return pnl

    def record_outcome(self, ticker: str, pnl: float) -> None:
        self._state.open_positions.pop(ticker, None)
        self._state.daily_pnl += pnl
        self._state.current_balance += pnl
        if pnl < 0:
            self._state.consecutive_losses += 1
            log.warning(f"risk: loss ${pnl:.2f} | consecutive={self._state.consecutive_losses} | daily_pnl=${self._state.daily_pnl:.2f}")
        else:
            self._state.consecutive_losses = 0
            log.info(f"risk: win ${pnl:.2f} | daily_pnl=${self._state.daily_pnl:.2f}")

    def get_stats(self) -> dict:
        return {
            "balance": self._state.current_balance,
            "daily_pnl": self._state.daily_pnl,
            "consecutive_losses": self._state.consecutive_losses,
            "trades_today": self._state.trade_count_today,
            "open_positions": list(self._state.open_positions.keys()),
        }

    @staticmethod
    def _reject(reason: str) -> RiskCheckResult:
        log.info(f"risk rejected: {reason}")
        return RiskCheckResult(False, reason, 0, 0.0)
