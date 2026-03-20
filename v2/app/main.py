"""
main.py - Live bot orchestrator.

Run with:
    python -m app.main

Modes:
    BOT_MODE=paper  (default) - no real orders
    BOT_MODE=live   - real Kalshi orders
"""
from __future__ import annotations
import asyncio
import signal as _signal
import time
from app.config import cfg
from app.logger import setup_logging, get_logger
from app.data.market_state import MarketState
from app.data.binance_ws import BinanceWebSocketClient
from app.data.hyperliquid_client import HyperliquidClient
from app.data.coinglass_client import CoinGlassClient
from app.data.kalshi_client import KalshiClient
from app.strategy.signal_engine import run_signal_engine
from app.strategy.edge_engine import compute_edge
from app.strategy.execution_engine import ExecutionEngine
from app.strategy.exit_engine import evaluate_exit
from app.strategy.risk_manager import RiskManager
from app.storage.db import init_db
from app.storage.recorder import record_decision, record_trade, record_exit, record_market_snapshot
from app.utils.time_utils import format_expiry_countdown

DECISION_INTERVAL_S = 5.0
SNAPSHOT_LOG_INTERVAL_S = 30.0

log = get_logger(__name__)


async def main() -> None:
    setup_logging(log_level=cfg.log_level, log_dir=cfg.log_dir)
    log.info(f"=== BTC Kalshi Bot starting | mode={cfg.bot_mode} | kalshi_env={cfg.kalshi.env} ===")

    await init_db(cfg.db_path)

    state = MarketState()
    binance = BinanceWebSocketClient(cfg.binance, state)
    hyperliquid = HyperliquidClient(cfg.hyperliquid, state)
    coinglass = CoinGlassClient(cfg.coinglass, state)
    kalshi = KalshiClient(cfg.kalshi, state)
    risk = RiskManager(cfg)
    executor = ExecutionEngine(cfg, kalshi, risk)

    # Set starting balance
    if cfg.bot_mode == "live":
        try:
            bal = await kalshi.get_balance()
            risk.set_balance(bal)
            log.info(f"live balance: ${bal:.2f}")
        except Exception as e:
            log.warning(f"could not fetch balance: {e} — using $1000 default")
            risk.set_balance(1000.0)
    else:
        risk.set_balance(1000.0)
        log.info("paper mode — balance set to $1000.00")

    # Start background tasks
    tasks = [
        asyncio.create_task(binance.run(), name="binance"),
        asyncio.create_task(hyperliquid.run(), name="hyperliquid"),
        asyncio.create_task(coinglass.run(), name="coinglass"),
        asyncio.create_task(kalshi.run_market_poller(), name="kalshi"),
    ]

    shutdown = asyncio.Event()

    def _on_signal(sig, frame):
        log.info(f"shutdown signal received ({sig})")
        shutdown.set()

    _signal.signal(_signal.SIGINT, _on_signal)
    _signal.signal(_signal.SIGTERM, _on_signal)

    log.info("waiting 5s for feeds to connect...")
    await asyncio.sleep(5.0)
    log.info("starting decision loop")

    last_snapshot_log = 0.0

    while not shutdown.is_set():
        loop_start = time.time()
        try:
            snap = state.snapshot()
            await _cycle(snap, executor, risk)

            if loop_start - last_snapshot_log >= SNAPSHOT_LOG_INTERVAL_S:
                await record_market_snapshot(snap)
                last_snapshot_log = loop_start

        except Exception as e:
            log.error(f"decision loop error: {e}", exc_info=True)

        elapsed = time.time() - loop_start
        sleep_s = max(0.0, DECISION_INTERVAL_S - elapsed)
        try:
            await asyncio.wait_for(asyncio.shield(shutdown.wait()), timeout=sleep_s)
        except asyncio.TimeoutError:
            pass

    log.info("shutting down...")
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("bot stopped")


async def _cycle(snap, executor, risk) -> None:
    now = snap.now

    if not snap.current_price:
        log.debug("no BTC price yet — waiting")
        return

    # Require fresh Binance data
    binance_age = now - snap.binance_last_update
    if binance_age > cfg.binance.staleness_threshold_s:
        log.warning(f"Binance data stale ({binance_age:.1f}s) — skipping cycle")
        return

    # Log current context
    market = snap.kalshi_market
    if market:
        tte = format_expiry_countdown(market.expiry_ts)
        log.info(
            f"BTC=${snap.current_price:,.2f} | "
            f"market={market.ticker} tte={tte} | "
            f"yes={market.yes_bid:.2f}/{market.yes_ask:.2f} implied={market.implied_prob_yes:.2f}"
        )
    else:
        kalshi_age = now - snap.kalshi_last_update
        log.info(f"BTC=${snap.current_price:,.2f} | no Kalshi market (last update {kalshi_age:.0f}s ago)")

    signal = run_signal_engine(snap, cfg)
    edge = compute_edge(signal, snap, cfg)
    await record_decision(snap, signal, edge)

    # Exit logic — check if we should exit an existing position
    if market:
        position = risk.get_position(market.ticker)
        if position:
            exit_decision = evaluate_exit(position, signal, snap, cfg)
            if exit_decision.should_exit:
                exit_result = await executor.execute_exit(exit_decision, snap)
                if exit_result.attempted:
                    await record_exit(exit_result, exit_decision, snap, signal)
                    log.info(f"EXIT completed | pnl=${exit_result.realized_pnl:+.2f} | reason={exit_decision.reason}")

    # Entry logic
    result = await executor.execute(signal, edge, snap)
    if result.attempted:
        await record_trade(result, snap, signal)

    log.debug(f"risk stats: {risk.get_stats()}")


if __name__ == "__main__":
    asyncio.run(main())
