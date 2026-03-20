"""config.py - All configuration via environment variables."""
from __future__ import annotations
from typing import Literal, Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BinanceConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="BINANCE_", env_file=".env", extra="ignore")
    api_key: str = ""
    api_secret: str = ""
    ws_base_url: str = "wss://stream.binance.us:9443"
    rest_base_url: str = "https://api.binance.us"
    symbol: str = "BTCUSDT"
    book_depth: int = 20
    ws_reconnect_delay: float = 3.0
    staleness_threshold_s: float = 10.0


class HyperliquidConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="HYPERLIQUID_", env_file=".env", extra="ignore")
    poll_interval_s: float = 30.0
    staleness_threshold_s: float = 120.0


class CoinGlassConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="COINGLASS_", env_file=".env", extra="ignore")
    api_key: str = ""
    rest_base_url: str = "https://open-api.coinglass.com/public/v2"
    poll_interval_s: float = 60.0
    staleness_threshold_s: float = 300.0


class KalshiConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KALSHI_", env_file=".env", extra="ignore")
    api_key: str = ""
    private_key_path: str = ""
    env: Literal["demo", "prod"] = "demo"
    btc_series_ticker: str = "KXBTC15M"
    market_refresh_interval_s: float = 10.0
    min_time_to_expiry_s: float = 120.0
    staleness_threshold_s: float = 15.0

    @property
    def rest_base_url(self) -> str:
        if self.env == "prod":
            return "https://api.elections.kalshi.com/trade-api/v2"
        return "https://demo-api.kalshi.co/trade-api/v2"


class StrategyConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    w_order_flow: float = 0.05
    w_taker_pressure: float = 0.40
    w_open_interest: float = 0.15
    w_liquidation: float = 0.15
    w_volatility: float = 0.10
    w_volume: float = 0.10
    w_funding: float = 0.05
    momentum_window_s: float = 3600.0
    momentum_min_move: float = 30.0
    min_edge_pct: float = 0.06
    max_edge_pct: float = 0.50
    min_confidence: float = 0.60
    fee_per_trade: float = 0.01
    slippage_buffer: float = 0.005
    max_spread_pct: float = 0.06
    min_contract_liquidity: int = 10
    taker_short_window_s: float = 15.0
    taker_medium_window_s: float = 60.0
    taker_long_window_s: float = 180.0
    book_imbalance_levels: int = 10
    vol_short_window_s: float = 60.0
    vol_long_window_s: float = 300.0
    volume_spike_multiplier: float = 2.5
    logistic_k: float = 4.0
    oi_significant_delta_pct: float = 0.003
    liq_cluster_distance_bps: float = 50.0
    # Active position management
    active_mode_start_hour_et: int = 10
    active_mode_end_hour_et: int = 16
    exit_confidence_threshold: float = 0.70
    exit_min_time_to_expiry_s: float = 120.0
    exit_max_spread_pct: float = 0.10
    reentry_allowed: bool = True


class RiskConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    max_position_pct: float = 0.02
    kelly_fraction: float = 0.25
    use_kelly: bool = False
    max_daily_loss_pct: float = 0.05
    max_consecutive_losses: int = 5
    loss_cooldown_s: float = 1800.0
    session_start_utc_hour: Optional[int] = None
    session_end_utc_hour: Optional[int] = None
    avoid_macro_news: bool = False
    max_contracts: int = 20


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    bot_mode: Literal["paper", "live"] = Field(default="paper", validation_alias="BOT_MODE")
    log_level: str = Field(default="INFO", validation_alias="LOG_LEVEL")
    log_dir: str = Field(default="./logs", validation_alias="LOG_DIR")
    db_path: str = Field(default="./data/bot.db", validation_alias="DB_PATH")
    binance: BinanceConfig = Field(default_factory=BinanceConfig)
    hyperliquid: HyperliquidConfig = Field(default_factory=HyperliquidConfig)
    coinglass: CoinGlassConfig = Field(default_factory=CoinGlassConfig)
    kalshi: KalshiConfig = Field(default_factory=KalshiConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)


cfg = AppConfig()
