"""models.py - SQLAlchemy ORM models."""
from __future__ import annotations
from datetime import datetime
from sqlalchemy import Column, Integer, Float, String, Boolean, DateTime, JSON, Text
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class DecisionLog(Base):
    __tablename__ = "decision_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    btc_price = Column(Float)
    kalshi_ticker = Column(String(50))
    kalshi_yes_bid = Column(Float)
    kalshi_yes_ask = Column(Float)
    kalshi_implied_prob = Column(Float)
    kalshi_seconds_to_expiry = Column(Float)
    model_prob_up = Column(Float)
    model_prob_down = Column(Float)
    composite_score = Column(Float)
    confidence = Column(Float)
    raw_edge = Column(Float)
    net_edge = Column(Float)
    signal_breakdown = Column(JSON)
    features_json = Column(JSON)
    recommended_action = Column(String(20))
    action_reason = Column(Text)
    binance_age_s = Column(Float)
    kalshi_age_s = Column(Float)


class TradeLog(Base):
    __tablename__ = "trade_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    order_id = Column(String(100))
    kalshi_ticker = Column(String(50))
    side = Column(String(10))
    count = Column(Integer)
    price_cents = Column(Integer)
    entry_cost_usd = Column(Float)
    dry_run = Column(Boolean, default=True)
    status = Column(String(30))
    model_prob = Column(Float)
    market_implied_prob = Column(Float)
    net_edge = Column(Float)
    confidence = Column(Float)
    btc_price_at_entry = Column(Float)
    pnl_usd = Column(Float, nullable=True)
    was_early_exit = Column(Boolean, default=False)


class ExitLog(Base):
    __tablename__ = "exit_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    entry_order_id = Column(String(100))
    exit_order_id = Column(String(100), nullable=True)
    kalshi_ticker = Column(String(50))
    side = Column(String(10))
    count = Column(Integer)
    entry_price_cents = Column(Integer)
    exit_price_cents = Column(Integer)
    realized_pnl_usd = Column(Float)
    entry_btc_price = Column(Float)
    exit_btc_price = Column(Float)
    hold_duration_s = Column(Float)
    exit_reason = Column(Text)
    dry_run = Column(Boolean, default=True)
    is_active_mode = Column(Boolean, default=True)
    exit_confidence = Column(Float)


class MarketSnapshot(Base):
    __tablename__ = "market_snapshot"
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    kalshi_ticker = Column(String(50))
    yes_bid = Column(Float)
    yes_ask = Column(Float)
    implied_prob = Column(Float)
    btc_price = Column(Float)
