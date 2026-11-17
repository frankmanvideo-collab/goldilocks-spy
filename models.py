"""Table definitions (SQLAlchemy Core). Only *closed, net-profitable* events and daily roll-ups live here."""
from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
)

metadata = MetaData()

# One row per gap that opened, stayed net-profitable, and closed.
gap_events = Table(
    "gap_events",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("coin", String(24), nullable=False),
    Column("buy_exchange", String(16), nullable=False),
    Column("sell_exchange", String(16), nullable=False),
    Column("opened_at", DateTime(timezone=True), nullable=False),
    Column("closed_at", DateTime(timezone=True), nullable=False),
    Column("duration_ms", Integer, nullable=False),
    Column("entry_spread_pct", Float, nullable=False),
    Column("peak_spread_pct", Float, nullable=False),
    Column("notional_usd", Float, nullable=False),
    Column("paper_profit_usd", Float, nullable=False),
    Column("event_day", Date, nullable=False),  # UTC day of closed_at
    Index("ix_gap_events_day_coin", "event_day", "coin"),
)

# One row per funding divergence > threshold APR (cooldown-limited per coin).
funding_events = Table(
    "funding_events",
    metadata,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("logged_at", DateTime(timezone=True), nullable=False),
    Column("coin", String(24), nullable=False),
    Column("long_exchange", String(16), nullable=False),   # lowest funding -> go long here
    Column("short_exchange", String(16), nullable=False),  # highest funding -> go short here
    Column("long_apr_pct", Float, nullable=False),
    Column("short_apr_pct", Float, nullable=False),
    Column("divergence_apr_pct", Float, nullable=False),
    Column("event_day", Date, nullable=False),
    Index("ix_funding_events_day", "event_day"),
)

# Tiny permanent roll-up: raw rows can be purged without losing the day-by-day history.
daily_stats = Table(
    "daily_stats",
    metadata,
    Column("day", Date, primary_key=True),
    Column("gap_count", Integer, nullable=False),
    Column("avg_duration_ms", Float, nullable=False),
    Column("max_duration_ms", Integer, nullable=False),
    Column("top_coin", String(24)),
    Column("profit_points", Float, nullable=False),
)
