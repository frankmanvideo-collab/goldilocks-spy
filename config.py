"""Central configuration. Every tunable is an environment variable with a safe default."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except ValueError as exc:  # fail loudly at boot, not at 3am
        raise RuntimeError(f"Environment variable {name}={raw!r} is not a number") from exc


def _int(name: str, default: int) -> int:
    return int(_float(name, float(default)))


def _str(name: str, default: str) -> str:
    raw = os.getenv(name)
    return raw.strip() if raw and raw.strip() else default


@dataclass(frozen=True, slots=True)
class Settings:
    # --- database -----------------------------------------------------------
    database_url: str

    # --- gap rules ------------------------------------------------------------
    min_profit_pct: float          # combined round-trip taker fees, in PERCENT (0.10 == 0.10%)
    min_liquidity_usd: float       # required top-of-book notional on BOTH books
    paper_notional_cap_usd: float  # max simulated trade size
    min_gap_duration_ms: int       # ignore gaps shorter than this (0 = keep everything)
    max_spread_sanity_pct: float   # spreads above this are treated as bad data, not alpha

    # --- stage 1: radar ---------------------------------------------------------
    vol_min_usd: float
    vol_max_usd: float
    top_n: int
    radar_interval_s: int
    max_price_divergence_pct: float  # cross-venue mid divergence above this => ticker/scale mismatch
    funding_apr_threshold_pct: float
    funding_log_cooldown_s: int

    # --- endpoints ----------------------------------------------------------------
    hl_rest: str
    hl_ws: str
    hl_feed: str                   # "bbo" (default) or "l2Book"
    bybit_rest: str
    bybit_ws: str
    mexc_rest: str
    mexc_ws: str

    # --- storage -------------------------------------------------------------------
    raw_retention_days: int        # raw rows older than this are purged (daily roll-ups are kept forever)

    log_level: str

    @property
    def min_profit(self) -> float:
        """Round-trip fee threshold as a fraction (0.001 for 0.10%)."""
        return self.min_profit_pct / 100.0

    @property
    def max_spread_sanity(self) -> float:
        return self.max_spread_sanity_pct / 100.0


def load_settings() -> Settings:
    return Settings(
        database_url=_str("DATABASE_URL", ""),
        min_profit_pct=_float("MIN_PROFIT_PCT", 0.10),
        min_liquidity_usd=_float("MIN_LIQUIDITY_USD", 300.0),
        paper_notional_cap_usd=_float("PAPER_NOTIONAL_CAP_USD", 1000.0),
        min_gap_duration_ms=_int("MIN_GAP_DURATION_MS", 0),
        max_spread_sanity_pct=_float("MAX_SPREAD_SANITY_PCT", 10.0),
        vol_min_usd=_float("VOL_MIN_USD", 1_000_000.0),
        vol_max_usd=_float("VOL_MAX_USD", 80_000_000.0),
        top_n=_int("TOP_N", 20),
        radar_interval_s=_int("RADAR_INTERVAL_S", 30),
        max_price_divergence_pct=_float("MAX_PRICE_DIVERGENCE_PCT", 20.0),
        funding_apr_threshold_pct=_float("FUNDING_APR_THRESHOLD_PCT", 50.0),
        funding_log_cooldown_s=_int("FUNDING_LOG_COOLDOWN_S", 3600),
        hl_rest=_str("HL_REST", "https://api.hyperliquid.xyz/info"),
        hl_ws=_str("HL_WS", "wss://api.hyperliquid.xyz/ws"),
        hl_feed=_str("HL_FEED", "bbo"),
        bybit_rest=_str("BYBIT_REST", "https://api.bybit.com"),
        bybit_ws=_str("BYBIT_WS", "wss://stream.bybit.com/v5/public/linear"),
        mexc_rest=_str("MEXC_REST", "https://contract.mexc.com"),
        mexc_ws=_str("MEXC_WS", "wss://contract.mexc.com/edge"),
        raw_retention_days=_int("RAW_RETENTION_DAYS", 60),
        log_level=_str("LOG_LEVEL", "INFO").upper(),
    )


settings = load_settings()
