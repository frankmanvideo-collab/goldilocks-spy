"""Stage 1 - Goldilocks Wide Radar (every RADAR_INTERVAL_S seconds).

1. Pull public snapshots from all exchanges concurrently (a failing exchange never blocks the others).
2. Match overlapping coins.
3. Drop HFT-dominated giants (any venue 24h volume > VOL_MAX_USD) and dead coins (venue < VOL_MIN_USD).
4. Score the remaining mid-caps and keep the Top-N hot coins for the WebSocket sniper.
5. Log funding divergences above the APR threshold (cooldown-limited, so storage stays tiny).
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiohttp

from .config import Settings
from .exchanges_rest import FETCHERS, VenueInfo
from .repository import Writer
from .state import EXCHANGES, Hub, Target, VenueSpec

if TYPE_CHECKING:
    from .feeds.base import BaseFeed

log = logging.getLogger("radar")
UTC = timezone.utc

# Score = W_PRICE * price divergence (bps) + W_EXEC * executable spread (bps)
#       + W_VOL * 24h range/volatility (%) + W_FUND * funding divergence (APR %)
W_PRICE = 2.0
W_EXEC = 2.0
W_VOL = 1.0
W_FUND = 0.05


def _effective_buy(v: VenueInfo) -> float:
    return v.ask if v.ask > 0 else v.mid


def _effective_sell(v: VenueInfo) -> float:
    return v.bid if v.bid > 0 else v.mid


def select_hot_coins(
    snapshots: dict[str, dict[str, VenueInfo]], cfg: Settings
) -> tuple[list[Target], list[tuple[str, str, str, float, float, float]], int, int]:
    """Pure function (easy to unit-test). Returns (top targets, funding events, universe size, eligible size)."""
    by_coin: dict[str, dict[str, VenueInfo]] = {}
    for ex, venues in snapshots.items():
        for coin, info in venues.items():
            by_coin.setdefault(coin, {})[ex] = info

    matched = {c: v for c, v in by_coin.items() if len(v) >= 2}
    candidates: list[Target] = []
    funding_hits: list[tuple[str, str, str, float, float, float]] = []

    for coin, venues in matched.items():
        # --- Goldilocks volume band ------------------------------------------------
        if max(v.vol24_usd for v in venues.values()) > cfg.vol_max_usd:
            continue  # HFT-dominated giant (BTC/ETH/SOL ...)
        live = {ex: v for ex, v in venues.items() if v.vol24_usd >= cfg.vol_min_usd}  # drop dead venues
        if len(live) < 2:
            continue

        mids = [v.mid for v in live.values() if v.mid > 0]
        if len(mids) < 2:
            continue
        lo, hi = min(mids), max(mids)
        price_div_bps = (hi - lo) / lo * 1e4
        if price_div_bps > cfg.max_price_divergence_pct * 100.0:
            continue  # same ticker, different asset / scale mismatch -> not an arbitrage

        exec_bps = -1e9
        for bx, bv in live.items():
            for sx, sv in live.items():
                if bx == sx:
                    continue
                b, s = _effective_buy(bv), _effective_sell(sv)
                if b > 0 and s > 0:
                    exec_bps = max(exec_bps, (s - b) / b * 1e4)
        exec_bps = max(exec_bps, 0.0)

        vols = [v.range_pct if v.range_pct is not None else v.change_pct for v in live.values()]
        vol_pct = sum(vols) / len(vols)

        fundings = {ex: v.funding_apr_pct for ex, v in live.items() if v.funding_apr_pct is not None}
        funding_div = 0.0
        if len(fundings) >= 2:
            lo_ex = min(fundings, key=fundings.__getitem__)
            hi_ex = max(fundings, key=fundings.__getitem__)
            funding_div = fundings[hi_ex] - fundings[lo_ex]
            if funding_div > cfg.funding_apr_threshold_pct:
                funding_hits.append((coin, lo_ex, hi_ex, fundings[lo_ex], fundings[hi_ex], funding_div))

        score = W_PRICE * price_div_bps + W_EXEC * exec_bps + W_VOL * vol_pct + W_FUND * min(funding_div, 2000.0)
        candidates.append(
            Target(
                coin=coin,
                score=score,
                price_div_bps=price_div_bps,
                exec_bps=exec_bps,
                vol_pct=vol_pct,
                funding_div_apr=funding_div,
                volumes={ex: v.vol24_usd for ex, v in live.items()},
                venues={
                    ex: VenueSpec(coin=coin, symbol=v.symbol, mult=v.mult, contract_size=v.contract_size)
                    for ex, v in live.items()
                },
            )
        )

    candidates.sort(key=lambda t: t.score, reverse=True)
    return candidates[: cfg.top_n], funding_hits, len(matched), len(candidates)


class Radar:
    def __init__(
        self,
        cfg: Settings,
        hub: Hub,
        writer: Writer,
        session: aiohttp.ClientSession,
        feeds: "list[BaseFeed]",
    ) -> None:
        self.cfg = cfg
        self.hub = hub
        self.writer = writer
        self.session = session
        self.feeds = feeds
        self._funding_last: dict[str, float] = {}

    async def run_once(self) -> None:
        t0 = time.perf_counter()
        results = await asyncio.gather(
            *(FETCHERS[ex](self.session, self.cfg) for ex in EXCHANGES), return_exceptions=True
        )
        snapshots: dict[str, dict] = {}
        for ex, res in zip(EXCHANGES, results):
            if isinstance(res, BaseException):
                if isinstance(res, asyncio.CancelledError):
                    raise res
                self.hub.exchange_rest_ok[ex] = False
                log.warning("REST %s failed: %s: %s", ex, type(res).__name__, res)
            else:
                self.hub.exchange_rest_ok[ex] = True
                snapshots[ex] = res

        if len(snapshots) < 2:
            log.warning("radar: need >= 2 exchanges, only %d responded; keeping previous targets", len(snapshots))
            return

        targets, funding_hits, universe, eligible = select_hot_coins(snapshots, self.cfg)
        del snapshots, results  # release the big JSON-derived dicts immediately

        added, removed = self.hub.set_targets(targets)
        tmap = self.hub.targets
        for feed in self.feeds:
            feed.set_targets(tmap)

        self.hub.last_radar_ms = time.time_ns() // 1_000_000
        self.hub.radar_universe = universe
        self.hub.radar_eligible = eligible
        self._log_funding(funding_hits)
        log.info(
            "radar %.0fms matched=%d goldilocks=%d locked=%d (+%d/-%d) top=%s",
            (time.perf_counter() - t0) * 1000, universe, eligible, len(targets), len(added), len(removed),
            ",".join(t.coin for t in targets[:5]),
        )

    def _log_funding(self, hits: list[tuple[str, str, str, float, float, float]]) -> None:
        now = time.monotonic()
        cooldown = self.cfg.funding_log_cooldown_s
        for coin, long_ex, short_ex, long_apr, short_apr, div in hits:
            if now - self._funding_last.get(coin, -1e12) < cooldown:
                continue
            self._funding_last[coin] = now
            ts = datetime.now(UTC)
            self.writer.put_funding(
                {
                    "logged_at": ts,
                    "coin": coin,
                    "long_exchange": long_ex,
                    "short_exchange": short_ex,
                    "long_apr_pct": round(long_apr, 2),
                    "short_apr_pct": round(short_apr, 2),
                    "divergence_apr_pct": round(div, 2),
                    "event_day": ts.date(),
                }
            )
            log.info("FUNDING %s long=%s(%.0f%%) short=%s(%.0f%%) div=%.0f%% APR", coin, long_ex, long_apr, short_ex, short_apr, div)
        if len(self._funding_last) > 2000:  # bound the dict
            cutoff = now - cooldown
            self._funding_last = {k: v for k, v in self._funding_last.items() if v > cutoff}

    async def run_forever(self) -> None:
        while True:
            started = time.monotonic()
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("radar cycle failed")
            await asyncio.sleep(max(1.0, self.cfg.radar_interval_s - (time.monotonic() - started)))
