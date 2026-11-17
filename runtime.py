"""Owns every long-lived task: DB bootstrap, writer, radar, three WebSocket feeds, housekeeping."""
from __future__ import annotations

import asyncio
import gc
import logging
import time
from typing import Any

import aiohttp

from .config import Settings
from .db import build_engine
from .feeds import BybitFeed, HyperliquidFeed, MexcFeed
from .radar import Radar
from .repository import Repository, Writer
from .state import Hub
from .sysinfo import rss_mb

log = logging.getLogger("runtime")


class _TTLCache:
    """Protects the 3-connection pool from dashboard polling."""

    def __init__(self) -> None:
        self._data: dict[str, tuple[float, Any]] = {}

    async def get(self, key: str, ttl: float, factory: Any) -> Any:
        now = time.monotonic()
        hit = self._data.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
        value = await factory()
        self._data[key] = (now, value)
        return value


class Runtime:
    def __init__(self, cfg: Settings) -> None:
        self.cfg = cfg
        self.engine = build_engine(cfg.database_url)
        self.repo = Repository(self.engine, cfg)
        self.writer = Writer(self.repo)
        self.hub = Hub(cfg, self.writer)
        self.cache = _TTLCache()
        self.session: aiohttp.ClientSession | None = None
        self.tasks: list[asyncio.Task] = []
        self.feeds: list = []

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=15, connect=8)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={"User-Agent": "goldilocks-arb/1.0", "Accept": "application/json"},
            connector=aiohttp.TCPConnector(limit=12, ttl_dns_cache=300),
        )
        cfg, hub, s = self.cfg, self.hub, self.session
        self.feeds = [
            HyperliquidFeed(cfg, hub, s, cfg.hl_ws),
            BybitFeed(cfg, hub, s, cfg.bybit_ws),
            MexcFeed(cfg, hub, s, cfg.mexc_ws),
        ]
        radar = Radar(cfg, hub, self.writer, s, self.feeds)
        coros = {
            "db-init": self.repo.init_schema_forever(),
            "writer": self.writer.run(),
            "radar": radar.run_forever(),
            "housekeeping": self._housekeeping(),
            **{f"feed-{f.name}": f.run() for f in self.feeds},
        }
        self.tasks = [asyncio.create_task(c, name=n) for n, c in coros.items()]
        log.info("runtime started: %s", ", ".join(coros))

    async def stop(self) -> None:
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.session:
            await self.session.close()
        await self.engine.dispose()

    async def _housekeeping(self) -> None:
        await self.repo.ready.wait()
        while True:
            try:
                await self.repo.rollup_and_purge()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("roll-up failed")
            gc.collect()
            await asyncio.sleep(3600)

    # ---- cached read models used by the API --------------------------------------
    async def today(self) -> dict[str, Any]:
        async def f() -> dict[str, Any]:
            return await self.repo.today_summary()

        return await self.cache.get("today", 4.0, f)

    async def history(self, days: int) -> list[dict[str, Any]]:
        async def f() -> list[dict[str, Any]]:
            return await self.repo.history(days)

        return await self.cache.get(f"history:{days}", 15.0, f)

    def health(self) -> dict[str, Any]:
        h = self.hub
        return {
            "status": "alive",
            "tracked_coins": h.tracked_coins(),
            "ram_mb": rss_mb(),
            "uptime_s": int((time.time_ns() // 1_000_000 - h.started_ms) / 1000),
            "exchanges_ws": dict(h.exchange_up),
            "exchanges_rest": dict(h.exchange_rest_ok),
            "open_gaps": len(h.open_gaps),
            "ticks": h.ticks,
            "gaps_closed_session": h.gaps_closed,
            "db_ready": self.repo.ready.is_set(),
            "rows_written": self.writer.rows_written,
        }
