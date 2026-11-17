"""All database access: batched writer (storage guard), dashboard queries, roll-up/purge, CSV streaming."""
from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncIterator

from sqlalchemy import insert, text
from sqlalchemy.ext.asyncio import AsyncEngine

from .config import Settings
from .models import funding_events, gap_events, metadata

log = logging.getLogger("repo")

UTC = timezone.utc


def utc_today() -> date:
    return datetime.now(UTC).date()


class Repository:
    def __init__(self, engine: AsyncEngine, cfg: Settings) -> None:
        self.engine = engine
        self.cfg = cfg
        self.ready = asyncio.Event()

    # ------------------------------------------------------------------ bootstrap
    async def init_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        self.ready.set()
        log.info("schema ready")

    async def init_schema_forever(self) -> None:
        """Retry until the DB is reachable. The trading engine never waits on this."""
        delay = 2.0
        while not self.ready.is_set():
            try:
                await self.init_schema()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("DB not ready (%s: %s); retrying in %.0fs", type(exc).__name__, exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60.0)

    async def ping(self) -> bool:
        try:
            async with self.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:  # noqa: BLE001
            return False

    # ------------------------------------------------------------------ writes
    async def insert_gaps(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            async with self.engine.begin() as conn:
                await conn.execute(insert(gap_events), rows)

    async def insert_funding(self, rows: list[dict[str, Any]]) -> None:
        if rows:
            async with self.engine.begin() as conn:
                await conn.execute(insert(funding_events), rows)

    # ------------------------------------------------------------------ dashboard reads
    async def today_summary(self, day: date | None = None) -> dict[str, Any]:
        day = day or utc_today()
        async with self.engine.connect() as conn:
            agg = (
                await conn.execute(
                    text(
                        """
                        SELECT count(*)                           AS gaps,
                               coalesce(avg(duration_ms), 0)      AS avg_ms,
                               coalesce(max(duration_ms), 0)      AS max_ms,
                               coalesce(sum(paper_profit_usd), 0) AS profit
                        FROM gap_events WHERE event_day = :d
                        """
                    ),
                    {"d": day},
                )
            ).mappings().one()
            best = (
                await conn.execute(
                    text(
                        """
                        SELECT coin, sum(paper_profit_usd) AS profit, count(*) AS n
                        FROM gap_events WHERE event_day = :d
                        GROUP BY coin ORDER BY profit DESC, n DESC LIMIT 1
                        """
                    ),
                    {"d": day},
                )
            ).mappings().first()
            funding = (
                await conn.execute(text("SELECT count(*) FROM funding_events WHERE event_day = :d"), {"d": day})
            ).scalar_one()
        return {
            "date": day.isoformat(),
            "gap_count": int(agg["gaps"]),
            "avg_duration_ms": round(float(agg["avg_ms"]), 1),
            "max_duration_ms": int(agg["max_ms"]),
            "best_coin": best["coin"] if best else None,
            "best_coin_profit": round(float(best["profit"]), 4) if best else 0.0,
            "profit_points": round(float(agg["profit"]), 4),
            "funding_events": int(funding),
        }

    async def history(self, days: int = 30) -> list[dict[str, Any]]:
        days = max(1, min(days, 3650))
        since = utc_today() - timedelta(days=days - 1)
        async with self.engine.connect() as conn:
            live = (
                await conn.execute(
                    text(
                        """
                        SELECT g.event_day AS day,
                               count(*) AS gap_count,
                               avg(g.duration_ms) AS avg_duration_ms,
                               max(g.duration_ms) AS max_duration_ms,
                               sum(g.paper_profit_usd) AS profit_points,
                               (SELECT x.coin FROM gap_events x
                                 WHERE x.event_day = g.event_day
                                 GROUP BY x.coin ORDER BY sum(x.paper_profit_usd) DESC LIMIT 1) AS top_coin
                        FROM gap_events g
                        WHERE g.event_day >= :since
                        GROUP BY g.event_day
                        """
                    ),
                    {"since": since},
                )
            ).mappings().all()
            rolled = (
                await conn.execute(
                    text(
                        """
                        SELECT day, gap_count, avg_duration_ms, max_duration_ms, profit_points, top_coin
                        FROM daily_stats WHERE day >= :since
                        """
                    ),
                    {"since": since},
                )
            ).mappings().all()

        merged: dict[date, dict[str, Any]] = {}
        for r in rolled:  # permanent roll-ups first ...
            merged[r["day"]] = dict(r)
        for r in live:  # ... raw rows win while they still exist
            merged[r["day"]] = dict(r)

        out = []
        for day in sorted(merged, reverse=True):
            r = merged[day]
            out.append(
                {
                    "date": day.isoformat(),
                    "gap_count": int(r["gap_count"]),
                    "avg_duration_ms": round(float(r["avg_duration_ms"] or 0), 1),
                    "max_duration_ms": int(r["max_duration_ms"] or 0),
                    "top_coin": r["top_coin"],
                    "profit_points": round(float(r["profit_points"] or 0), 4),
                }
            )
        return out

    # ------------------------------------------------------------------ housekeeping
    async def rollup_and_purge(self) -> None:
        """Freeze every completed UTC day into daily_stats, then purge raw rows past retention."""
        if not self.ready.is_set():
            return
        today = utc_today()
        cutoff = today - timedelta(days=self.cfg.raw_retention_days)
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO daily_stats (day, gap_count, avg_duration_ms, max_duration_ms, top_coin, profit_points)
                    SELECT d.day, d.cnt, d.avg_ms, d.max_ms, tc.coin, d.profit
                    FROM (
                        SELECT event_day AS day, count(*) AS cnt, avg(duration_ms) AS avg_ms,
                               max(duration_ms) AS max_ms, sum(paper_profit_usd) AS profit
                        FROM gap_events WHERE event_day < :today GROUP BY event_day
                    ) d
                    LEFT JOIN LATERAL (
                        SELECT g.coin FROM gap_events g WHERE g.event_day = d.day
                        GROUP BY g.coin ORDER BY sum(g.paper_profit_usd) DESC LIMIT 1
                    ) tc ON true
                    ON CONFLICT (day) DO NOTHING
                    """
                ),
                {"today": today},
            )
            purged = await conn.execute(text("DELETE FROM gap_events WHERE event_day < :c"), {"c": cutoff})
            purged_f = await conn.execute(text("DELETE FROM funding_events WHERE event_day < :c"), {"c": cutoff})
        log.info(
            "roll-up done; purged gap_events=%s funding_events=%s (cutoff %s)",
            purged.rowcount,
            purged_f.rowcount,
            cutoff,
        )

    # ------------------------------------------------------------------ CSV
    async def stream_csv(self, kind: str = "gaps", days: int | None = None) -> AsyncIterator[str]:
        """Keyset-paginated so memory stays flat no matter how many rows exist."""
        since = utc_today() - timedelta(days=days - 1) if days else date(1970, 1, 1)

        def render(header: list[str] | None, rows: list[list[Any]]) -> str:
            buf = io.StringIO()
            w = csv.writer(buf)
            if header:
                w.writerow(header)
            w.writerows(rows)
            return buf.getvalue()

        if kind == "daily":
            hist = await self.history(days or 3650)
            yield render(
                ["date", "gap_count", "avg_duration_ms", "max_duration_ms", "top_coin", "profit_points"],
                [
                    [h["date"], h["gap_count"], h["avg_duration_ms"], h["max_duration_ms"], h["top_coin"], h["profit_points"]]
                    for h in hist
                ],
            )
            return

        if kind == "funding":
            header = ["id", "logged_at", "coin", "long_exchange", "short_exchange", "long_apr_pct", "short_apr_pct", "divergence_apr_pct"]
            sql = text(
                """
                SELECT id, logged_at, coin, long_exchange, short_exchange, long_apr_pct, short_apr_pct, divergence_apr_pct
                FROM funding_events WHERE id > :last AND event_day >= :since ORDER BY id LIMIT 2000
                """
            )
        else:
            header = [
                "id", "coin", "buy_exchange", "sell_exchange", "opened_at", "closed_at", "duration_ms",
                "entry_spread_pct", "peak_spread_pct", "notional_usd", "paper_profit_usd",
            ]
            sql = text(
                """
                SELECT id, coin, buy_exchange, sell_exchange, opened_at, closed_at, duration_ms,
                       entry_spread_pct, peak_spread_pct, notional_usd, paper_profit_usd
                FROM gap_events WHERE id > :last AND event_day >= :since ORDER BY id LIMIT 2000
                """
            )

        yield render(header, [])
        last = 0
        while True:
            async with self.engine.connect() as conn:
                rows = (await conn.execute(sql, {"last": last, "since": since})).all()
            if not rows:
                break
            last = rows[-1][0]
            yield render(None, [[v.isoformat(timespec="milliseconds") if isinstance(v, datetime) else v for v in r] for r in rows])


class Writer:
    """Storage guard: the ONLY path into the database from the hot loop.

    The hub enqueues a row solely when (a) a net-profitable gap has closed or (b) a funding
    divergence above the APR threshold is logged. Raw ticks never reach this class.
    """

    def __init__(self, repo: Repository) -> None:
        self.repo = repo
        self.q: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=5000)
        self.rows_written = 0
        self.rows_dropped = 0

    def put_gap(self, row: dict[str, Any]) -> None:
        self._put(("gap", row))

    def put_funding(self, row: dict[str, Any]) -> None:
        self._put(("funding", row))

    def _put(self, item: tuple[str, dict[str, Any]]) -> None:
        try:
            self.q.put_nowait(item)
        except asyncio.QueueFull:
            self.rows_dropped += 1
            log.error("writer queue full; dropping %s row", item[0])

    async def run(self) -> None:
        await self.repo.ready.wait()
        while True:
            first = await self.q.get()
            batch = [first]
            await asyncio.sleep(0.25)  # coalesce bursts into one round trip
            while len(batch) < 200 and not self.q.empty():
                batch.append(self.q.get_nowait())
            gaps = [r for k, r in batch if k == "gap"]
            funds = [r for k, r in batch if k == "funding"]
            delay = 1.0
            for attempt in range(1, 7):
                try:
                    await self.repo.insert_gaps(gaps)
                    gaps = []
                    await self.repo.insert_funding(funds)
                    funds = []
                    self.rows_written += len(batch)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.warning("insert failed (attempt %d): %s: %s", attempt, type(exc).__name__, exc)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, 30.0)
            else:
                self.rows_dropped += len(gaps) + len(funds)
                log.error("giving up on %d rows", len(gaps) + len(funds))
