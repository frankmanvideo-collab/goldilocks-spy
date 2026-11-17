"""In-memory state + the O(1) gap engine.

Memory discipline
-----------------
* `latest_ticks[coin][exchange]` holds ONE preallocated `Quote` (``__slots__``) per live pair.
  Every incoming tick overwrites its fields in place - no per-tick allocation, no history.
* The dictionary only ever contains the current Top-N coins (<= 20 x 3 quotes); coins that rotate
  out are deleted, so RAM is bounded by construction.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from .config import Settings
from .repository import Writer

log = logging.getLogger("hub")
UTC = timezone.utc

EXCHANGES = ("hyperliquid", "bybit", "mexc")


@dataclass(slots=True)
class VenueSpec:
    """How to subscribe to / normalise one coin on one exchange."""

    coin: str
    symbol: str          # exchange-native symbol (HL: coin name, Bybit: XYZUSDT, MEXC: XYZ_USDT)
    mult: float = 1.0    # price multiplier, e.g. 1000 for kPEPE / 1000PEPEUSDT
    contract_size: float = 1.0  # base units per contract (MEXC)


@dataclass(slots=True)
class Target:
    """A Top-N Goldilocks coin selected by the radar."""

    coin: str
    score: float
    price_div_bps: float
    exec_bps: float
    vol_pct: float
    funding_div_apr: float
    volumes: dict[str, float]
    venues: dict[str, VenueSpec] = field(default_factory=dict)


class Quote:
    """Top of book for one (coin, exchange). Prices are normalised to 1 unit of the base coin."""

    __slots__ = ("bid", "bid_usd", "ask", "ask_usd", "valid", "recv_ms")

    def __init__(self) -> None:
        self.bid = 0.0
        self.bid_usd = 0.0
        self.ask = 0.0
        self.ask_usd = 0.0
        self.valid = False
        self.recv_ms = 0


class OpenGap:
    __slots__ = ("coin", "buy_ex", "sell_ex", "opened_wall_ms", "opened_mono_ns", "entry_spread", "peak_spread", "notional")

    def __init__(self, coin: str, buy_ex: str, sell_ex: str, spread: float, notional: float) -> None:
        self.coin = coin
        self.buy_ex = buy_ex
        self.sell_ex = sell_ex
        self.opened_wall_ms = time.time_ns() // 1_000_000
        self.opened_mono_ns = time.perf_counter_ns()
        self.entry_spread = spread
        self.peak_spread = spread
        self.notional = notional


class Hub:
    def __init__(self, cfg: Settings, writer: Writer) -> None:
        self.cfg = cfg
        self.writer = writer
        self.latest_ticks: dict[str, dict[str, Quote]] = {}
        self.targets: dict[str, Target] = {}
        self.open_gaps: dict[tuple[str, str, str], OpenGap] = {}
        self.exchange_up: dict[str, bool] = {ex: False for ex in EXCHANGES}
        self.exchange_rest_ok: dict[str, bool] = {ex: False for ex in EXCHANGES}
        self.ticks = 0
        self.gaps_closed = 0
        self.last_radar_ms = 0
        self.radar_universe = 0
        self.radar_eligible = 0
        self.started_ms = time.time_ns() // 1_000_000

    # ------------------------------------------------------------------ target rotation
    def set_targets(self, new_targets: list[Target]) -> tuple[list[str], list[str]]:
        """Swap the Top-N set in place. Returns (added, removed) coins."""
        new_map = {t.coin: t for t in new_targets}
        removed = [c for c in self.targets if c not in new_map]
        added = [c for c in new_map if c not in self.targets]

        for coin in removed:
            self._close_matching(lambda g, c=coin: g.coin == c, reason="rotated-out")
            self.latest_ticks.pop(coin, None)
        for coin, tgt in new_map.items():
            book = self.latest_ticks.setdefault(coin, {})
            for ex in tgt.venues:
                if ex not in book:
                    book[ex] = Quote()
            for ex in [e for e in book if e not in tgt.venues]:
                self._close_matching(lambda g, c=coin, e=ex: g.coin == c and e in (g.buy_ex, g.sell_ex), "venue-dropped")
                del book[ex]
        self.targets = new_map
        return added, removed

    # ------------------------------------------------------------------ hot path (called per tick)
    def on_book(self, coin: str, ex: str, bid: float, bid_usd: float, ask: float, ask_usd: float) -> None:
        book = self.latest_ticks.get(coin)
        if book is None:
            return
        q = book.get(ex)
        if q is None:
            return
        self.ticks += 1
        if bid <= 0.0 or ask <= 0.0 or ask < bid * 0.5:  # empty / crossed-garbage book
            if q.valid:
                q.valid = False
                self._close_matching(lambda g: g.coin == coin and ex in (g.buy_ex, g.sell_ex), "book-invalid")
            return
        # overwrite in place - zero allocation
        q.bid = bid
        q.bid_usd = bid_usd
        q.ask = ask
        q.ask_usd = ask_usd
        q.valid = True
        q.recv_ms = time.time_ns() // 1_000_000

        # O(1): at most 2 other venues x 2 directions
        for other_ex, oq in book.items():
            if other_ex == ex or not oq.valid:
                continue
            self._eval(coin, ex, other_ex, q, oq)   # buy here, sell there
            self._eval(coin, other_ex, ex, oq, q)   # and the reverse direction

    def _eval(self, coin: str, buy_ex: str, sell_ex: str, buy_q: Quote, sell_q: Quote) -> None:
        cfg = self.cfg
        spread = (sell_q.bid - buy_q.ask) / buy_q.ask  # (Sell_Bid - Buy_Ask) / Buy_Ask
        key = (coin, buy_ex, sell_ex)
        is_gap = (
            cfg.min_profit < spread < cfg.max_spread_sanity
            and buy_q.ask_usd >= cfg.min_liquidity_usd
            and sell_q.bid_usd >= cfg.min_liquidity_usd
        )
        gap = self.open_gaps.get(key)
        if is_gap:
            if gap is None:
                notional = min(buy_q.ask_usd, sell_q.bid_usd, cfg.paper_notional_cap_usd)
                self.open_gaps[key] = OpenGap(coin, buy_ex, sell_ex, spread, notional)
            elif spread > gap.peak_spread:
                gap.peak_spread = spread
        elif gap is not None:
            self._close(key, gap)

    # ------------------------------------------------------------------ gap lifecycle
    def _close(self, key: tuple[str, str, str], gap: OpenGap) -> None:
        self.open_gaps.pop(key, None)
        duration_ms = max(0, (time.perf_counter_ns() - gap.opened_mono_ns) // 1_000_000)
        if duration_ms < self.cfg.min_gap_duration_ms:
            return
        closed_wall_ms = gap.opened_wall_ms + duration_ms
        opened = datetime.fromtimestamp(gap.opened_wall_ms / 1000.0, UTC)
        closed = datetime.fromtimestamp(closed_wall_ms / 1000.0, UTC)
        net = gap.entry_spread - self.cfg.min_profit  # fees already priced in by the trigger
        profit = gap.notional * net
        self.gaps_closed += 1
        # STORAGE GUARD: this is the only place a gap row is produced.
        self.writer.put_gap(
            {
                "coin": gap.coin,
                "buy_exchange": gap.buy_ex,
                "sell_exchange": gap.sell_ex,
                "opened_at": opened,
                "closed_at": closed,
                "duration_ms": int(duration_ms),
                "entry_spread_pct": round(gap.entry_spread * 100.0, 5),
                "peak_spread_pct": round(gap.peak_spread * 100.0, 5),
                "notional_usd": round(gap.notional, 2),
                "paper_profit_usd": round(profit, 4),
                "event_day": closed.date(),
            }
        )
        log.info(
            "GAP CLOSED %s buy=%s sell=%s %dms spread=%.3f%% peak=%.3f%% paper=$%.2f",
            gap.coin, gap.buy_ex, gap.sell_ex, duration_ms, gap.entry_spread * 100, gap.peak_spread * 100, profit,
        )

    def _close_matching(self, pred: Any, reason: str) -> None:
        for key, gap in list(self.open_gaps.items()):
            if pred(gap):
                log.debug("closing %s (%s)", key, reason)
                self._close(key, gap)

    def invalidate_exchange(self, ex: str) -> None:
        """Feed dropped: its quotes are no longer trustworthy, so close any gap that depends on it."""
        self.exchange_up[ex] = False
        for book in self.latest_ticks.values():
            q = book.get(ex)
            if q is not None:
                q.valid = False
        self._close_matching(lambda g: ex in (g.buy_ex, g.sell_ex), "feed-down")

    # ------------------------------------------------------------------ read models for the dashboard
    def tracked_coins(self) -> list[str]:
        return sorted(self.targets)

    def live_snapshot(self) -> dict[str, Any]:
        fee = self.cfg.min_profit
        rows = []
        for coin, tgt in sorted(self.targets.items(), key=lambda kv: -kv[1].score):
            book = self.latest_ticks.get(coin, {})
            venues: dict[str, Any] = {}
            for ex in EXCHANGES:
                q = book.get(ex)
                if q is not None and q.valid:
                    venues[ex] = {
                        "bid": q.bid,
                        "ask": q.ask,
                        "bid_usd": round(q.bid_usd, 0),
                        "ask_usd": round(q.ask_usd, 0),
                        "spread_bps": round((q.ask - q.bid) / q.ask * 1e4, 2),
                    }
                else:
                    venues[ex] = None
            best: tuple[float, str, str] | None = None
            for bex, bq in book.items():
                if not bq.valid:
                    continue
                for sex, sq in book.items():
                    if sex == bex or not sq.valid:
                        continue
                    s = (sq.bid - bq.ask) / bq.ask
                    if best is None or s > best[0]:
                        best = (s, bex, sex)
            is_open = any(k[0] == coin for k in self.open_gaps)
            rows.append(
                {
                    "coin": coin,
                    "score": round(tgt.score, 2),
                    "volumes": {k: round(v) for k, v in tgt.volumes.items()},
                    "venues": venues,
                    "best_route": {"buy": best[1], "sell": best[2]} if best else None,
                    "gross_pct": round(best[0] * 100, 4) if best else None,
                    "net_pct": round((best[0] - fee) * 100, 4) if best else None,
                    "gap_open": is_open,
                }
            )
        now = time.perf_counter_ns()
        return {
            "rows": rows,
            "open_gaps": [
                {
                    "coin": g.coin,
                    "buy": g.buy_ex,
                    "sell": g.sell_ex,
                    "spread_pct": round(g.entry_spread * 100, 4),
                    "age_ms": (now - g.opened_mono_ns) // 1_000_000,
                }
                for g in self.open_gaps.values()
            ],
            "fee_pct": self.cfg.min_profit_pct,
            "min_liquidity_usd": self.cfg.min_liquidity_usd,
            "exchanges": dict(self.exchange_up),
            "last_radar_ms": self.last_radar_ms,
            "ticks": self.ticks,
        }
