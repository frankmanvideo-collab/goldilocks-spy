"""Stage 1 data sources: public REST snapshots from Hyperliquid, Bybit and MEXC (no API keys)."""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from typing import Any

import aiohttp
import orjson

from .config import Settings

log = logging.getLogger("rest")

# 1000PEPE / 1000000MOG style prefixes on Bybit & MEXC; "kPEPE" on Hyperliquid.
_MULT_PREFIX = re.compile(r"^(1(?:0{3,9}))([A-Z][A-Z0-9]*)$")
_HL_K = re.compile(r"^k([A-Z][A-Z0-9]+)$")


@dataclass(slots=True)
class VenueInfo:
    symbol: str
    mult: float
    contract_size: float
    vol24_usd: float
    bid: float          # normalised to 1 base coin; 0 when the REST payload has no BBO
    ask: float
    mid: float
    funding_apr_pct: float | None
    change_pct: float   # absolute 24h change in %
    range_pct: float | None  # (high-low)/last in % when available


def _f(v: Any, default: float = 0.0) -> float:
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def split_mult(base: str) -> tuple[str, float]:
    m = _MULT_PREFIX.match(base)
    if m:
        return m.group(2), float(m.group(1))
    return base, 1.0


async def _get_json(session: aiohttp.ClientSession, url: str, **kw: Any) -> Any:
    async with session.get(url, **kw) as r:
        body = await r.read()
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} from {url.split('?')[0]}: {body[:120]!r}")
        return orjson.loads(body)


# ---------------------------------------------------------------------------- Hyperliquid
async def fetch_hyperliquid(session: aiohttp.ClientSession, cfg: Settings) -> dict[str, VenueInfo]:
    async with session.post(cfg.hl_rest, json={"type": "metaAndAssetCtxs"}) as r:
        body = await r.read()
        if r.status != 200:
            raise RuntimeError(f"HTTP {r.status} from Hyperliquid: {body[:120]!r}")
    meta, ctxs = orjson.loads(body)
    out: dict[str, VenueInfo] = {}
    for asset, ctx in zip(meta["universe"], ctxs):
        if asset.get("isDelisted"):
            continue
        name: str = asset["name"]
        mult = 1.0
        base = name
        km = _HL_K.match(name)
        if km:
            base, mult = km.group(1), 1000.0
        mark = _f(ctx.get("markPx"))
        mid = _f(ctx.get("midPx")) or mark
        if mid <= 0:
            continue
        prev = _f(ctx.get("prevDayPx"))
        change = abs(mark - prev) / prev * 100.0 if prev > 0 and mark > 0 else 0.0
        funding_hourly = ctx.get("funding")
        out[base] = VenueInfo(
            symbol=name,
            mult=mult,
            contract_size=1.0,
            vol24_usd=_f(ctx.get("dayNtlVlm")),
            bid=0.0,
            ask=0.0,
            mid=mid / mult,
            funding_apr_pct=_f(funding_hourly) * 24 * 365 * 100 if funding_hourly is not None else None,
            change_pct=change,
            range_pct=None,
        )
    return out


# ---------------------------------------------------------------------------- Bybit
async def fetch_bybit(session: aiohttp.ClientSession, cfg: Settings) -> dict[str, VenueInfo]:
    data = await _get_json(session, f"{cfg.bybit_rest}/v5/market/tickers", params={"category": "linear"})
    if data.get("retCode") != 0:
        raise RuntimeError(f"Bybit retCode={data.get('retCode')} {data.get('retMsg')}")
    out: dict[str, VenueInfo] = {}
    for t in data["result"]["list"]:
        sym: str = t.get("symbol", "")
        if not sym.endswith("USDT") or "-" in sym:  # skip USDC perps and dated futures
            continue
        base, mult = split_mult(sym[:-4])
        last = _f(t.get("lastPrice"))
        if last <= 0:
            continue
        bid, ask = _f(t.get("bid1Price")), _f(t.get("ask1Price"))
        hi, lo = _f(t.get("highPrice24h")), _f(t.get("lowPrice24h"))
        interval_h = _f(t.get("fundingIntervalHour"), 8.0) or 8.0
        fr = t.get("fundingRate")
        out[base] = VenueInfo(
            symbol=sym,
            mult=mult,
            contract_size=1.0,
            vol24_usd=_f(t.get("turnover24h")),
            bid=bid / mult,
            ask=ask / mult,
            mid=((bid + ask) / 2 if bid > 0 and ask > 0 else last) / mult,
            funding_apr_pct=_f(fr) * (24.0 / interval_h) * 365 * 100 if fr not in (None, "") else None,
            change_pct=abs(_f(t.get("price24hPcnt"))) * 100.0,
            range_pct=(hi - lo) / last * 100.0 if hi > 0 and lo > 0 else None,
        )
    return out


# ---------------------------------------------------------------------------- MEXC
_mexc_detail: dict[str, tuple[str, float]] = {}  # symbol -> (baseCoin, contractSize); <1000 entries
_mexc_detail_ts = 0.0


async def _refresh_mexc_detail(session: aiohttp.ClientSession, cfg: Settings) -> None:
    global _mexc_detail_ts
    if _mexc_detail and time.monotonic() - _mexc_detail_ts < 1800:
        return
    data = await _get_json(session, f"{cfg.mexc_rest}/api/v1/contract/detail")
    fresh: dict[str, tuple[str, float]] = {}
    for c in data.get("data", []):
        if c.get("state") != 0 or c.get("quoteCoin") != "USDT" or c.get("isHidden"):
            continue
        size = _f(c.get("contractSize"))
        if size > 0:
            fresh[c["symbol"]] = (str(c.get("baseCoin", "")).upper(), size)
    _mexc_detail.clear()
    _mexc_detail.update(fresh)
    _mexc_detail_ts = time.monotonic()


async def fetch_mexc(session: aiohttp.ClientSession, cfg: Settings) -> dict[str, VenueInfo]:
    await _refresh_mexc_detail(session, cfg)
    data = await _get_json(session, f"{cfg.mexc_rest}/api/v1/contract/ticker")
    if not data.get("success", False):
        raise RuntimeError(f"MEXC error: {data.get('code')} {data.get('message')}")
    out: dict[str, VenueInfo] = {}
    for t in data.get("data", []):
        sym = t.get("symbol", "")
        det = _mexc_detail.get(sym)
        if det is None:
            continue
        base_raw, csize = det
        base, mult = split_mult(base_raw)
        last = _f(t.get("lastPrice"))
        if last <= 0:
            continue
        bid, ask = _f(t.get("bid1")), _f(t.get("ask1"))
        vol_usd = _f(t.get("amount24")) or _f(t.get("volume24")) * csize * last
        hi, lo = _f(t.get("high24Price")), _f(t.get("lower24Price"))
        fr = t.get("fundingRate")
        out[base] = VenueInfo(
            symbol=sym,
            mult=mult,
            contract_size=csize,
            vol24_usd=vol_usd,
            bid=bid / mult,
            ask=ask / mult,
            mid=((bid + ask) / 2 if bid > 0 and ask > 0 else last) / mult,
            funding_apr_pct=_f(fr) * 3 * 365 * 100 if fr is not None else None,  # 8h cycle
            change_pct=abs(_f(t.get("riseFallRate"))) * 100.0,
            range_pct=(hi - lo) / last * 100.0 if hi > 0 and lo > 0 else None,
        )
    return out


FETCHERS = {
    "hyperliquid": fetch_hyperliquid,
    "bybit": fetch_bybit,
    "mexc": fetch_mexc,
}
