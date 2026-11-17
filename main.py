"""FastAPI entrypoint: dashboard, JSON APIs, CSV export and the UptimeRobot keep-alive endpoint."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncIterator

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from .config import settings
from .runtime import Runtime

logging.basicConfig(
    level=getattr(logging, settings.log_level, logging.INFO),
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

TEMPLATE = Path(__file__).parent / "templates" / "index.html"
runtime: Runtime | None = None


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    global runtime
    runtime = Runtime(settings)
    await runtime.start()
    try:
        yield
    finally:
        await runtime.stop()


app = FastAPI(title="Goldilocks Cross-Exchange Arbitrage", lifespan=lifespan, docs_url=None, redoc_url=None)


def rt() -> Runtime:
    assert runtime is not None, "runtime not started"
    return runtime


@app.api_route("/health", methods=["GET", "HEAD"])
async def health() -> JSONResponse:
    # UptimeRobot (free) pings with HEAD or GET - both are served. No DB hit: stays instant.
    return JSONResponse(rt().health())


@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    return FileResponse(TEMPLATE, media_type="text/html", headers={"Cache-Control": "no-store"})


@app.get("/api/today")
async def api_today() -> JSONResponse:
    r = rt()
    try:
        data = await r.today()
        data["db"] = "ok"
    except Exception as exc:  # noqa: BLE001
        data = {"date": datetime.now(timezone.utc).date().isoformat(), "gap_count": 0, "avg_duration_ms": 0,
                "max_duration_ms": 0, "best_coin": None, "best_coin_profit": 0, "profit_points": 0,
                "funding_events": 0, "db": f"unavailable: {type(exc).__name__}"}
    data["open_gaps"] = len(r.hub.open_gaps)
    return JSONResponse(data)


@app.get("/api/history")
async def api_history(days: int = Query(30, ge=1, le=3650)) -> JSONResponse:
    try:
        return JSONResponse(await rt().history(days))
    except Exception:  # noqa: BLE001
        return JSONResponse([])


@app.get("/api/live")
async def api_live() -> JSONResponse:
    return JSONResponse(rt().hub.live_snapshot())


@app.get("/export.csv")
async def export_csv(
    kind: str = Query("gaps", pattern="^(gaps|funding|daily)$"),
    days: int | None = Query(None, ge=1, le=3650),
) -> StreamingResponse:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    return StreamingResponse(
        rt().repo.stream_csv(kind, days),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="goldilocks_{kind}_{stamp}.csv"'},
    )
