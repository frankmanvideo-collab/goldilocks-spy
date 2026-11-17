"""Async SQLAlchemy engine tuned for the Supabase Session/Transaction pooler (pgbouncer / Supavisor)."""
from __future__ import annotations

import logging
import ssl
import uuid
from urllib.parse import parse_qsl

from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

log = logging.getLogger("db")

# Query-string options that libpq understands but asyncpg does NOT.
_STRIP_QUERY_KEYS = {"sslmode", "pgbouncer", "connect_timeout", "supa", "options", "channel_binding"}


def normalise_url(raw: str) -> URL:
    """Accept postgres:// | postgresql:// | postgresql+asyncpg:// and return an asyncpg URL."""
    if not raw:
        raise RuntimeError(
            "DATABASE_URL is not set. Paste your Supabase *Session pooler* URI into the DATABASE_URL env var."
        )
    url = make_url(raw.strip())
    url = url.set(drivername="postgresql+asyncpg")
    clean = {k: v for k, v in url.query.items() if k.lower() not in _STRIP_QUERY_KEYS}
    return url.set(query=clean)


def _wants_ssl(url: URL, raw: str) -> bool:
    host = (url.host or "").lower()
    if host in {"localhost", "127.0.0.1", "::1", ""}:
        return False
    sslmode = dict(parse_qsl(raw.split("?", 1)[1])).get("sslmode", "require") if "?" in raw else "require"
    return sslmode != "disable"


def build_engine(raw_url: str) -> AsyncEngine:
    url = normalise_url(raw_url)

    connect_args: dict = {
        # --- Supabase pooler compatibility (CRITICAL) -----------------------------
        # pgbouncer/Supavisor in transaction mode cannot keep server-side prepared statements.
        "statement_cache_size": 0,             # asyncpg's own statement cache
        "prepared_statement_cache_size": 0,    # SQLAlchemy's asyncpg-adapter cache
        # Unique names avoid "prepared statement __asyncpg_stmt_1__ already exists" on shared backends.
        "prepared_statement_name_func": lambda: f"__asyncpg_{uuid.uuid4()}__",
        "server_settings": {"application_name": "goldilocks-arb"},
        "command_timeout": 30,
        "timeout": 15,
    }
    if _wants_ssl(url, raw_url):
        ctx = ssl.create_default_context()
        # Supabase poolers present a chain that is not in the default CA bundle on slim images.
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        connect_args["ssl"] = ctx

    engine = create_async_engine(
        url,
        pool_size=3,          # persistent connections
        max_overflow=2,       # burst headroom -> never more than 5 total
        pool_timeout=20,
        pool_pre_ping=True,   # pooler may recycle idle backends
        pool_recycle=240,
        connect_args=connect_args,
    )
    log.info("DB engine ready host=%s port=%s db=%s", url.host, url.port, url.database)
    return engine
