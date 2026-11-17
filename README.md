# Two-Stage Goldilocks Cross-Exchange Arbitrage & Daily Analytics Dashboard

Paper-trading radar for **Hyperliquid · Bybit · MEXC** perpetuals. No exchange API keys. Built for
Render's free tier (512 MB RAM / 0.1 vCPU) and Supabase's free 500 MB Postgres.

```
Stage 1  Goldilocks Wide Radar   (REST, every 30 s)
         match overlapping coins -> drop giants (>$80M/24h) & dead coins (<$1M/24h)
         -> score mid-caps -> lock Top-20 hot coins
                         |
Stage 2  Millisecond Sniper      (persistent WebSockets, Top-20 only)
         in-place `latest_ticks` -> O(1) Sell_Bid vs Buy_Ask on every tick
         gap = (Sell_Bid-Buy_Ask)/Buy_Ask > MIN_PROFIT_PCT  AND  >= $300 top-of-book on both sides
                         |
Storage guard            row written ONLY when a gap closes (opened_at..closed_at)
                         or funding diverges > 50% APR. Raw ticks never touch the DB.
```

## Folder structure

```
goldilocks-arb/
├── .env.example
├── .gitignore
├── .python-version
├── README.md
├── render.yaml
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── config.py            # env-driven settings
│   ├── db.py                # asyncpg + SQLAlchemy engine tuned for the Supabase pooler
│   ├── models.py            # gap_events, funding_events, daily_stats
│   ├── repository.py        # queries, roll-up/purge, CSV stream, batched Writer (storage guard)
│   ├── state.py             # Hub: latest_ticks (in place), O(1) gap engine, lifecycle
│   ├── exchanges_rest.py    # Stage-1 REST snapshots (HL / Bybit / MEXC)
│   ├── radar.py             # Stage-1 filter, scoring, Top-20, funding divergence
│   ├── runtime.py           # task orchestration + cached read models
│   ├── sysinfo.py           # RSS probe
│   ├── main.py              # FastAPI: /, /health, /api/*, /export.csv
│   ├── feeds/
│   │   ├── __init__.py
│   │   ├── base.py          # reconnect, heartbeat, watchdog, delta-based re-subscribe
│   │   ├── hyperliquid.py   # bbo (or l2Book)
│   │   ├── bybit.py         # tickers.<symbol> (bid1/ask1 + sizes)
│   │   └── mexc.py          # sub.depth.full (limit 5)
│   └── templates/
│       └── index.html       # dark, mobile-first dashboard
└── tests/
    ├── test_core.py         # offline unit tests (pytest -q)
    └── e2e_live_smoke.py    # live pipeline smoke test
```

## requirements.txt

```
fastapi==0.115.6
uvicorn==0.34.0
uvloop==0.21.0
httptools==0.6.4
SQLAlchemy[asyncio]==2.0.36
asyncpg==0.30.0
aiohttp==3.11.11
orjson==3.10.12
```

## Render commands

| Field | Value |
|---|---|
| Root Directory | `goldilocks-arb` (leave empty if this folder is the repo root) |
| Build Command | `pip install --upgrade pip && pip install -r requirements.txt` |
| Start Command | `uvicorn app.main:app --host 0.0.0.0 --port $PORT --workers 1 --loop uvloop --http httptools --no-access-log` |
| Health Check Path | `/health` |
| Region | **Frankfurt or Singapore** (Bybit/MEXC geo-block US IPs: Oregon/Ohio/Virginia return HTTP 403) |

`--workers 1` is mandatory: the sniper keeps one in-memory book.

## Environment variables

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | – (required) | Supabase **Session pooler** URI |
| `MIN_PROFIT_PCT` | `0.10` | Combined round-trip taker fees, in percent |
| `MIN_LIQUIDITY_USD` | `300` | Top-of-book notional required on both books |
| `PAPER_NOTIONAL_CAP_USD` | `1000` | Max simulated trade size per gap |
| `VOL_MIN_USD` / `VOL_MAX_USD` | `1e6` / `8e7` | Goldilocks 24h-volume band |
| `TOP_N` | `20` | Coins locked into the WebSocket sniper |
| `RADAR_INTERVAL_S` | `30` | Stage-1 refresh period |
| `FUNDING_APR_THRESHOLD_PCT` | `50` | Funding-divergence logging threshold |
| `FUNDING_LOG_COOLDOWN_S` | `3600` | Max one funding row per coin per hour |
| `MIN_GAP_DURATION_MS` | `0` | Ignore gaps shorter than this (latency model) |
| `RAW_RETENTION_DAYS` | `60` | Raw rows older than this are purged; daily roll-ups are kept forever |
| `HL_FEED` | `bbo` | Set `l2Book` to use Hyperliquid's L2 channel instead |

"Points": 1 paper-profit point = $1 of simulated net profit = `notional × (entry_spread − MIN_PROFIT)`.
Days are grouped in **UTC**.

## Step-by-step: Supabase Session Pooler URI

1. Supabase dashboard -> your project -> **Connect** (top bar) -> **Connection string** (or Project Settings -> Database).
2. Choose **Session pooler** (IPv4 compatible, port `5432`). Transaction pooler (port `6543`) also works; the engine is configured for both.
   Do **not** use the "Direct connection" host (`db.<ref>.supabase.co`): it is IPv6-only and fails on Render.
3. Copy the URI, e.g. `postgresql://postgres.<ref>:[YOUR-PASSWORD]@aws-0-<region>.pooler.supabase.com:5432/postgres`.
4. Replace `[YOUR-PASSWORD]` with your database password. Percent-encode special characters (`@`->`%40`, `#`->`%23`, `/`->`%2F`, `:`->`%3A`).
5. Paste it as `DATABASE_URL`. Tables are created automatically on first boot.

## Step-by-step: deploy on Render

1. Push this folder to a GitHub repo.
2. Render -> **New +** -> **Web Service** -> connect the repo (or **Blueprint** to use `render.yaml`).
3. Runtime **Python 3**, Region **Frankfurt** (or Singapore), Instance type **Free**.
4. Fill in Build/Start commands from the table above. Add env var `PYTHON_VERSION=3.11.9`.
5. Add `DATABASE_URL` (+ any overrides) under **Environment**.
6. Health Check Path: `/health`. Click **Create Web Service**.
7. When the deploy is live, open `https://<service>.onrender.com/` for the dashboard and `/health` to verify:
   `{"status":"alive","tracked_coins":[...],"ram_mb":...}`.

## Step-by-step: UptimeRobot keep-alive

Render's free web services sleep after 15 minutes without inbound traffic, which would kill the WebSockets.

1. uptimerobot.com -> **+ New monitor**.
2. Monitor type **HTTP(s)**, friendly name `goldilocks-arb`.
3. URL: `https://<service>.onrender.com/health`.
4. Monitoring interval: **5 minutes**.
5. Create. `/health` answers both `GET` and `HEAD`, never touches the database, and responds instantly.

## How storage stays tiny

* Hot loop writes nothing. A closed gap is one ~150-byte row; a funding event at most once per coin per hour.
* Completed UTC days are frozen into `daily_stats` (one row/day, kept forever); raw rows are purged after `RAW_RETENTION_DAYS`.
* Even 20,000 gaps/day for 60 days is ~180 MB; typical usage is orders of magnitude less.
* Pool: `pool_size=3, max_overflow=2`, `statement_cache_size=0`, `prepared_statement_cache_size=0`, unique prepared-statement names.

## Memory & CPU notes

* `latest_ticks[coin][exchange]` is a preallocated `__slots__` `Quote`; ticks overwrite it in place. Coins that rotate out are deleted.
  Observed RSS ~60 MB with live feeds; bounded by (<= 20 coins x 3 venues).
* Bybit uses the `tickers` topic (~100 ms) rather than `orderbook.1` (10 ms) to stay inside 0.1 vCPU.
* If one exchange is unreachable (e.g. geo-block) the radar keeps running on the others (needs >= 2).
* Dashboard JSON is cached (4 s / 15 s) so browser polling cannot exhaust the 3-connection pool.

## Local run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt pytest
export DATABASE_URL='postgresql://...'
uvicorn app.main:app --port 8000 --loop uvloop --http httptools
pytest -q                          # offline unit tests
python tests/e2e_live_smoke.py     # live pipeline smoke test (writes test rows to your DB!)
```

Endpoints: `/` dashboard · `/health` · `/api/today` · `/api/history?days=30` · `/api/live` ·
`/export.csv?kind=gaps|daily|funding&days=N`.

> Paper trading only. Taker fees, funding, withdrawal/transfer time and slippage beyond the top of book are not modelled
> beyond `MIN_PROFIT_PCT`; treat results as an opportunity-frequency study, not guaranteed PnL.
