# Grocery Savings Optimizer

This change implements the ranking service's normalized-deal projection only. It consumes
the documented `grocery.deals.normalized.v1` topic (override with
`NORMALIZED_DEALS_TOPIC`), writes the ranking-owned PostgreSQL projection idempotently,
and exposes active deals through FastAPI. Recommendation and optimization behavior is
intentionally not included.

## Setup

```powershell
py -m venv .venv
.venv\Scripts\python -m pip install -e ".[test]"
```

Apply `infra/migrations/001_ranking_projection.sql` to a PostgreSQL database with
pgvector available, then set `RANKING_DATABASE_URL`.

Run the API and projector separately:

```powershell
.venv\Scripts\ranking-api.exe
.venv\Scripts\ranking-projector.exe
```

The read API provides `GET /v1/deals` (with optional store/product filters and
pagination), `GET /v1/deals/{deal_id}`, and health endpoints. Both deal endpoints use
the server's current UTC time and never return future or expired offers.

Run tests with:

```powershell
.venv\Scripts\python -m pytest
```

The PostgreSQL integration test is enabled when `TEST_RANKING_DATABASE_URL` points to
a migrated disposable database; otherwise that one test is skipped by the default suite.
