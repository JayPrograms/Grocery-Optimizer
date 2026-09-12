# Grocery Savings Optimizer

This MVP accepts fixture flyer deals, normalizes product descriptions through a
deterministic embedding path with a structured offline LLM fallback, persists the
normalized state, projects it through Kafka, and recommends the best single nearby
store after estimated round-trip fuel cost. The browser client displays the matched
items and the grocery-savings, distance, fuel-cost, and net-savings breakdown.

## Run the complete MVP locally

Prerequisites: Docker Desktop with Compose, Python 3.11 or newer, and Node/npm for
running tests outside containers.

Create the local configuration. The checked-in example uses development-only
credentials, a static ingestion token, and one-second retry delays so the retry
acceptance fixture finishes quickly.

```powershell
Copy-Item .env.example .env
```

Build and start Kafka, PostgreSQL with pgvector, migrations, all workers and APIs, and
the Next.js client:

```powershell
docker compose up --build --detach --wait
docker compose ps --all
```

Submit the included flyer through the authenticated ingestion API:

```powershell
curl.exe -X POST http://127.0.0.1:8001/internal/v1/deal-batches `
  -H "Authorization: Bearer local-development-token" `
  -H "Content-Type: application/json" `
  --data-binary '@fixtures/flyer.json'
```

The endpoint returns `202 Accepted`; normalization and projection are asynchronous.
Open `http://127.0.0.1:3000`, enter `bread`, latitude `43.6532`, longitude `-79.3832`,
and submit. The local fixture recommends **Fixture Market Downtown** and displays a
`$1.50` grocery-savings and net-savings breakdown at zero distance.

Available HTTP endpoints:

- Web client: `http://127.0.0.1:3000`
- Ranking health: `http://127.0.0.1:8000/health/live` and `/health/ready`
- Active deals: `GET http://127.0.0.1:8000/v1/deals`
- One active deal: `GET http://127.0.0.1:8000/v1/deals/{deal_id}`
- Recommendation: `POST http://127.0.0.1:8000/v1/optimizations`
- Ingestion health: `http://127.0.0.1:8001/health/live` and `/health/ready`
- Flyer ingestion: `POST http://127.0.0.1:8001/internal/v1/deal-batches`

Stop the stack without deleting its named data volumes:

```powershell
docker compose down
```

Use `docker compose down --volumes` only when you intentionally want to delete all
local Kafka and PostgreSQL data.

## Pipeline and local providers

Compose provisions these three-partition topics with the retention policies documented
in `docs/ARCHITECTURE.md`:

- `grocery.deals.raw.v1`
- `grocery.deals.normalize.retry.1m.v1`
- `grocery.deals.normalize.retry.10m.v1`
- `grocery.deals.normalized.v1`
- `grocery.deals.dlq.v1`

The local MVP is deliberately offline and deterministic. It uses a fixed 1536-value
embedding implementation and a structured fake LLM provider; no external AI key is
required. The fixture includes a `retry-once` deal. Set
`NORMALIZATION_FIXTURE_FAILURES=0` and restore retry delays to `60` and `600` seconds
when the acceptance failure injection is not wanted.

The deployment assumptions are configured with `CURRENCY`,
`FUEL_PRICE_PER_LITRE`, and `ROAD_FACTOR`. Every ingestion replica must have a unique
`INGESTION_TRANSACTIONAL_ID`; every ranking projector replica must likewise have a
unique `RANKING_PROJECTOR_TRANSACTIONAL_ID`.

## Tests and production build

Create a virtual environment and install every Python dependency:

```powershell
python -m venv .venv
& '.\.venv\Scripts\python.exe' -m pip install -e '.[infrastructure,dev]'
```

Install frontend dependencies, run its tests, and make a production build:

```powershell
Push-Location apps/web
npm ci
npm test
npm run build
Pop-Location
```

Run the non-container Python suite:

```powershell
& '.\.venv\Scripts\python.exe' -m pytest
```

With Compose running, enable every PostgreSQL, Kafka, Compose, and complete-pipeline
acceptance gate:

```powershell
$env:RUN_INFRA_SMOKE = '1'
$env:RUN_ACCEPTANCE = '1'
$env:TEST_RANKING_DATABASE_URL = 'postgresql://grocery_app:replace_me_for_local_development@127.0.0.1:55432/grocery_savings'
$env:TEST_KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
& '.\.venv\Scripts\python.exe' -m pytest
Remove-Item Env:RUN_INFRA_SMOKE
Remove-Item Env:RUN_ACCEPTANCE
Remove-Item Env:TEST_RANKING_DATABASE_URL
Remove-Item Env:TEST_KAFKA_BOOTSTRAP_SERVERS
```

The live suite creates uniquely named data and also exercises duplicate raw events,
both retry topics, malformed and unresolvable events, retry exhaustion, the DLQ,
expired-deal filtering, the frontend proxy, and exact recommendation totals.
