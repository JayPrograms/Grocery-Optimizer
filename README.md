# Grocery Savings Optimizer

The repository contains the shared event contracts, local MVP infrastructure, the
ranking service's normalized-deal projection, and the single-store recommendation API.

## Local infrastructure

Prerequisites: Docker Desktop with Compose and Python 3.11 or newer.

Create a local environment file and replace the placeholder database password:

```powershell
Copy-Item .env.example .env
```

Start the local Kafka and PostgreSQL dependencies and application service shells:

```powershell
docker compose up --build --detach --wait
```

Inspect service status:

```powershell
docker compose ps
```

The exposed health endpoints are:

- ranking API: `http://127.0.0.1:8000/health/live` and `/health/ready`;
- ingestion service: `http://127.0.0.1:8001/health/live` and `/health/ready`.

The web client is available at `http://127.0.0.1:3000`. It forwards optimization
requests to the ranking API over the Compose network.

The ranking API also provides `GET /v1/deals` and `GET /v1/deals/{deal_id}`. Both
return only deals active at the server's current UTC time.

`POST /v1/optimizations` accepts an origin, grocery list, fuel efficiency, optional
per-request fuel price, and radius.
It reads active normalized deals and returns the single store with the highest estimated
net savings, or an empty ranking when no savings-eligible deal matches. Set `CURRENCY`,
`FUEL_PRICE_PER_LITRE`, and `ROAD_FACTOR` to configure its deployment assumptions.
When the request omits fuel price, the configured price remains the default.

Run the web client directly with:

```powershell
Set-Location apps/web
npm install
npm run dev
```

By default the development web server forwards to `http://127.0.0.1:8000`. Set
`RANKING_API_URL` to use a different ranking API.

## Ranking projection

Apply the SQL files in `infra/migrations` in numeric order. Configure the projector
with `RANKING_EMBEDDING_MODEL`, `RANKING_EMBEDDING_VERSION`, and
`RANKING_EMBEDDING_DIMENSION=1536`. Each projector replica requires a stable, unique
`RANKING_PROJECTOR_TRANSACTIONAL_ID`.

Run the API and projector directly with:

```powershell
python -m uvicorn apps.ranking.main:app --host 0.0.0.0 --port 8000
python -m apps.ranking.consumer
```

## Tests

Install the development and infrastructure dependencies, then run non-Docker tests:

```powershell
python -m pip install -e ".[infrastructure,dev]"
python -m pytest
```

Run the web tests and production build with:

```powershell
Set-Location apps/web
npm test
npm run build
```

With the Compose stack running, run the live infrastructure smoke tests:

```powershell
$env:RUN_INFRA_SMOKE = "1"
python -m pytest tests/smoke/test_local_infrastructure.py
Remove-Item Env:RUN_INFRA_SMOKE
```

The PostgreSQL and Kafka persistence integration tests are enabled with
`TEST_RANKING_DATABASE_URL` and `TEST_KAFKA_BOOTSTRAP_SERVERS`, respectively.

Stop the stack without deleting its named data volumes:

```powershell
docker compose down
```
