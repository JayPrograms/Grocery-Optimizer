# Grocery Savings Optimizer

The repository currently contains the shared event contracts and the local infrastructure shell for the MVP. Product ingestion, normalization, and ranking features are not implemented yet.

## Local infrastructure

Prerequisites: Docker Desktop with Compose and Python 3.11 or newer.

Create a local environment file and replace the placeholder database password:

```powershell
Copy-Item .env.example .env
```

Start Kafka, PostgreSQL with pgvector, the ranking API health shell, the ingestion health shell, and the normalization worker health shell:

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

The normalization worker health endpoint is internal to the Compose network. No application feature endpoints exist yet.

Run all non-Docker tests:

```powershell
python -m pytest
```

With the Compose stack running, run the live infrastructure smoke tests:

```powershell
$env:RUN_INFRA_SMOKE = "1"
python -m pytest tests/smoke/test_local_infrastructure.py
Remove-Item Env:RUN_INFRA_SMOKE
```

Stop the stack without deleting its named data volumes:

```powershell
docker compose down
```
