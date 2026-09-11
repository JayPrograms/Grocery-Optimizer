"""HTTP entry point for the ranking and recommendation API."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Literal, TypedDict

import psycopg
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from apps.ranking.api import register_deal_routes, register_optimization_route
from apps.ranking.config import Settings
from apps.ranking.repository import PostgresDealRepository


SERVICE_NAME = os.getenv("SERVICE_NAME", "ranking-api")


class HealthResponse(TypedDict):
    status: Literal["ok", "not_ready"]
    service: str
    dependencies: dict[str, str]


def check_postgres() -> None:
    """Open a short-lived connection and execute a side-effect-free query."""
    with psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=3,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            if cursor.fetchone() != (1,):
                raise RuntimeError("PostgreSQL readiness query returned an invalid result")


app = FastAPI(
    title="Grocery Savings Ranking API",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
settings = Settings.from_env()
settings.validate_recommendations()
app.state.settings = settings
app.state.clock = lambda: datetime.now(timezone.utc)
app.state.deal_repository = PostgresDealRepository(settings.database_url)


@app.get("/health/live")
def live() -> HealthResponse:
    return {"status": "ok", "service": SERVICE_NAME, "dependencies": {}}


@app.get("/health/ready", response_model=None)
def ready() -> HealthResponse | JSONResponse:
    try:
        check_postgres()
    except Exception:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": SERVICE_NAME,
                "dependencies": {"postgres": "unavailable"},
            },
        )
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "dependencies": {"postgres": "ok"},
    }


register_deal_routes(app)
register_optimization_route(app)


def run() -> None:
    import uvicorn

    uvicorn.run("apps.ranking.main:app", host="0.0.0.0", port=8000)

