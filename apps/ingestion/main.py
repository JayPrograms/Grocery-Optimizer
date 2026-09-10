"""Infrastructure-only shell for the ingestion API."""

from __future__ import annotations

import os
from typing import Literal, TypedDict

from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import JSONResponse


SERVICE_NAME = os.getenv("SERVICE_NAME", "ingestion-service")


class HealthResponse(TypedDict):
    status: Literal["ok", "not_ready"]
    service: str
    dependencies: dict[str, str]


def check_kafka() -> None:
    """Fetch broker metadata without producing or consuming any records."""
    client = AdminClient(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "socket.timeout.ms": 3_000,
        }
    )
    metadata = client.list_topics(timeout=3)
    if not metadata.brokers:
        raise RuntimeError("Kafka metadata contained no brokers")


app = FastAPI(
    title="Grocery Savings Ingestion API",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/health/live")
def live() -> HealthResponse:
    return {"status": "ok", "service": SERVICE_NAME, "dependencies": {}}


@app.get("/health/ready", response_model=None)
def ready() -> HealthResponse | JSONResponse:
    try:
        check_kafka()
    except Exception:
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": SERVICE_NAME,
                "dependencies": {"kafka": "unavailable"},
            },
        )
    return {
        "status": "ok",
        "service": SERVICE_NAME,
        "dependencies": {"kafka": "ok"},
    }

