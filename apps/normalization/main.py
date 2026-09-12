"""Infrastructure-only shell for the future normalization worker."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import logging
import os
from typing import AsyncIterator, Literal, TypedDict

import psycopg
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from apps.normalization.worker import serve


SERVICE_NAME = os.getenv("SERVICE_NAME", "normalization-worker")
WORKER_TASK: asyncio.Task[None] | None = None
LOGGER = logging.getLogger(__name__)


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


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    global WORKER_TASK
    WORKER_TASK = asyncio.create_task(serve())
    WORKER_TASK.add_done_callback(_log_worker_exit)
    try:
        yield
    finally:
        WORKER_TASK.cancel()
        with suppress(asyncio.CancelledError):
            await WORKER_TASK
        WORKER_TASK = None


def _log_worker_exit(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        LOGGER.error("normalization worker stopped", exc_info=error)


app = FastAPI(
    title="Grocery Savings Normalization Worker",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
)


@app.get("/health/live")
def live() -> HealthResponse:
    return {"status": "ok", "service": SERVICE_NAME, "dependencies": {}}


@app.get("/health/ready", response_model=None)
def ready() -> HealthResponse | JSONResponse:
    dependencies: dict[str, str] = {}
    checks = {"kafka": check_kafka, "postgres": check_postgres}

    for dependency, check in checks.items():
        try:
            check()
        except Exception:
            dependencies[dependency] = "unavailable"
        else:
            dependencies[dependency] = "ok"

    if WORKER_TASK is not None:
        dependencies["worker"] = "unavailable" if WORKER_TASK.done() else "ok"

    if "unavailable" in dependencies.values():
        return JSONResponse(
            status_code=503,
            content={
                "status": "not_ready",
                "service": SERVICE_NAME,
                "dependencies": dependencies,
            },
        )
    return {"status": "ok", "service": SERVICE_NAME, "dependencies": dependencies}

