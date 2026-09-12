"""Authenticated ingestion API that publishes validated raw deals transactionally."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
import hmac
import json
import os
from typing import Any, AsyncIterator, Literal, TypedDict
from uuid import UUID, uuid4

from aiokafka import AIOKafkaProducer
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from event_contracts import RawDealEventV1, RawDealPayload


SERVICE_NAME = os.getenv("SERVICE_NAME", "ingestion-service")
RAW_TOPIC = os.getenv("RAW_DEALS_TOPIC", "grocery.deals.raw.v1")


class HealthResponse(TypedDict):
    status: Literal["ok", "not_ready"]
    service: str
    dependencies: dict[str, str]


class DealBatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    deals: list[dict[str, Any]] = Field(min_length=1, max_length=500)
    correlation_id: UUID = Field(default_factory=uuid4)
    trace_id: str = Field(default_factory=lambda: uuid4().hex, min_length=1, max_length=256)


class DealBatchAccepted(BaseModel):
    accepted: int
    correlation_id: UUID


def check_kafka() -> None:
    client = AdminClient(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "socket.timeout.ms": 3_000,
        }
    )
    metadata = client.list_topics(timeout=3)
    if not metadata.brokers:
        raise RuntimeError("Kafka metadata contained no brokers")


def _validated_payloads(batch: DealBatchRequest) -> list[RawDealPayload]:
    payloads: list[RawDealPayload] = []
    errors: list[dict[str, object]] = []
    for index, raw in enumerate(batch.deals):
        try:
            payloads.append(RawDealPayload.model_validate_json(json.dumps(raw)))
        except ValidationError as error:
            for issue in error.errors(include_url=False, include_input=False):
                errors.append({**issue, "loc": ["body", "deals", index, *issue["loc"]]})
    if errors:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=errors)
    return payloads


def _authorize(authorization: str | None) -> None:
    expected = os.getenv("INGESTION_TOKEN", "local-development-token")
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not authorization or not authorization.startswith("Bearer ") or not hmac.compare_digest(
        supplied, expected
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid service token",
            headers={"WWW-Authenticate": "Bearer"},
        )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        enable_idempotence=True,
        transactional_id=os.getenv("INGESTION_TRANSACTIONAL_ID", "ingestion-api-0"),
    )
    await producer.start()
    app.state.producer = producer
    try:
        yield
    finally:
        await producer.stop()


app = FastAPI(
    title="Grocery Savings Ingestion API",
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


@app.post(
    "/internal/v1/deal-batches",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=DealBatchAccepted,
)
async def ingest_batch(
    batch: DealBatchRequest,
    request: Request,
    authorization: str | None = Header(default=None),
) -> DealBatchAccepted:
    _authorize(authorization)
    payloads = _validated_payloads(batch)
    producer: AIOKafkaProducer = request.app.state.producer
    occurred_at = datetime.now(timezone.utc)
    await producer.begin_transaction()
    try:
        for payload in payloads:
            event = RawDealEventV1(
                event_id=uuid4(),
                event_type="grocery.deal.raw",
                schema_version=1,
                occurred_at=occurred_at,
                producer=SERVICE_NAME,
                correlation_id=batch.correlation_id,
                causation_id=None,
                trace_id=batch.trace_id,
                payload=payload,
            )
            key = f"{payload.source}:{payload.store.store_location_id}:{payload.source_deal_id}"
            await producer.send_and_wait(
                RAW_TOPIC,
                key=key.encode(),
                value=event.model_dump_json().encode(),
                headers=[
                    ("event_id", str(event.event_id).encode()),
                    ("event_type", event.event_type.encode()),
                    ("schema_version", b"1"),
                    ("correlation_id", str(event.correlation_id).encode()),
                    ("trace_id", event.trace_id.encode()),
                ],
            )
        await producer.commit_transaction()
    except Exception:
        await producer.abort_transaction()
        raise HTTPException(status_code=503, detail="Raw deal publication failed")
    return DealBatchAccepted(accepted=len(payloads), correlation_id=batch.correlation_id)
