"""Kafka consumers for raw deal normalization, retries, and terminal DLQ handling."""

from __future__ import annotations

import asyncio
import base64
from datetime import datetime, timedelta, timezone
import json
import os
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
import psycopg
from pydantic import ValidationError

from apps.normalization.pipeline import (
    NormalizationRepository,
    RecordMetadata,
    UnresolvableProduct,
)
from event_contracts import (
    DeadLetterEventV1,
    DeadLetterPayload,
    NormalizationRetryEventV1,
    NormalizationRetryPayload,
    RawDealEventV1,
    RetryFailure,
)


RAW_TOPIC = "grocery.deals.raw.v1"
RETRY_1M_TOPIC = "grocery.deals.normalize.retry.1m.v1"
RETRY_10M_TOPIC = "grocery.deals.normalize.retry.10m.v1"
DLQ_TOPIC = "grocery.deals.dlq.v1"


def _header(record: object, name: str) -> bytes | None:
    for key, value in getattr(record, "headers", ()) or ():
        if key == name:
            return value
    return None


def _parsed(value: bytes) -> dict[str, object] | None:
    try:
        result = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return result if isinstance(result, dict) else None


def _uuid_header(record: object, name: str) -> UUID | None:
    value = _header(record, name)
    try:
        return UUID(value.decode()) if value else None
    except (ValueError, UnicodeDecodeError):
        return None


def _text_header(record: object, name: str) -> str | None:
    value = _header(record, name)
    try:
        return value.decode()[:256] if value else None
    except UnicodeDecodeError:
        return None


def _dlq_value(
    record: object,
    *,
    attempts: int,
    stage: str,
    classification: str,
    code: str,
    message: str,
) -> bytes:
    now = datetime.now(timezone.utc)
    original = _parsed(record.value)
    raw = None if original is not None else base64.b64encode(record.value[:65_536]).decode()
    event = DeadLetterEventV1(
        event_id=uuid4(),
        event_type="grocery.deal.dead_lettered",
        schema_version=1,
        occurred_at=now,
        producer="normalization-service",
        correlation_id=_uuid_header(record, "correlation_id") or uuid4(),
        causation_id=_uuid_header(record, "event_id"),
        trace_id=_text_header(record, "trace_id") or "unavailable",
        payload=DeadLetterPayload(
            original_topic=record.topic,
            original_partition=record.partition,
            original_offset=record.offset,
            original_key=record.key.decode(errors="replace")[:512] if record.key else None,
            original_event=original,
            raw_value_base64=raw,
            failed_stage=stage,
            classification=classification,
            attempts=attempts,
            error_code=code,
            error_message=message[:2048],
            failed_at=now,
        ),
    )
    return event.model_dump_json().encode()


async def _transactional_handoff(
    producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
    record: object,
    group_id: str,
    *,
    topic: str,
    value: bytes,
    headers: list[tuple[str, bytes]],
) -> None:
    partition = TopicPartition(record.topic, record.partition)
    await producer.begin_transaction()
    try:
        await producer.send_and_wait(topic, key=record.key, value=value, headers=headers)
        await producer.send_offsets_to_transaction(
            {partition: OffsetAndMetadata(record.offset + 1, "")}, group_id
        )
        await producer.commit_transaction()
    except Exception:
        await producer.abort_transaction()
        raise


async def _retry_or_exhaust(
    producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
    record: object,
    group_id: str,
    event: RawDealEventV1,
    total_attempt: int,
    message: str,
) -> None:
    if total_attempt >= 3:
        value = _dlq_value(
            record,
            attempts=total_attempt,
            stage="normalize",
            classification="retries_exhausted",
            code="NORMALIZATION_RETRIES_EXHAUSTED",
            message=message,
        )
        await _transactional_handoff(
            producer,
            consumer,
            record,
            group_id,
            topic=DLQ_TOPIC,
            value=value,
            headers=[("event_type", b"grocery.deal.dead_lettered")],
        )
        return
    retry_topic = RETRY_1M_TOPIC if total_attempt == 1 else RETRY_10M_TOPIC
    delay_variable = (
        "NORMALIZATION_RETRY_1M_DELAY_SECONDS"
        if total_attempt == 1
        else "NORMALIZATION_RETRY_10M_DELAY_SECONDS"
    )
    delay = float(os.getenv(delay_variable, "60" if total_attempt == 1 else "600"))
    not_before = datetime.now(timezone.utc) + timedelta(seconds=delay)
    retry = NormalizationRetryEventV1(
        event_id=uuid4(),
        event_type="grocery.deal.normalization_retry",
        schema_version=1,
        occurred_at=datetime.now(timezone.utc),
        producer="normalization-service",
        correlation_id=event.correlation_id,
        causation_id=_uuid_header(record, "event_id") or event.event_id,
        trace_id=event.trace_id,
        payload=NormalizationRetryPayload(
            original_event=event,
            attempt=total_attempt,
            not_before=not_before,
            last_error=RetryFailure(
                classification="transient_database",
                code="NORMALIZATION_DEPENDENCY_FAILURE",
                message=message[:512],
            ),
        ),
    )
    headers = [
        ("event_id", str(retry.event_id).encode()),
        ("event_type", retry.event_type.encode()),
        ("schema_version", b"1"),
        ("correlation_id", str(retry.correlation_id).encode()),
        ("trace_id", retry.trace_id.encode()),
    ]
    await _transactional_handoff(
        producer,
        consumer,
        record,
        group_id,
        topic=retry_topic,
        value=retry.model_dump_json().encode(),
        headers=headers,
    )


async def _wait_until_due(due: datetime) -> None:
    seconds = (due - datetime.now(timezone.utc)).total_seconds()
    if seconds > 0:
        await asyncio.sleep(seconds)


async def serve_topic(topic: str, group_id: str, instance: str) -> None:
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    repository = NormalizationRepository()
    await repository.ensure_seed_catalog()
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        group_id=group_id,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        enable_idempotence=True,
        transactional_id=f"normalization-{instance}",
    )
    await consumer.start()
    await producer.start()
    try:
        async for record in consumer:
            partition = TopicPartition(record.topic, record.partition)
            try:
                if topic == RAW_TOPIC:
                    event = RawDealEventV1.model_validate_json(record.value)
                    total_attempt = 1
                else:
                    retry = NormalizationRetryEventV1.model_validate_json(record.value)
                    event = retry.payload.original_event
                    total_attempt = retry.payload.attempt + 1
            except (ValidationError, ValueError):
                value = _dlq_value(
                    record,
                    attempts=1,
                    stage="validate",
                    classification="terminal_invalid",
                    code="INVALID_RAW_DEAL_V1",
                    message="raw deal does not match schema v1",
                )
                await _transactional_handoff(
                    producer,
                    consumer,
                    record,
                    group_id,
                    topic=DLQ_TOPIC,
                    value=value,
                    headers=[("event_type", b"grocery.deal.dead_lettered")],
                )
                continue
            if topic != RAW_TOPIC:
                await _wait_until_due(retry.payload.not_before)
            try:
                fixture_failures = os.getenv("NORMALIZATION_FIXTURE_FAILURES", "0") == "1"
                if fixture_failures and event.payload.source_deal_id == "retry-exhaust":
                    raise psycopg.OperationalError("fixture transient failure")
                if (
                    fixture_failures
                    and event.payload.source_deal_id == "retry-once"
                    and total_attempt == 1
                ):
                    raise psycopg.OperationalError("fixture transient failure")
                await repository.process(
                    event, RecordMetadata(record.topic, record.partition, record.offset)
                )
            except UnresolvableProduct as error:
                value = _dlq_value(
                    record,
                    attempts=total_attempt,
                    stage="normalize",
                    classification="terminal_unresolvable",
                    code="NO_DEFENSIBLE_MATCH",
                    message=str(error),
                )
                await _transactional_handoff(
                    producer,
                    consumer,
                    record,
                    group_id,
                    topic=DLQ_TOPIC,
                    value=value,
                    headers=[("event_type", b"grocery.deal.dead_lettered")],
                )
            except psycopg.OperationalError as error:
                await _retry_or_exhaust(
                    producer,
                    consumer,
                    record,
                    group_id,
                    event,
                    total_attempt,
                    str(error),
                )
            else:
                await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})
    finally:
        await producer.stop()
        await consumer.stop()


async def serve() -> None:
    await asyncio.gather(
        serve_topic(RAW_TOPIC, "normalization-v1", "raw-0"),
        serve_topic(RETRY_1M_TOPIC, "normalization-retry-1m-v1", "retry-1m-0"),
        serve_topic(RETRY_10M_TOPIC, "normalization-retry-10m-v1", "retry-10m-0"),
    )


def run() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    run()
