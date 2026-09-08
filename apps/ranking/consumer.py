import asyncio
import base64
from dataclasses import dataclass
from enum import StrEnum
import json
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
import psycopg

from apps.ranking.config import Settings
from apps.ranking.projector import (
    EmbeddingCompatibility,
    InvalidNormalizedDeal,
    NormalizedDealProjector,
    RecordMetadata,
)
from apps.ranking.repository import PostgresDealRepository
from packages.event_contracts import DeadLetterEventV1, DeadLetterPayloadV1


class DatabaseFailureDisposition(StrEnum):
    RETRY = "retry"
    DEAD_LETTER = "dead_letter"
    FATAL = "fatal"


@dataclass(frozen=True, slots=True)
class DlqFailure:
    failed_stage: str
    classification: str
    error_code: str
    error_message: str


INVALID_SCHEMA_FAILURE = DlqFailure(
    failed_stage="validate",
    classification="terminal_invalid",
    error_code="INVALID_NORMALIZED_DEAL_V1",
    error_message="normalized deal does not match schema v1",
)


def classify_database_error(error: psycopg.Error) -> DatabaseFailureDisposition:
    sqlstate = error.sqlstate or ""
    if (
        isinstance(error, (psycopg.OperationalError, psycopg.InterfaceError))
        or sqlstate.startswith("08")
        or sqlstate in {"40001", "40P01", "55P03", "53300", "57P01", "57P02", "57P03"}
    ):
        return DatabaseFailureDisposition.RETRY
    if isinstance(error, (psycopg.DataError, psycopg.IntegrityError)) or sqlstate[:2] in {
        "22",
        "23",
    }:
        return DatabaseFailureDisposition.DEAD_LETTER
    return DatabaseFailureDisposition.FATAL


def database_retry_delay(attempt: int, *, base: float, maximum: float) -> float:
    return min(base * (2 ** max(attempt - 1, 0)), maximum)


def _header_uuid(record: object, name: str) -> UUID | None:
    for key, value in getattr(record, "headers", ()):
        if key == name and value:
            try:
                return UUID(value.decode("ascii"))
            except (ValueError, UnicodeDecodeError):
                return None
    return None


def _header_text(record: object, name: str) -> str | None:
    for key, value in getattr(record, "headers", ()):
        if key == name and value:
            try:
                return value.decode("utf-8")[:255]
            except UnicodeDecodeError:
                return None
    return None


def _parsed_object(value: bytes) -> dict[str, object] | None:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _build_dlq_event(
    record: object,
    *,
    event_id: UUID,
    failure: DlqFailure,
    original_event: dict[str, object] | None,
    raw_value_base64: str | None,
) -> DeadLetterEventV1:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    causation_id = _header_uuid(record, "event_id")
    return DeadLetterEventV1(
        event_id=event_id,
        event_type="grocery.deal.dead_lettered",
        schema_version=1,
        occurred_at=now,
        producer="ranking-projector",
        correlation_id=_header_uuid(record, "correlation_id") or uuid4(),
        causation_id=causation_id,
        trace_id=_header_text(record, "trace_id") or "unavailable",
        payload=DeadLetterPayloadV1(
            original_topic=record.topic,
            original_partition=record.partition,
            original_offset=record.offset,
            original_key=record.key.decode("utf-8", errors="replace")[:500]
            if record.key
            else None,
            original_event=original_event,
            raw_value_base64=raw_value_base64,
            failed_stage=failure.failed_stage,
            classification=failure.classification,
            attempts=1,
            error_code=failure.error_code,
            error_message=failure.error_message,
            failed_at=now,
        ),
    )


def _dlq_value(
    record: object,
    *,
    failure: DlqFailure = INVALID_SCHEMA_FAILURE,
    max_event_bytes: int = 900_000,
) -> tuple[bytes, UUID]:
    event_id = uuid4()
    parsed = _parsed_object(record.value)
    if parsed is not None:
        event = _build_dlq_event(
            record,
            event_id=event_id,
            failure=failure,
            original_event=parsed,
            raw_value_base64=None,
        )
        encoded = event.model_dump_json().encode()
        if len(encoded) <= max_event_bytes:
            return encoded, event_id

    empty_raw = _build_dlq_event(
        record,
        event_id=event_id,
        failure=failure,
        original_event=None,
        raw_value_base64="",
    ).model_dump_json().encode()
    available_base64_bytes = max_event_bytes - len(empty_raw)
    if available_base64_bytes < 0:
        raise ValueError("DLQ_MAX_EVENT_BYTES is too small for the dead-letter envelope")
    raw_byte_budget = (available_base64_bytes // 4) * 3
    raw_value = base64.b64encode(record.value[:raw_byte_budget]).decode("ascii")
    event = _build_dlq_event(
        record,
        event_id=event_id,
        failure=failure,
        original_event=None,
        raw_value_base64=raw_value,
    )
    encoded = event.model_dump_json().encode()
    while len(encoded) > max_event_bytes and raw_byte_budget > 0:
        raw_byte_budget = max(
            0, raw_byte_budget - ((len(encoded) - max_event_bytes) * 3 // 4 + 3)
        )
        event.payload.raw_value_base64 = base64.b64encode(
            record.value[:raw_byte_budget]
        ).decode("ascii")
        encoded = event.model_dump_json().encode()
    if len(encoded) > max_event_bytes:
        raise ValueError("DLQ_MAX_EVENT_BYTES is too small for the dead-letter envelope")
    return encoded, event_id


async def _send_to_dlq_and_commit(
    producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
    record: object,
    dlq_topic: str,
    consumer_group: str,
    *,
    failure: DlqFailure,
    max_event_bytes: int,
) -> None:
    value, event_id = _dlq_value(
        record, failure=failure, max_event_bytes=max_event_bytes
    )
    headers = [
        ("event_id", str(event_id).encode()),
        ("event_type", b"grocery.deal.dead_lettered"),
        ("schema_version", b"1"),
    ]
    partition = TopicPartition(record.topic, record.partition)
    await producer.begin_transaction()
    try:
        await producer.send_and_wait(dlq_topic, key=record.key, value=value, headers=headers)
        await producer.send_offsets_to_transaction(
            {partition: OffsetAndMetadata(record.offset + 1, "")}, consumer_group
        )
        await producer.commit_transaction()
    except BaseException:
        await producer.abort_transaction()
        raise


async def serve(settings: Settings | None = None) -> None:
    settings = settings or Settings.from_env()
    settings.validate_projector()
    repository = PostgresDealRepository(settings.database_url)
    projector = NormalizedDealProjector(
        repository,
        EmbeddingCompatibility(
            model=settings.embedding_model or "",
            version=settings.embedding_version or "",
            dimension=settings.embedding_dimension,
        ),
    )
    consumer = AIOKafkaConsumer(
        settings.normalized_deals_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.consumer_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        transactional_id=settings.transactional_id,
    )
    retry_attempts: dict[tuple[str, int, int], int] = {}
    await consumer.start()
    await producer.start()
    try:
        async for record in consumer:
            partition = TopicPartition(record.topic, record.partition)
            record_key = (record.topic, record.partition, record.offset)
            metadata = RecordMetadata(record.topic, record.partition, record.offset)
            try:
                await projector.consume(record.value, metadata)
            except InvalidNormalizedDeal as error:
                await _send_to_dlq_and_commit(
                    producer,
                    consumer,
                    record,
                    settings.dlq_topic,
                    settings.consumer_group,
                    failure=DlqFailure(
                        failed_stage="validate",
                        classification="terminal_invalid",
                        error_code=error.error_code,
                        error_message=str(error),
                    ),
                    max_event_bytes=settings.dlq_max_event_bytes,
                )
                retry_attempts.pop(record_key, None)
            except psycopg.Error as error:
                disposition = classify_database_error(error)
                if disposition is DatabaseFailureDisposition.DEAD_LETTER:
                    await _send_to_dlq_and_commit(
                        producer,
                        consumer,
                        record,
                        settings.dlq_topic,
                        settings.consumer_group,
                        failure=DlqFailure(
                            failed_stage="project",
                            classification="terminal_invalid",
                            error_code="INVALID_PROJECTION_DATA",
                            error_message="normalized deal could not be stored",
                        ),
                        max_event_bytes=settings.dlq_max_event_bytes,
                    )
                    retry_attempts.pop(record_key, None)
                elif disposition is DatabaseFailureDisposition.RETRY:
                    attempt = retry_attempts.get(record_key, 0) + 1
                    retry_attempts[record_key] = attempt
                    consumer.seek(partition, record.offset)
                    consumer.pause(partition)
                    await asyncio.sleep(
                        database_retry_delay(
                            attempt,
                            base=settings.database_retry_base_seconds,
                            maximum=settings.database_retry_max_seconds,
                        )
                    )
                    consumer.resume(partition)
                else:
                    raise
            else:
                await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})
                retry_attempts.pop(record_key, None)
    finally:
        await producer.stop()
        await consumer.stop()


def run() -> None:
    asyncio.run(serve())
