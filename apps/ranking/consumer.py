import asyncio
import base64
from datetime import datetime, timezone
import json
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
import psycopg

from apps.ranking.config import Settings
from apps.ranking.projector import (
    InvalidNormalizedDeal,
    NormalizedDealProjector,
    RecordMetadata,
)
from apps.ranking.repository import PostgresDealRepository


def _safe_original_event(value: bytes) -> tuple[object | None, str | None]:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, base64.b64encode(value[:1_000_000]).decode("ascii")
    if isinstance(parsed, dict):
        return parsed, None
    return None, base64.b64encode(value[:1_000_000]).decode("ascii")


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


def _dlq_value(record: object) -> tuple[bytes, UUID]:
    original_event, raw_value = _safe_original_event(record.value)
    dlq_event_id = uuid4()
    correlation_id = _header_uuid(record, "correlation_id") or uuid4()
    causation_id = _header_uuid(record, "event_id")
    event = {
        "event_id": str(dlq_event_id),
        "event_type": "grocery.deal.dead_lettered",
        "schema_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "producer": "ranking-projector",
        "correlation_id": str(correlation_id),
        "causation_id": str(causation_id) if causation_id else None,
        "trace_id": _header_text(record, "trace_id") or "unavailable",
        "payload": {
            "original_topic": record.topic,
            "original_partition": record.partition,
            "original_offset": record.offset,
            "original_key": record.key.decode("utf-8", errors="replace")[:500]
            if record.key
            else None,
            "original_event": original_event,
            "raw_value_base64": raw_value,
            "failed_stage": "validate",
            "classification": "terminal_invalid",
            "attempts": 1,
            "error_code": "INVALID_NORMALIZED_DEAL_V1",
            "error_message": "normalized deal does not match schema v1",
            "failed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
    }
    return json.dumps(event, separators=(",", ":")).encode(), dlq_event_id


async def _send_to_dlq_and_commit(
    producer: AIOKafkaProducer,
    consumer: AIOKafkaConsumer,
    record: object,
    dlq_topic: str,
    consumer_group: str,
) -> None:
    value, event_id = _dlq_value(record)
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
    repository = PostgresDealRepository(settings.database_url)
    projector = NormalizedDealProjector(repository)
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
        transactional_id=f"ranking-projector-dlq-{uuid4()}",
    )
    await consumer.start()
    await producer.start()
    try:
        async for record in consumer:
            partition = TopicPartition(record.topic, record.partition)
            metadata = RecordMetadata(record.topic, record.partition, record.offset)
            try:
                await projector.consume(record.value, metadata)
            except InvalidNormalizedDeal:
                await _send_to_dlq_and_commit(
                    producer, consumer, record, settings.dlq_topic, settings.consumer_group
                )
            except psycopg.Error:
                # The fetched record remains uncommitted and is explicitly rewound.
                consumer.seek(partition, record.offset)
                consumer.pause(partition)
                await asyncio.sleep(1)
                consumer.resume(partition)
            else:
                await consumer.commit({partition: OffsetAndMetadata(record.offset + 1, "")})
    finally:
        await producer.stop()
        await consumer.stop()


def run() -> None:
    asyncio.run(serve())
