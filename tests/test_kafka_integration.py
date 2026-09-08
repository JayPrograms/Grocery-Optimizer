import asyncio
import os
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import pytest

from apps.ranking.consumer import INVALID_SCHEMA_FAILURE, _send_to_dlq_and_commit
from packages.event_contracts import DeadLetterEventV1


BOOTSTRAP_SERVERS = os.environ.get("TEST_KAFKA_BOOTSTRAP_SERVERS")
pytestmark = pytest.mark.skipif(
    BOOTSTRAP_SERVERS is None,
    reason="set TEST_KAFKA_BOOTSTRAP_SERVERS to run Kafka transaction tests",
)


async def test_dlq_publish_and_source_offset_commit_are_one_kafka_transaction() -> None:
    assert BOOTSTRAP_SERVERS is not None
    suffix = uuid4().hex
    source_topic = f"normalized-test-{suffix}"
    dlq_topic = f"dlq-test-{suffix}"
    source_group = f"projector-test-{suffix}"
    admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    await admin.start()
    try:
        await admin.create_topics(
            [
                NewTopic(source_topic, num_partitions=1, replication_factor=1),
                NewTopic(dlq_topic, num_partitions=1, replication_factor=1),
            ]
        )
    finally:
        await admin.close()

    source_producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    source_consumer = AIOKafkaConsumer(
        source_topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=source_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    transactional_producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_idempotence=True,
        transactional_id=f"dlq-test-producer-{suffix}",
    )
    await source_producer.start()
    await source_consumer.start()
    await transactional_producer.start()
    try:
        await source_producer.send_and_wait(
            source_topic,
            value=b'{"bad":"schema"}',
            key=b"source:store:deal",
        )
        record = await asyncio.wait_for(source_consumer.getone(), timeout=10)
        await _send_to_dlq_and_commit(
            transactional_producer,
            source_consumer,
            record,
            dlq_topic,
            source_group,
            failure=INVALID_SCHEMA_FAILURE,
            max_event_bytes=900_000,
        )
    finally:
        await transactional_producer.stop()
        await source_consumer.stop()
        await source_producer.stop()

    dlq_consumer = AIOKafkaConsumer(
        dlq_topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"dlq-reader-{suffix}",
        auto_offset_reset="earliest",
        isolation_level="read_committed",
    )
    replay_consumer = AIOKafkaConsumer(
        source_topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=source_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        isolation_level="read_committed",
    )
    await dlq_consumer.start()
    await replay_consumer.start()
    try:
        dlq_record = await asyncio.wait_for(dlq_consumer.getone(), timeout=10)
        DeadLetterEventV1.model_validate_json(dlq_record.value)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(replay_consumer.getone(), timeout=1)
    finally:
        await replay_consumer.stop()
        await dlq_consumer.stop()
        admin = AIOKafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
        await admin.start()
        try:
            await admin.delete_topics([source_topic, dlq_topic])
        finally:
            await admin.close()
