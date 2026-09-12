"""At-least-once relay for committed normalization outbox events."""

from __future__ import annotations

import asyncio
import json
import os

from aiokafka import AIOKafkaProducer
import psycopg
from psycopg.rows import dict_row

from apps.normalization.pipeline import database_url


TOPIC = "grocery.deals.normalized.v1"


def _pending_rows() -> list[dict[str, object]]:
    with psycopg.connect(database_url(), row_factory=dict_row) as connection:
        return connection.execute(
            """
            SELECT event_id, aggregate_key, event_value
            FROM normalization.outbox
            WHERE published_at IS NULL
            ORDER BY created_at
            LIMIT 100
            """
        ).fetchall()


def _mark_published(event_id: object) -> None:
    with psycopg.connect(database_url()) as connection:
        connection.execute(
            """
            UPDATE normalization.outbox
            SET published_at = now(), attempts = attempts + 1
            WHERE event_id = %s AND published_at IS NULL
            """,
            (event_id,),
        )


async def serve() -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        enable_idempotence=True,
    )
    await producer.start()
    try:
        while True:
            rows = await asyncio.to_thread(_pending_rows)
            if not rows:
                await asyncio.sleep(0.2)
                continue
            for row in rows:
                value = row["event_value"]
                encoded = json.dumps(value).encode()
                correlation_id = str(value["correlation_id"]).encode()
                trace_id = str(value["trace_id"]).encode()
                await producer.send_and_wait(
                    TOPIC,
                    key=str(row["aggregate_key"]).encode(),
                    value=encoded,
                    headers=[
                        ("event_id", str(row["event_id"]).encode()),
                        ("event_type", b"grocery.deal.normalized"),
                        ("schema_version", b"1"),
                        ("correlation_id", correlation_id),
                        ("trace_id", trace_id),
                    ],
                )
                await asyncio.to_thread(_mark_published, row["event_id"])
    finally:
        await producer.stop()


def run() -> None:
    asyncio.run(serve())


if __name__ == "__main__":
    run()
