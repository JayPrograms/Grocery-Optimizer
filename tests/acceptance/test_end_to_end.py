from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import httpx
import psycopg
import pytest

from event_contracts import (
    DeadLetterEventV1,
    NormalizationRetryEventV1,
    RawDealEventV1,
    RawDealPayload,
)


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_ACCEPTANCE") != "1",
    reason="set RUN_ACCEPTANCE=1 with the Compose stack running",
)

ROOT = Path(__file__).parents[2]
BOOTSTRAP = os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
DATABASE_URL = os.getenv(
    "TEST_RANKING_DATABASE_URL",
    "postgresql://grocery_app:replace_me_for_local_development@127.0.0.1:55432/grocery_savings",
)
INGESTION_URL = os.getenv("TEST_INGESTION_URL", "http://127.0.0.1:8001")
WEB_URL = os.getenv("TEST_WEB_URL", "http://127.0.0.1:3000")
TOKEN = os.getenv("INGESTION_TOKEN", "local-development-token")


async def _wait_for(query: str, parameters: tuple[object, ...], expected: int) -> None:
    for _ in range(100):
        with psycopg.connect(DATABASE_URL) as connection:
            row = connection.execute(query, parameters).fetchone()
        if row and row[0] == expected:
            return
        await asyncio.sleep(0.2)
    raise AssertionError(f"timed out waiting for count {expected}")


def _dynamic_batch(suffix: str) -> tuple[dict[str, object], str]:
    batch = json.loads((ROOT / "fixtures" / "flyer.json").read_text(encoding="utf-8"))
    now = datetime.now(timezone.utc)
    source = f"acceptance-{suffix}"
    unknown_name = f"rare fruit {suffix}"
    for deal in batch["deals"]:
        deal["source"] = source
        deal["source_updated_at"] = now.isoformat().replace("+00:00", "Z")
        deal["offer"]["valid_from"] = (now - timedelta(days=1)).isoformat().replace(
            "+00:00", "Z"
        )
        deal["offer"]["valid_to"] = (now + timedelta(days=1)).isoformat().replace(
            "+00:00", "Z"
        )
    batch["deals"][0]["source_deal_id"] = f"milk-{suffix}"
    batch["deals"][1]["source_deal_id"] = f"unknown-{suffix}"
    batch["deals"][1]["product"]["raw_name"] = unknown_name
    expired = deepcopy(batch["deals"][0])
    expired["source_deal_id"] = f"expired-{suffix}"
    expired["offer"]["valid_from"] = (now - timedelta(days=2)).isoformat().replace(
        "+00:00", "Z"
    )
    expired["offer"]["valid_to"] = (now - timedelta(days=1)).isoformat().replace(
        "+00:00", "Z"
    )
    batch["deals"].append(expired)
    return batch, unknown_name


def _raw_event(payload: dict[str, object], event_id=None) -> RawDealEventV1:
    now = datetime.now(timezone.utc)
    return RawDealEventV1(
        event_id=event_id or uuid4(),
        event_type="grocery.deal.raw",
        schema_version=1,
        occurred_at=now,
        producer="acceptance-test",
        correlation_id=uuid4(),
        causation_id=None,
        trace_id=f"acceptance-{uuid4().hex}",
        payload=RawDealPayload.model_validate_json(json.dumps(payload)),
    )


def _headers(event: RawDealEventV1) -> list[tuple[str, bytes]]:
    return [
        ("event_id", str(event.event_id).encode()),
        ("event_type", event.event_type.encode()),
        ("schema_version", b"1"),
        ("correlation_id", str(event.correlation_id).encode()),
        ("trace_id", event.trace_id.encode()),
    ]


async def test_complete_fixture_pipeline_idempotency_retries_and_dlq() -> None:
    suffix = uuid4().hex[:12]
    source = f"acceptance-{suffix}"
    batch, unknown_name = _dynamic_batch(suffix)
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(
            f"{INGESTION_URL}/internal/v1/deal-batches",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json=batch,
        )
        assert response.status_code == 202, response.text
        assert response.json()["accepted"] == 4

        await _wait_for(
            "SELECT count(*) FROM normalization.deals WHERE source = %s", (source,), 4
        )
        await _wait_for(
            "SELECT count(*) FROM ranking.deal_projection WHERE source = %s", (source,), 4
        )
        with psycopg.connect(DATABASE_URL) as connection:
            methods = {
                source_deal_id: set(method_rows)
                for source_deal_id, method_rows in connection.execute(
                    """
                    SELECT source_deal_id, array_agg(normalization_method)
                    FROM normalization.normalization_audit WHERE source = %s
                    GROUP BY source_deal_id
                    """,
                    (source,),
                ).fetchall()
            }
        assert "vector" in set().union(*methods.values())
        assert "llm" in methods[f"unknown-{suffix}"]

        deals = (await client.get("http://127.0.0.1:8000/v1/deals")).json()["deals"]
        source_deals = {deal["source_deal_id"] for deal in deals if deal["source"] == source}
        assert f"expired-{suffix}" not in source_deals
        assert f"milk-{suffix}" in source_deals

        optimization = await client.post(
            f"{WEB_URL}/api/optimizations",
            json={
                "origin": {"latitude": "43.6532", "longitude": "-79.3832"},
                "fuel_efficiency_l_per_100km": "8",
                "fuel_price_per_litre": "1.60",
                "radius_km": "15",
                "items": [
                    {"client_item_id": "milk", "name": "whole milk", "quantity": "1"},
                    {"client_item_id": "fruit", "name": unknown_name, "quantity": "1"},
                    {"client_item_id": "bread", "name": "bread", "quantity": "1"},
                ],
            },
        )
        assert optimization.status_code == 200, optimization.text
        recommendation = optimization.json()["rankings"][0]
        assert recommendation["store"]["store_location_id"] == "fixture-store-1"
        assert recommendation["grocery_savings"] == "4.50"
        assert recommendation["net_savings"] == "4.50"

        duplicate_response = await client.post(
            f"{INGESTION_URL}/internal/v1/deal-batches",
            headers={"Authorization": f"Bearer {TOKEN}"},
            json=batch,
        )
        assert duplicate_response.status_code == 202
        await asyncio.sleep(2)
        with psycopg.connect(DATABASE_URL) as connection:
            assert connection.execute(
                "SELECT count(*) FROM normalization.deals WHERE source = %s", (source,)
            ).fetchone() == (4,)
            assert connection.execute(
                "SELECT count(*) FROM ranking.deal_projection WHERE source = %s", (source,)
            ).fetchone() == (4,)

    producer = AIOKafkaProducer(bootstrap_servers=BOOTSTRAP)
    dlq = AIOKafkaConsumer(
        "grocery.deals.dlq.v1",
        bootstrap_servers=BOOTSTRAP,
        group_id=f"acceptance-dlq-{suffix}",
        auto_offset_reset="latest",
    )
    retry_1m = AIOKafkaConsumer(
        "grocery.deals.normalize.retry.1m.v1",
        bootstrap_servers=BOOTSTRAP,
        group_id=f"acceptance-retry-1m-{suffix}",
        auto_offset_reset="latest",
    )
    retry_10m = AIOKafkaConsumer(
        "grocery.deals.normalize.retry.10m.v1",
        bootstrap_servers=BOOTSTRAP,
        group_id=f"acceptance-retry-10m-{suffix}",
        auto_offset_reset="latest",
    )
    await producer.start()
    await dlq.start()
    await retry_1m.start()
    await retry_10m.start()
    try:
        await asyncio.sleep(0.5)
        template = deepcopy(batch["deals"][0])
        duplicate_source = f"duplicate-{suffix}"
        template["source"] = duplicate_source
        template["source_deal_id"] = f"duplicate-{suffix}"
        duplicate_event = _raw_event(template)
        for _ in range(2):
            await producer.send_and_wait(
                "grocery.deals.raw.v1",
                key=f"{duplicate_source}:fixture-store-1:duplicate-{suffix}".encode(),
                value=duplicate_event.model_dump_json().encode(),
                headers=_headers(duplicate_event),
            )
        await _wait_for(
            "SELECT count(*) FROM normalization.processed_events WHERE event_id = %s",
            (duplicate_event.event_id,),
            1,
        )
        await _wait_for(
            "SELECT count(*) FROM normalization.deals WHERE source = %s",
            (duplicate_source,),
            1,
        )

        await producer.send_and_wait(
            "grocery.deals.raw.v1", key=f"malformed-{suffix}".encode(), value=b"{bad-json"
        )
        unresolved_payload = deepcopy(template)
        unresolved_payload["source_deal_id"] = f"unresolved-{suffix}"
        unresolved_payload["product"]["raw_name"] = "unresolvable mystery"
        unresolved = _raw_event(unresolved_payload)
        await producer.send_and_wait(
            "grocery.deals.raw.v1",
            key=f"unresolved-{suffix}".encode(),
            value=unresolved.model_dump_json().encode(),
            headers=_headers(unresolved),
        )
        exhausted_payload = deepcopy(template)
        exhausted_payload["source_deal_id"] = "retry-exhaust"
        exhausted = _raw_event(exhausted_payload)
        await producer.send_and_wait(
            "grocery.deals.raw.v1",
            key=f"exhausted-{suffix}".encode(),
            value=exhausted.model_dump_json().encode(),
            headers=_headers(exhausted),
        )
        first_retry = NormalizationRetryEventV1.model_validate_json(
            (await asyncio.wait_for(retry_1m.getone(), timeout=10)).value
        )
        final_retry = NormalizationRetryEventV1.model_validate_json(
            (await asyncio.wait_for(retry_10m.getone(), timeout=10)).value
        )
        assert first_retry.payload.original_event.event_id == exhausted.event_id
        assert first_retry.payload.attempt == 1
        assert final_retry.payload.original_event.event_id == exhausted.event_id
        assert final_retry.payload.attempt == 2

        classifications: set[str] = set()
        attempts: dict[str, int] = {}
        for _ in range(3):
            record = await asyncio.wait_for(dlq.getone(), timeout=15)
            event = DeadLetterEventV1.model_validate_json(record.value)
            classifications.add(event.payload.classification)
            attempts[event.payload.classification] = event.payload.attempts
        assert classifications == {
            "terminal_invalid",
            "terminal_unresolvable",
            "retries_exhausted",
        }
        assert attempts["retries_exhausted"] == 3
    finally:
        await retry_10m.stop()
        await retry_1m.stop()
        await dlq.stop()
        await producer.stop()
