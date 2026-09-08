from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
from uuid import UUID

import pytest


BASE_TIME = datetime(2026, 9, 4, 15, 0, tzinfo=timezone.utc)


def normalized_event_dict(
    *,
    event_id: str = "10000000-0000-0000-0000-000000000001",
    source_deal_id: str = "deal-1",
    source_updated_at: datetime = BASE_TIME,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
    deal_price: str = "3.49",
    embedding: list[str] | None = None,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "grocery.deal.normalized",
        "schema_version": 1,
        "occurred_at": "2026-09-04T15:00:01Z",
        "producer": "normalization-service",
        "correlation_id": "20000000-0000-0000-0000-000000000001",
        "causation_id": "30000000-0000-0000-0000-000000000001",
        "trace_id": "trace-1",
        "payload": {
            "source": "fixture",
            "source_deal_id": source_deal_id,
            "source_updated_at": source_updated_at.isoformat().replace("+00:00", "Z"),
            "store": {
                "store_location_id": "store-1",
                "chain_name": "Market",
                "store_name": "Market Downtown",
                "address_summary": "1 Main St",
                "latitude": "43.6532",
                "longitude": "-79.3832",
            },
            "product": {
                "canonical_product_id": "40000000-0000-0000-0000-000000000001",
                "canonical_name": "whole milk",
                "brand": None,
                "package_quantity": "1",
                "package_unit": "l",
                "category": "dairy",
                "search_text": "whole milk 1 l",
                "embedding": embedding or ["0.1"] * 1536,
            },
            "offer": {
                "deal_price": deal_price,
                "regular_price": "4.99",
                "currency": "CAD",
                "package_quantity": "1",
                "package_unit": "l",
                "valid_from": (valid_from or BASE_TIME - timedelta(days=1))
                .isoformat()
                .replace("+00:00", "Z"),
                "valid_to": (valid_to or BASE_TIME + timedelta(days=1))
                .isoformat()
                .replace("+00:00", "Z"),
            },
            "normalization": {
                "method": "vector",
                "confidence": "0.95",
                "embedding_model": "fixture-embedding",
                "embedding_version": "1",
                "llm_model": None,
                "prompt_version": None,
                "catalog_version": 1,
            },
        },
    }


@pytest.fixture
def event_bytes() -> bytes:
    return json.dumps(normalized_event_dict()).encode()
