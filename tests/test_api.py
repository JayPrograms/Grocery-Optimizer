from datetime import datetime, timedelta, timezone
import json

from httpx import ASGITransport, AsyncClient

from apps.ranking.api import create_app
from apps.ranking.projector import (
    EmbeddingCompatibility,
    NormalizedDealProjector,
    RecordMetadata,
)
from tests.conftest import normalized_event_dict
from tests.fakes import InMemoryDealRepository


async def _client_with_deals() -> tuple[AsyncClient, InMemoryDealRepository]:
    now = datetime.now(timezone.utc)
    repository = InMemoryDealRepository()
    projector = NormalizedDealProjector(
        repository, EmbeddingCompatibility("fixture-embedding", "1", 1536)
    )
    values = [
        normalized_event_dict(
            event_id="10000000-0000-0000-0000-000000000001",
            source_deal_id="active",
            valid_from=now - timedelta(hours=1),
            valid_to=now + timedelta(hours=1),
        ),
        normalized_event_dict(
            event_id="10000000-0000-0000-0000-000000000002",
            source_deal_id="expired",
            valid_from=now - timedelta(hours=2),
            valid_to=now - timedelta(hours=1),
        ),
        normalized_event_dict(
            event_id="10000000-0000-0000-0000-000000000003",
            source_deal_id="future",
            valid_from=now + timedelta(hours=1),
            valid_to=now + timedelta(hours=2),
        ),
    ]
    for offset, value in enumerate(values):
        await projector.consume(json.dumps(value).encode(), RecordMetadata("topic", 0, offset))
    client = AsyncClient(transport=ASGITransport(app=create_app(repository)), base_url="http://test")
    return client, repository


async def test_list_deals_filters_expired_and_future_offers() -> None:
    client, _ = await _client_with_deals()
    async with client:
        response = await client.get("/v1/deals")

    assert response.status_code == 200
    assert [deal["source_deal_id"] for deal in response.json()["deals"]] == ["active"]


async def test_get_deal_hides_expired_offer() -> None:
    client, repository = await _client_with_deals()
    expired = next(deal for _, deal in repository.deals.values() if deal.source_deal_id == "expired")
    async with client:
        response = await client.get(f"/v1/deals/{expired.id}")

    assert response.status_code == 404


async def test_list_deals_supports_store_and_product_filters() -> None:
    client, _ = await _client_with_deals()
    async with client:
        response = await client.get(
            "/v1/deals",
            params={
                "store_location_id": "store-1",
                "canonical_product_id": "40000000-0000-0000-0000-000000000001",
                "limit": 1,
            },
        )

    assert response.status_code == 200
    assert len(response.json()["deals"]) == 1


async def test_list_deals_rejects_unbounded_offset() -> None:
    client, _ = await _client_with_deals()
    async with client:
        response = await client.get("/v1/deals", params={"offset": 10_001})

    assert response.status_code == 422


async def test_optimization_uses_active_projection_and_returns_exactly_one_store() -> None:
    client, repository = await _client_with_deals()
    second = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000004",
        source_deal_id="active-second-store",
        valid_from=datetime.now(timezone.utc) - timedelta(hours=1),
        valid_to=datetime.now(timezone.utc) + timedelta(hours=1),
        deal_price="1.00",
    )
    second["payload"]["store"].update(
        {
            "store_location_id": "store-2",
            "store_name": "Market Uptown",
            "address_summary": "2 Main St",
            "latitude": "43.6532",
            "longitude": "-79.3832",
        }
    )
    projector = NormalizedDealProjector(
        repository, EmbeddingCompatibility("fixture-embedding", "1", 1536)
    )
    await projector.consume(
        json.dumps(second).encode(), RecordMetadata("topic", 0, 4)
    )

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "43.6532", "longitude": "-79.3832"},
                "fuel_efficiency_l_per_100km": "8",
                "items": [
                    {"client_item_id": "milk", "name": "Whole Milk", "quantity": "2"},
                    {"client_item_id": "eggs", "name": "eggs"},
                ],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert len(body["rankings"]) == 1
    recommendation = body["rankings"][0]
    assert recommendation["rank"] == 1
    assert recommendation["store"]["store_location_id"] == "store-2"
    assert recommendation["grocery_savings"] == "7.98"
    assert recommendation["incremental_fuel_cost"] == "0.00"
    assert recommendation["net_savings"] == "7.98"
    assert recommendation["unmatched_items"][0]["client_item_id"] == "eggs"


async def test_optimization_with_no_useful_match_returns_no_recommendation() -> None:
    client, _ = await _client_with_deals()

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "43.6532", "longitude": "-79.3832"},
                "fuel_efficiency_l_per_100km": "8",
                "items": [{"client_item_id": "eggs", "name": "eggs"}],
            },
        )

    assert response.status_code == 200
    assert response.json()["rankings"] == []
    assert response.json()["unmatched_items"] == [
        {
            "client_item_id": "eggs",
            "requested_name": "eggs",
            "reason": "no_savings_eligible_deal",
        }
    ]
