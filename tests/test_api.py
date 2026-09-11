from dataclasses import replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json

from httpx import ASGITransport, AsyncClient

from apps.ranking import api as ranking_api
from apps.ranking.api import create_app
from apps.ranking.config import Settings
from apps.ranking.projector import (
    EmbeddingCompatibility,
    NormalizedDealProjector,
    RecordMetadata,
)
from tests.conftest import BASE_TIME, normalized_event_dict
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


async def _client_for_events(
    values: list[dict[str, object]],
    *,
    now: datetime = BASE_TIME,
    settings: Settings | None = None,
) -> tuple[AsyncClient, InMemoryDealRepository]:
    repository = InMemoryDealRepository()
    projector = NormalizedDealProjector(
        repository, EmbeddingCompatibility("fixture-embedding", "1", 1536)
    )
    for offset, value in enumerate(values):
        await projector.consume(
            json.dumps(value).encode(), RecordMetadata("topic", 0, offset)
        )
    app = create_app(
        repository,
        settings=settings or Settings.from_env(),
        clock=lambda: now,
    )
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test"), repository


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
                    {"client_item_id": "milk", "name": "milk", "quantity": "2"},
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


async def test_optimization_reports_nonzero_distance_fuel_and_net(
    monkeypatch,
) -> None:
    event = normalized_event_dict(valid_from=BASE_TIME, valid_to=BASE_TIME + timedelta(days=1))
    event["payload"]["offer"].update(
        {"regular_price": "10.00", "deal_price": "4.00"}
    )
    settings = replace(
        Settings.from_env(),
        road_factor=Decimal("1.2"),
        fuel_price_per_litre=Decimal("1.75"),
    )
    client, _ = await _client_for_events([event], settings=settings)
    monkeypatch.setattr(
        ranking_api, "haversine_distance_km", lambda *coordinates: Decimal("12.5")
    )

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "43", "longitude": "-79"},
                "fuel_efficiency_l_per_100km": "8",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )

    recommendation = response.json()["rankings"][0]
    assert recommendation["one_way_distance_km"] == "15.000"
    assert recommendation["incremental_round_trip_distance_km"] == "30.000"
    assert recommendation["fuel_consumed_litres"] == "2.400"
    assert recommendation["grocery_savings"] == "6.00"
    assert recommendation["incremental_fuel_cost"] == "4.20"
    assert recommendation["net_savings"] == "1.80"


async def test_optimization_uses_request_fuel_price_and_reports_effective_assumption(
    monkeypatch,
) -> None:
    event = normalized_event_dict(valid_from=BASE_TIME, valid_to=BASE_TIME + timedelta(days=1))
    event["payload"]["offer"].update(
        {"regular_price": "10.00", "deal_price": "4.00"}
    )
    client, _ = await _client_for_events([event])
    monkeypatch.setattr(
        ranking_api, "haversine_distance_km", lambda *coordinates: Decimal("10")
    )

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "43", "longitude": "-79"},
                "fuel_efficiency_l_per_100km": "10",
                "fuel_price_per_litre": "2.00",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert body["assumptions"]["fuel_price_per_litre"] == "2.00"
    assert body["rankings"][0]["incremental_fuel_cost"] == "4.80"
    assert body["rankings"][0]["net_savings"] == "1.20"


async def test_optimization_rejects_negative_request_fuel_price() -> None:
    client, _ = await _client_with_deals()

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "43", "longitude": "-79"},
                "fuel_efficiency_l_per_100km": "8",
                "fuel_price_per_litre": "-0.01",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )

    assert response.status_code == 422


async def test_radius_includes_boundary_and_excludes_just_outside(
    monkeypatch,
) -> None:
    event = normalized_event_dict(valid_from=BASE_TIME, valid_to=BASE_TIME + timedelta(days=1))
    client, _ = await _client_for_events([event])
    monkeypatch.setattr(
        ranking_api, "haversine_distance_km", lambda *coordinates: Decimal("15")
    )

    async with client:
        boundary = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "0", "longitude": "0"},
                "radius_km": "15",
                "fuel_efficiency_l_per_100km": "8",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )
        monkeypatch.setattr(
            ranking_api,
            "haversine_distance_km",
            lambda *coordinates: Decimal("15.0001"),
        )
        outside = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "0", "longitude": "0"},
                "radius_km": "15",
                "fuel_efficiency_l_per_100km": "8",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )

    assert len(boundary.json()["rankings"]) == 1
    assert outside.json()["rankings"] == []


async def test_optimization_excludes_expired_and_future_deals_at_boundaries() -> None:
    active = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000011",
        source_deal_id="active-at-boundary",
        valid_from=BASE_TIME,
        valid_to=BASE_TIME + timedelta(hours=1),
        deal_price="3.99",
    )
    expired = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000012",
        source_deal_id="expired-at-boundary",
        valid_from=BASE_TIME - timedelta(hours=1),
        valid_to=BASE_TIME,
        deal_price="0.00",
    )
    future = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000013",
        source_deal_id="future",
        valid_from=BASE_TIME + timedelta(seconds=1),
        valid_to=BASE_TIME + timedelta(hours=1),
        deal_price="0.00",
    )
    client, _ = await _client_for_events([active, expired, future])

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "43.6532", "longitude": "-79.3832"},
                "fuel_efficiency_l_per_100km": "8",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )

    recommendation = response.json()["rankings"][0]
    assert recommendation["grocery_savings"] == "1.00"


async def test_presented_money_preserves_item_sum_and_net_identity(monkeypatch) -> None:
    event = normalized_event_dict(
        valid_from=BASE_TIME,
        valid_to=BASE_TIME + timedelta(days=1),
        deal_price="3.985",
    )
    settings = replace(
        Settings.from_env(),
        road_factor=Decimal("1"),
        fuel_price_per_litre=Decimal("0.8"),
    )
    client, _ = await _client_for_events([event], settings=settings)
    monkeypatch.setattr(
        ranking_api, "haversine_distance_km", lambda *coordinates: Decimal("0.025")
    )

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "0", "longitude": "0"},
                "fuel_efficiency_l_per_100km": "10",
                "items": [{"client_item_id": "milk", "name": "milk"}],
            },
        )

    recommendation = response.json()["rankings"][0]
    item_total = sum(
        (Decimal(item["item_savings"]) for item in recommendation["matched_items"]),
        start=Decimal("0"),
    )
    grocery_savings = Decimal(recommendation["grocery_savings"])
    fuel_cost = Decimal(recommendation["incremental_fuel_cost"])
    net_savings = Decimal(recommendation["net_savings"])
    assert item_total == grocery_savings == Decimal("1.01")
    assert net_savings == grocery_savings - fuel_cost == Decimal("1.01")


async def test_presented_grocery_savings_equals_sum_of_multiple_displayed_items(
    monkeypatch,
) -> None:
    milk = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000021",
        source_deal_id="fractional-milk",
        valid_from=BASE_TIME,
        valid_to=BASE_TIME + timedelta(days=1),
        deal_price="3.985",
    )
    bread = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000022",
        source_deal_id="fractional-bread",
        valid_from=BASE_TIME,
        valid_to=BASE_TIME + timedelta(days=1),
        deal_price="4.985",
    )
    bread["payload"]["product"].update(
        {
            "canonical_product_id": "40000000-0000-0000-0000-000000000002",
            "canonical_name": "bread",
            "search_text": "bread",
        }
    )
    client, _ = await _client_for_events([milk, bread])
    monkeypatch.setattr(
        ranking_api, "haversine_distance_km", lambda *coordinates: Decimal("0")
    )

    async with client:
        response = await client.post(
            "/v1/optimizations",
            json={
                "origin": {"latitude": "0", "longitude": "0"},
                "fuel_efficiency_l_per_100km": "8",
                "items": [
                    {"client_item_id": "milk", "name": "milk"},
                    {"client_item_id": "bread", "name": "bread"},
                ],
            },
        )

    recommendation = response.json()["rankings"][0]
    displayed_items = [
        Decimal(item["item_savings"]) for item in recommendation["matched_items"]
    ]
    assert displayed_items == [Decimal("1.01"), Decimal("0.01")]
    assert Decimal(recommendation["grocery_savings"]) == sum(
        displayed_items, start=Decimal("0")
    )
