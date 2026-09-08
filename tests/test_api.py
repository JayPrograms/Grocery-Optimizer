from datetime import datetime, timedelta, timezone
import json

from httpx import ASGITransport, AsyncClient

from apps.ranking.api import create_app
from apps.ranking.projector import NormalizedDealProjector, RecordMetadata
from tests.conftest import normalized_event_dict
from tests.fakes import InMemoryDealRepository


async def _client_with_deals() -> tuple[AsyncClient, InMemoryDealRepository]:
    now = datetime.now(timezone.utc)
    repository = InMemoryDealRepository()
    projector = NormalizedDealProjector(repository)
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


async def test_readiness_reports_database_failure() -> None:
    client, repository = await _client_with_deals()
    repository.ready = False
    async with client:
        response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {"status": "unavailable"}
