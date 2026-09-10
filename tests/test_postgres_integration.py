import asyncio
import json
import os

import psycopg
import pytest

from apps.ranking.repository import PostgresDealRepository
from event_contracts import NormalizedDealEventV1
from tests.conftest import BASE_TIME, normalized_event_dict


DATABASE_URL = os.environ.get("TEST_RANKING_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="set TEST_RANKING_DATABASE_URL to run PostgreSQL projection tests",
)


def _truncate_projection() -> None:
    if DATABASE_URL is None:
        return
    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(
            "TRUNCATE ranking.deal_projection, ranking.catalog_projection, "
            "ranking.store_locations, ranking.processed_events CASCADE"
        )


@pytest.fixture(autouse=True)
def clean_projection() -> None:
    _truncate_projection()


def _event(**overrides: object) -> NormalizedDealEventV1:
    return NormalizedDealEventV1.model_validate_json(
        json.dumps(normalized_event_dict(**overrides))
    )


def _counts() -> tuple[int, int]:
    assert DATABASE_URL is not None
    with psycopg.connect(DATABASE_URL) as connection:
        deal_count = connection.execute(
            "SELECT count(*) FROM ranking.deal_projection"
        ).fetchone()
        event_count = connection.execute(
            "SELECT count(*) FROM ranking.processed_events"
        ).fetchone()
    assert deal_count is not None and event_count is not None
    return deal_count[0], event_count[0]


async def test_postgres_projection_is_idempotent_and_queries_only_active_deals() -> None:
    assert DATABASE_URL is not None
    repository = PostgresDealRepository(DATABASE_URL)
    event = _event()

    assert await repository.project(event, topic="topic", partition=0, offset=1) is True
    assert await repository.project(event, topic="topic", partition=0, offset=1) is False

    active = await repository.list_active(
        now=event.payload.source_updated_at,
        store_location_id=None,
        canonical_product_id=None,
        limit=50,
        offset=0,
    )
    expired = await repository.list_active(
        now=event.payload.offer.valid_to,
        store_location_id=None,
        canonical_product_id=None,
        limit=50,
        offset=0,
    )

    assert len(active) == 1
    assert expired == []
    assert _counts() == (1, 1)


async def test_concurrent_duplicate_delivery_applies_once() -> None:
    assert DATABASE_URL is not None
    repository = PostgresDealRepository(DATABASE_URL)
    event = _event()

    outcomes = await asyncio.gather(
        repository.project(event, topic="topic", partition=0, offset=1),
        repository.project(event, topic="topic", partition=0, offset=1),
    )

    assert sorted(outcomes) == [False, True]
    assert _counts() == (1, 1)


async def test_projection_failure_rolls_back_inbox_record() -> None:
    assert DATABASE_URL is not None
    repository = PostgresDealRepository(DATABASE_URL)
    incompatible = _event(embedding=["0.1", "0.2"])

    with pytest.raises(psycopg.DataError):
        await repository.project(incompatible, topic="topic", partition=0, offset=1)

    assert _counts() == (0, 0)


async def test_equal_source_version_uses_larger_event_id_deterministically() -> None:
    assert DATABASE_URL is not None
    repository = PostgresDealRepository(DATABASE_URL)
    winner = _event(
        event_id="10000000-0000-0000-0000-000000000009",
        source_updated_at=BASE_TIME,
        deal_price="2.99",
    )
    loser = _event(
        event_id="10000000-0000-0000-0000-000000000001",
        source_updated_at=BASE_TIME,
        deal_price="1.00",
    )

    await repository.project(winner, topic="topic", partition=0, offset=1)
    await repository.project(loser, topic="topic", partition=0, offset=2)
    deals = await repository.list_active(
        now=BASE_TIME,
        store_location_id=None,
        canonical_product_id=None,
        limit=50,
        offset=0,
    )

    assert len(deals) == 1
    assert deals[0].offer.deal_price == winner.payload.offer.deal_price


async def test_projection_rebuild_preserves_public_deal_id() -> None:
    assert DATABASE_URL is not None
    repository = PostgresDealRepository(DATABASE_URL)
    event = _event()

    await repository.project(event, topic="topic", partition=0, offset=1)
    first = await repository.list_active(
        now=BASE_TIME,
        store_location_id=None,
        canonical_product_id=None,
        limit=50,
        offset=0,
    )
    _truncate_projection()
    await repository.project(event, topic="topic", partition=0, offset=1)
    rebuilt = await repository.list_active(
        now=BASE_TIME,
        store_location_id=None,
        canonical_product_id=None,
        limit=50,
        offset=0,
    )

    assert first[0].id == rebuilt[0].id
