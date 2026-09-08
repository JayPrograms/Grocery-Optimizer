import json
import os

import psycopg
import pytest

from apps.ranking.repository import PostgresDealRepository
from packages.event_contracts import NormalizedDealEventV1
from tests.conftest import normalized_event_dict


DATABASE_URL = os.environ.get("TEST_RANKING_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="set TEST_RANKING_DATABASE_URL to run PostgreSQL projection tests",
)


async def test_postgres_projection_is_idempotent_and_queries_only_active_deals() -> None:
    assert DATABASE_URL is not None
    repository = PostgresDealRepository(DATABASE_URL)
    event = NormalizedDealEventV1.model_validate_json(json.dumps(normalized_event_dict()))

    with psycopg.connect(DATABASE_URL) as connection:
        connection.execute(
            "TRUNCATE ranking.deal_projection, ranking.catalog_projection, "
            "ranking.store_locations, ranking.processed_events CASCADE"
        )

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

    with psycopg.connect(DATABASE_URL) as connection:
        deal_count = connection.execute(
            "SELECT count(*) FROM ranking.deal_projection"
        ).fetchone()
        event_count = connection.execute(
            "SELECT count(*) FROM ranking.processed_events"
        ).fetchone()

    assert len(active) == 1
    assert expired == []
    assert deal_count == (1,)
    assert event_count == (1,)
