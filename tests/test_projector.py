import json

import pytest

from apps.ranking.projector import (
    InvalidNormalizedDeal,
    NormalizedDealProjector,
    ProjectionOutcome,
    RecordMetadata,
)
from tests.conftest import BASE_TIME, normalized_event_dict
from tests.fakes import InMemoryDealRepository


async def test_duplicate_delivery_creates_one_logical_deal(event_bytes: bytes) -> None:
    repository = InMemoryDealRepository()
    projector = NormalizedDealProjector(repository)
    metadata = RecordMetadata("grocery.deals.normalized.v1", 0, 12)

    first = await projector.consume(event_bytes, metadata)
    duplicate = await projector.consume(event_bytes, metadata)

    assert first is ProjectionOutcome.APPLIED
    assert duplicate is ProjectionOutcome.DUPLICATE
    assert len(repository.processed_events) == 1
    assert len(repository.deals) == 1


async def test_older_source_version_cannot_overwrite_newer_deal() -> None:
    repository = InMemoryDealRepository()
    projector = NormalizedDealProjector(repository)
    newer = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000009",
        source_updated_at=BASE_TIME,
        deal_price="2.99",
    )
    older = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000010",
        source_updated_at=BASE_TIME.replace(hour=14),
        deal_price="1.00",
    )

    await projector.consume(json.dumps(newer).encode(), RecordMetadata("topic", 0, 1))
    await projector.consume(json.dumps(older).encode(), RecordMetadata("topic", 0, 2))

    projected = next(iter(repository.deals.values()))[1]
    assert str(projected.offer.deal_price) == "2.99"
    assert len(repository.processed_events) == 2


async def test_distinct_events_for_one_logical_deal_still_use_one_row() -> None:
    repository = InMemoryDealRepository()
    projector = NormalizedDealProjector(repository)
    first = normalized_event_dict(event_id="10000000-0000-0000-0000-000000000001")
    update = normalized_event_dict(
        event_id="10000000-0000-0000-0000-000000000002",
        source_updated_at=BASE_TIME.replace(minute=1),
        deal_price="2.49",
    )

    await projector.consume(json.dumps(first).encode(), RecordMetadata("topic", 0, 1))
    await projector.consume(json.dumps(update).encode(), RecordMetadata("topic", 0, 2))

    assert len(repository.deals) == 1
    assert str(next(iter(repository.deals.values()))[1].offer.deal_price) == "2.49"


async def test_invalid_event_is_a_terminal_projection_error() -> None:
    projector = NormalizedDealProjector(InMemoryDealRepository())

    with pytest.raises(InvalidNormalizedDeal):
        await projector.consume(b'{"not":"a normalized deal"}', RecordMetadata("topic", 0, 1))
