from collections.abc import Sequence
from datetime import datetime
from uuid import UUID, uuid5, NAMESPACE_URL

from apps.ranking.models import (
    ActiveDeal,
    NormalizationView,
    OfferView,
    ProductView,
    StoreView,
)
from packages.event_contracts import NormalizedDealEventV1


class InMemoryDealRepository:
    def __init__(self) -> None:
        self.processed_events: set[UUID] = set()
        self.deals: dict[tuple[str, str, str], tuple[NormalizedDealEventV1, ActiveDeal]] = {}
        self.ready = True

    async def project(
        self,
        event: NormalizedDealEventV1,
        *,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        del topic, partition, offset
        if event.event_id in self.processed_events:
            return False
        self.processed_events.add(event.event_id)
        payload = event.payload
        key = (payload.source, payload.store.store_location_id, payload.source_deal_id)
        existing = self.deals.get(key)
        incoming_order = (payload.source_updated_at, event.event_id)
        if existing:
            current_event = existing[0]
            current_order = (current_event.payload.source_updated_at, current_event.event_id)
            if incoming_order <= current_order:
                return True
        deal_id = uuid5(NAMESPACE_URL, ":".join(key))
        self.deals[key] = (event, self._to_active_deal(event, deal_id))
        return True

    @staticmethod
    def _to_active_deal(event: NormalizedDealEventV1, deal_id: UUID) -> ActiveDeal:
        payload = event.payload
        return ActiveDeal(
            id=deal_id,
            source=payload.source,
            source_deal_id=payload.source_deal_id,
            source_updated_at=payload.source_updated_at,
            store=StoreView(**payload.store.model_dump()),
            product=ProductView(**payload.product.model_dump(exclude={"search_text", "embedding"})),
            offer=OfferView(**payload.offer.model_dump()),
            normalization=NormalizationView(**payload.normalization.model_dump()),
        )

    async def list_active(
        self,
        *,
        now: datetime,
        store_location_id: str | None,
        canonical_product_id: UUID | None,
        limit: int,
        offset: int,
    ) -> Sequence[ActiveDeal]:
        active = [
            deal
            for _, deal in self.deals.values()
            if deal.offer.valid_from <= now < deal.offer.valid_to
            and (store_location_id is None or deal.store.store_location_id == store_location_id)
            and (
                canonical_product_id is None
                or deal.product.canonical_product_id == canonical_product_id
            )
        ]
        active.sort(key=lambda deal: (deal.offer.valid_to, deal.id))
        return active[offset : offset + limit]

    async def get_active(self, deal_id: UUID, *, now: datetime) -> ActiveDeal | None:
        for _, deal in self.deals.values():
            if deal.id == deal_id and deal.offer.valid_from <= now < deal.offer.valid_to:
                return deal
        return None

    async def ping(self) -> bool:
        return self.ready
