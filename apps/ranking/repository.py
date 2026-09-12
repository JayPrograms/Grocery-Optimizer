import asyncio
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Protocol
from uuid import UUID, uuid5

import psycopg
from psycopg.rows import dict_row

from apps.ranking.models import (
    ActiveDeal,
    NormalizationView,
    OfferView,
    ProductView,
    StoreView,
)
from event_contracts import NormalizedDealEventV1
from domain import deterministic_embedding, vector_literal


class DealRepository(Protocol):
    async def project(
        self,
        event: NormalizedDealEventV1,
        *,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool: ...

    async def list_active(
        self,
        *,
        now: datetime,
        store_location_id: str | None,
        canonical_product_id: UUID | None,
        limit: int,
        offset: int,
    ) -> Sequence[ActiveDeal]: ...

    async def get_active(self, deal_id: UUID, *, now: datetime) -> ActiveDeal | None: ...

    async def ping(self) -> bool: ...

    async def match_product(self, name: str) -> UUID | None: ...


_ACTIVE_DEAL_SELECT = """
SELECT
    d.id,
    d.source,
    d.source_deal_id,
    d.source_updated_at,
    s.store_location_id,
    s.chain_name,
    s.store_name,
    s.address_summary,
    s.latitude,
    s.longitude,
    c.canonical_product_id,
    c.canonical_name,
    c.brand,
    c.package_quantity AS product_package_quantity,
    c.package_unit AS product_package_unit,
    c.category,
    d.deal_price,
    d.regular_price,
    d.currency,
    d.package_quantity AS offer_package_quantity,
    d.package_unit AS offer_package_unit,
    d.valid_from,
    d.valid_to,
    d.normalization_method,
    d.normalization_confidence,
    d.embedding_model,
    d.embedding_version,
    d.llm_model,
    d.prompt_version,
    d.catalog_version
FROM ranking.deal_projection AS d
JOIN ranking.store_locations AS s ON s.store_location_id = d.store_location_id
JOIN ranking.catalog_projection AS c ON c.canonical_product_id = d.canonical_product_id
"""

_DEAL_ID_NAMESPACE = UUID("ed702d37-9f4c-5c2e-a137-72822b37763e")


def deal_projection_id(source: str, store_location_id: str, source_deal_id: str) -> UUID:
    identity = "".join(
        f"{len(value)}:{value}" for value in (source, store_location_id, source_deal_id)
    )
    return uuid5(_DEAL_ID_NAMESPACE, identity)


def _vector_literal(values: list[Decimal]) -> str:
    return "[" + ",".join(str(value) for value in values) + "]"


def _row_to_deal(row: dict[str, object]) -> ActiveDeal:
    return ActiveDeal(
        id=row["id"],
        source=row["source"],
        source_deal_id=row["source_deal_id"],
        source_updated_at=row["source_updated_at"],
        store=StoreView(
            store_location_id=row["store_location_id"],
            chain_name=row["chain_name"],
            store_name=row["store_name"],
            address_summary=row["address_summary"],
            latitude=row["latitude"],
            longitude=row["longitude"],
        ),
        product=ProductView(
            canonical_product_id=row["canonical_product_id"],
            canonical_name=row["canonical_name"],
            brand=row["brand"],
            package_quantity=row["product_package_quantity"],
            package_unit=row["product_package_unit"],
            category=row["category"],
        ),
        offer=OfferView(
            deal_price=row["deal_price"],
            regular_price=row["regular_price"],
            currency=row["currency"],
            package_quantity=row["offer_package_quantity"],
            package_unit=row["offer_package_unit"],
            valid_from=row["valid_from"],
            valid_to=row["valid_to"],
        ),
        normalization=NormalizationView(
            method=row["normalization_method"],
            confidence=row["normalization_confidence"],
            embedding_model=row["embedding_model"],
            embedding_version=row["embedding_version"],
            llm_model=row["llm_model"],
            prompt_version=row["prompt_version"],
            catalog_version=row["catalog_version"],
        ),
    )


class PostgresDealRepository:
    """Ranking-owned PostgreSQL projection.

    Inbox insertion and all projection upserts share one database transaction.
    A repeated event ID therefore either applies all changes once or none.
    """

    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    async def project(
        self,
        event: NormalizedDealEventV1,
        *,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        return await asyncio.to_thread(
            self._project_sync,
            event,
            topic=topic,
            partition=partition,
            offset=offset,
        )

    def _project_sync(
        self,
        event: NormalizedDealEventV1,
        *,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool:
        payload = event.payload
        with psycopg.connect(self._database_url) as connection:
            with connection.transaction():
                inserted = connection.execute(
                    """
                    INSERT INTO ranking.processed_events
                        (event_id, source_topic, source_partition, source_offset)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                    RETURNING event_id
                    """,
                    (event.event_id, topic, partition, offset),
                )
                if inserted.fetchone() is None:
                    return False

                store = payload.store
                connection.execute(
                    """
                    INSERT INTO ranking.store_locations (
                        store_location_id, chain_name, store_name, address_summary,
                        latitude, longitude, source_updated_at, last_event_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (store_location_id) DO UPDATE SET
                        chain_name = EXCLUDED.chain_name,
                        store_name = EXCLUDED.store_name,
                        address_summary = EXCLUDED.address_summary,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        source_updated_at = EXCLUDED.source_updated_at,
                        last_event_id = EXCLUDED.last_event_id,
                        updated_at = now()
                    WHERE EXCLUDED.source_updated_at > ranking.store_locations.source_updated_at
                       OR (EXCLUDED.source_updated_at = ranking.store_locations.source_updated_at
                           AND EXCLUDED.last_event_id > ranking.store_locations.last_event_id)
                    """,
                    (
                        store.store_location_id,
                        store.chain_name,
                        store.store_name,
                        store.address_summary,
                        store.latitude,
                        store.longitude,
                        payload.source_updated_at,
                        event.event_id,
                    ),
                )

                product = payload.product
                audit = payload.normalization
                connection.execute(
                    """
                    INSERT INTO ranking.catalog_projection (
                        canonical_product_id, canonical_name, brand, package_quantity,
                        package_unit, category, search_text, embedding, embedding_model,
                        embedding_version, catalog_version, last_event_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::vector, %s, %s, %s, %s)
                    ON CONFLICT (canonical_product_id) DO UPDATE SET
                        canonical_name = EXCLUDED.canonical_name,
                        brand = EXCLUDED.brand,
                        package_quantity = EXCLUDED.package_quantity,
                        package_unit = EXCLUDED.package_unit,
                        category = EXCLUDED.category,
                        search_text = EXCLUDED.search_text,
                        embedding = EXCLUDED.embedding,
                        embedding_model = EXCLUDED.embedding_model,
                        embedding_version = EXCLUDED.embedding_version,
                        catalog_version = EXCLUDED.catalog_version,
                        last_event_id = EXCLUDED.last_event_id,
                        updated_at = now()
                    WHERE EXCLUDED.catalog_version > ranking.catalog_projection.catalog_version
                       OR (EXCLUDED.catalog_version = ranking.catalog_projection.catalog_version
                           AND EXCLUDED.last_event_id > ranking.catalog_projection.last_event_id)
                    """,
                    (
                        product.canonical_product_id,
                        product.canonical_name,
                        product.brand,
                        product.package_quantity,
                        product.package_unit,
                        product.category,
                        product.search_text,
                        _vector_literal(product.embedding),
                        audit.embedding_model,
                        audit.embedding_version,
                        audit.catalog_version,
                        event.event_id,
                    ),
                )

                offer = payload.offer
                connection.execute(
                    """
                    INSERT INTO ranking.deal_projection (
                        id, source, store_location_id, source_deal_id, canonical_product_id,
                        source_updated_at, last_event_id, deal_price, regular_price,
                        currency, package_quantity, package_unit, valid_from, valid_to,
                        normalization_method, normalization_confidence, embedding_model,
                        embedding_version, llm_model, prompt_version, catalog_version
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (source, store_location_id, source_deal_id) DO UPDATE SET
                        canonical_product_id = EXCLUDED.canonical_product_id,
                        source_updated_at = EXCLUDED.source_updated_at,
                        last_event_id = EXCLUDED.last_event_id,
                        deal_price = EXCLUDED.deal_price,
                        regular_price = EXCLUDED.regular_price,
                        currency = EXCLUDED.currency,
                        package_quantity = EXCLUDED.package_quantity,
                        package_unit = EXCLUDED.package_unit,
                        valid_from = EXCLUDED.valid_from,
                        valid_to = EXCLUDED.valid_to,
                        normalization_method = EXCLUDED.normalization_method,
                        normalization_confidence = EXCLUDED.normalization_confidence,
                        embedding_model = EXCLUDED.embedding_model,
                        embedding_version = EXCLUDED.embedding_version,
                        llm_model = EXCLUDED.llm_model,
                        prompt_version = EXCLUDED.prompt_version,
                        catalog_version = EXCLUDED.catalog_version,
                        updated_at = now()
                    WHERE EXCLUDED.source_updated_at > ranking.deal_projection.source_updated_at
                       OR (EXCLUDED.source_updated_at = ranking.deal_projection.source_updated_at
                           AND EXCLUDED.last_event_id > ranking.deal_projection.last_event_id)
                    """,
                    (
                        deal_projection_id(
                            payload.source,
                            store.store_location_id,
                            payload.source_deal_id,
                        ),
                        payload.source,
                        store.store_location_id,
                        payload.source_deal_id,
                        product.canonical_product_id,
                        payload.source_updated_at,
                        event.event_id,
                        offer.deal_price,
                        offer.regular_price,
                        offer.currency,
                        offer.package_quantity,
                        offer.package_unit,
                        offer.valid_from,
                        offer.valid_to,
                        audit.method,
                        audit.confidence,
                        audit.embedding_model,
                        audit.embedding_version,
                        audit.llm_model,
                        audit.prompt_version,
                        audit.catalog_version,
                    ),
                )
        return True

    async def list_active(
        self,
        *,
        now: datetime,
        store_location_id: str | None,
        canonical_product_id: UUID | None,
        limit: int,
        offset: int,
    ) -> Sequence[ActiveDeal]:
        return await asyncio.to_thread(
            self._list_active_sync,
            now=now,
            store_location_id=store_location_id,
            canonical_product_id=canonical_product_id,
            limit=limit,
            offset=offset,
        )

    def _list_active_sync(
        self,
        *,
        now: datetime,
        store_location_id: str | None,
        canonical_product_id: UUID | None,
        limit: int,
        offset: int,
    ) -> Sequence[ActiveDeal]:
        query = _ACTIVE_DEAL_SELECT + """
        WHERE d.valid_from <= %(now)s AND d.valid_to > %(now)s
          AND (%(store_location_id)s::text IS NULL OR d.store_location_id = %(store_location_id)s)
          AND (%(canonical_product_id)s::uuid IS NULL
               OR d.canonical_product_id = %(canonical_product_id)s)
        ORDER BY d.valid_to ASC, d.id ASC
        LIMIT %(limit)s OFFSET %(offset)s
        """
        parameters = {
            "now": now,
            "store_location_id": store_location_id,
            "canonical_product_id": canonical_product_id,
            "limit": limit,
            "offset": offset,
        }
        with psycopg.connect(self._database_url, row_factory=dict_row) as connection:
            cursor = connection.execute(query, parameters)
            return [_row_to_deal(row) for row in cursor.fetchall()]

    async def get_active(self, deal_id: UUID, *, now: datetime) -> ActiveDeal | None:
        return await asyncio.to_thread(self._get_active_sync, deal_id, now=now)

    def _get_active_sync(self, deal_id: UUID, *, now: datetime) -> ActiveDeal | None:
        query = _ACTIVE_DEAL_SELECT + """
        WHERE d.id = %(deal_id)s
          AND d.valid_from <= %(now)s AND d.valid_to > %(now)s
        """
        with psycopg.connect(self._database_url, row_factory=dict_row) as connection:
            cursor = connection.execute(query, {"deal_id": deal_id, "now": now})
            row = cursor.fetchone()
            return None if row is None else _row_to_deal(row)

    async def ping(self) -> bool:
        return await asyncio.to_thread(self._ping_sync)

    async def match_product(self, name: str) -> UUID | None:
        return await asyncio.to_thread(self._match_product_sync, name)

    def _match_product_sync(self, name: str) -> UUID | None:
        embedding = vector_literal(deterministic_embedding(name))
        with psycopg.connect(self._database_url, row_factory=dict_row) as connection:
            rows = connection.execute(
                """
                SELECT canonical_product_id, 1 - (embedding <=> %s::vector) AS similarity
                FROM ranking.catalog_projection
                WHERE embedding_model = 'fixture-embedding-v1' AND embedding_version = '1'
                ORDER BY embedding <=> %s::vector, canonical_product_id
                LIMIT 2
                """,
                (embedding, embedding),
            ).fetchall()
        if not rows:
            return None
        best = Decimal(str(rows[0]["similarity"]))
        second = Decimal(str(rows[1]["similarity"])) if len(rows) > 1 else Decimal("-1")
        if best < Decimal("0.70") or best - second < Decimal("0.05"):
            return None
        return rows[0]["canonical_product_id"]

    def _ping_sync(self) -> bool:
        try:
            with psycopg.connect(self._database_url) as connection:
                connection.execute("SELECT 1")
            return True
        except psycopg.Error:
            return False
