"""Deterministic, offline normalization and transactional persistence for the MVP."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import os
from typing import Literal
from uuid import UUID, NAMESPACE_URL, uuid5

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict

from domain import (
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    EMBEDDING_VERSION,
    deterministic_embedding,
    normalize_product_text,
    vector_literal,
)

from event_contracts import (
    CanonicalProduct,
    NormalizationAudit,
    NormalizedDealEventV1,
    NormalizedDealPayload,
    RawDealEventV1,
)


LLM_MODEL = "fixture-llm-v1"
PROMPT_VERSION = "product-match-v1"


def database_url() -> str:
    explicit = os.getenv("NORMALIZATION_DATABASE_URL")
    if explicit:
        return explicit
    return psycopg.conninfo.make_conninfo(
        host=os.environ["POSTGRES_HOST"],
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


normalize_text = normalize_product_text
embedding_for = deterministic_embedding


class LlmDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: Literal["create", "unresolved"]
    canonical_name: str | None = None
    category: str | None = None


class FixtureLlmProvider:
    """Offline structured fallback used by the local MVP and default tests."""

    def decide(self, cleaned_name: str) -> LlmDecision:
        if not cleaned_name or "unresolvable" in cleaned_name:
            return LlmDecision(action="unresolved")
        category = "produce" if any(word in cleaned_name for word in ("fruit", "apple")) else "grocery"
        return LlmDecision(action="create", canonical_name=cleaned_name, category=category)


class UnresolvableProduct(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RecordMetadata:
    topic: str
    partition: int
    offset: int


@dataclass(frozen=True, slots=True)
class ProcessResult:
    outcome: Literal["applied", "duplicate", "stale"]
    method: Literal["alias", "vector", "llm"] | None = None


SEED_PRODUCTS = (
    ("whole milk", "dairy"),
    ("eggs", "dairy"),
    ("bread", "bakery"),
)


class NormalizationRepository:
    def __init__(self, connection_string: str | None = None) -> None:
        self.connection_string = connection_string or database_url()
        self.llm = FixtureLlmProvider()

    async def ensure_seed_catalog(self) -> None:
        await asyncio.to_thread(self._ensure_seed_catalog_sync)

    def _ensure_seed_catalog_sync(self) -> None:
        with psycopg.connect(self.connection_string) as connection:
            for name, category in SEED_PRODUCTS:
                fingerprint = normalize_text(name)
                product_id = uuid5(NAMESPACE_URL, f"grocery-product:{fingerprint}")
                connection.execute(
                    """
                    INSERT INTO normalization.products (
                        canonical_product_id, fingerprint, canonical_name, brand,
                        package_quantity, package_unit, category, search_text, embedding,
                        embedding_model, embedding_version, catalog_version
                    ) VALUES (%s, %s, %s, NULL, NULL, NULL, %s, %s, %s::vector, %s, %s, 1)
                    ON CONFLICT (fingerprint) DO NOTHING
                    """,
                    (
                        product_id,
                        fingerprint,
                        name,
                        category,
                        name,
                        vector_literal(embedding_for(name)),
                        EMBEDDING_MODEL,
                        EMBEDDING_VERSION,
                    ),
                )

    async def process(self, event: RawDealEventV1, metadata: RecordMetadata) -> ProcessResult:
        return await asyncio.to_thread(self._process_sync, event, metadata)

    def _select_product(self, connection: psycopg.Connection, event: RawDealEventV1):
        payload = event.payload
        fingerprint = normalize_text(payload.product.raw_name)
        alias = connection.execute(
            """
            SELECT p.* FROM normalization.aliases a
            JOIN normalization.products p USING (canonical_product_id)
            WHERE a.source = %s AND a.fingerprint = %s
            """,
            (payload.source, fingerprint),
        ).fetchone()
        if alias:
            return alias, "alias", Decimal("1")

        embedding = embedding_for(fingerprint)
        candidates = connection.execute(
            """
            SELECT *, 1 - (embedding <=> %s::vector) AS similarity
            FROM normalization.products
            WHERE embedding_model = %s AND embedding_version = %s
            ORDER BY embedding <=> %s::vector, canonical_product_id
            LIMIT 2
            """,
            (
                vector_literal(embedding),
                EMBEDDING_MODEL,
                EMBEDDING_VERSION,
                vector_literal(embedding),
            ),
        ).fetchall()
        best = candidates[0] if candidates else None
        runner_up = candidates[1] if len(candidates) > 1 else None
        if best and Decimal(str(best["similarity"])) >= Decimal("0.90") and (
            runner_up is None
            or Decimal(str(best["similarity"])) - Decimal(str(runner_up["similarity"]))
            >= Decimal("0.05")
        ):
            return best, "vector", Decimal(str(best["similarity"]))

        decision = self.llm.decide(fingerprint)
        if decision.action == "unresolved" or not decision.canonical_name:
            raise UnresolvableProduct("No compatible canonical product candidate was found")
        canonical_fingerprint = normalize_text(decision.canonical_name)
        product_id = uuid5(NAMESPACE_URL, f"grocery-product:{canonical_fingerprint}")
        product_embedding = embedding_for(decision.canonical_name)
        connection.execute(
            """
            INSERT INTO normalization.products (
                canonical_product_id, fingerprint, canonical_name, brand,
                package_quantity, package_unit, category, search_text, embedding,
                embedding_model, embedding_version, catalog_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::vector, %s, %s, 1)
            ON CONFLICT (fingerprint) DO NOTHING
            """,
            (
                product_id,
                canonical_fingerprint,
                decision.canonical_name,
                payload.product.raw_brand,
                payload.product.raw_size,
                payload.product.raw_unit.casefold() if payload.product.raw_unit else None,
                decision.category,
                decision.canonical_name,
                vector_literal(product_embedding),
                EMBEDDING_MODEL,
                EMBEDDING_VERSION,
            ),
        )
        product = connection.execute(
            "SELECT * FROM normalization.products WHERE fingerprint = %s",
            (canonical_fingerprint,),
        ).fetchone()
        return product, "llm", Decimal("0.75")

    def _process_sync(self, event: RawDealEventV1, metadata: RecordMetadata) -> ProcessResult:
        with psycopg.connect(self.connection_string, row_factory=dict_row) as connection:
            inserted = connection.execute(
                """
                INSERT INTO normalization.processed_events (
                    event_id, source_topic, source_partition, source_offset
                ) VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING RETURNING event_id
                """,
                (event.event_id, metadata.topic, metadata.partition, metadata.offset),
            ).fetchone()
            if not inserted:
                return ProcessResult("duplicate")

            product, method, confidence = self._select_product(connection, event)
            payload = event.payload
            alias_fingerprint = normalize_text(payload.product.raw_name)
            connection.execute(
                """
                INSERT INTO normalization.aliases (source, fingerprint, canonical_product_id)
                VALUES (%s, %s, %s) ON CONFLICT (source, fingerprint) DO NOTHING
                """,
                (payload.source, alias_fingerprint, product["canonical_product_id"]),
            )

            normalized_event_id = uuid5(NAMESPACE_URL, f"normalized:{event.event_id}")
            normalized = NormalizedDealEventV1(
                event_id=normalized_event_id,
                event_type="grocery.deal.normalized",
                schema_version=1,
                occurred_at=datetime.now(timezone.utc),
                producer="normalization-service",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                trace_id=event.trace_id,
                payload=NormalizedDealPayload(
                    source=payload.source,
                    source_deal_id=payload.source_deal_id,
                    source_updated_at=payload.source_updated_at,
                    store=payload.store,
                    product=CanonicalProduct(
                        canonical_product_id=product["canonical_product_id"],
                        canonical_name=product["canonical_name"],
                        brand=product["brand"],
                        package_quantity=product["package_quantity"],
                        package_unit=product["package_unit"],
                        category=product["category"],
                        search_text=product["search_text"],
                        embedding=embedding_for(product["search_text"]),
                    ),
                    offer=payload.offer,
                    normalization=NormalizationAudit(
                        method=method,
                        confidence=confidence,
                        embedding_model=EMBEDDING_MODEL,
                        embedding_version=EMBEDDING_VERSION,
                        llm_model=LLM_MODEL if method == "llm" else None,
                        prompt_version=PROMPT_VERSION if method == "llm" else None,
                        catalog_version=product["catalog_version"],
                    ),
                ),
            )
            connection.execute(
                """
                INSERT INTO normalization.normalization_audit (
                    event_id, source, source_deal_id, normalization_method,
                    confidence, embedding_model, embedding_version, llm_model,
                    prompt_version, catalog_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event.event_id,
                    payload.source,
                    payload.source_deal_id,
                    method,
                    confidence,
                    EMBEDDING_MODEL,
                    EMBEDDING_VERSION,
                    LLM_MODEL if method == "llm" else None,
                    PROMPT_VERSION if method == "llm" else None,
                    product["catalog_version"],
                ),
            )
            aggregate_key = f"{payload.source}:{payload.store.store_location_id}:{payload.source_deal_id}"
            deal_id = uuid5(NAMESPACE_URL, f"deal:{aggregate_key}")
            applied = connection.execute(
                """
                INSERT INTO normalization.deals (
                    id, source, store_location_id, source_deal_id, source_updated_at,
                    last_raw_event_id, canonical_product_id, normalized_event
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, store_location_id, source_deal_id) DO UPDATE SET
                    source_updated_at = EXCLUDED.source_updated_at,
                    last_raw_event_id = EXCLUDED.last_raw_event_id,
                    canonical_product_id = EXCLUDED.canonical_product_id,
                    normalized_event = EXCLUDED.normalized_event,
                    updated_at = now()
                WHERE EXCLUDED.source_updated_at > normalization.deals.source_updated_at
                   OR (EXCLUDED.source_updated_at = normalization.deals.source_updated_at
                       AND EXCLUDED.last_raw_event_id > normalization.deals.last_raw_event_id)
                RETURNING id
                """,
                (
                    deal_id,
                    payload.source,
                    payload.store.store_location_id,
                    payload.source_deal_id,
                    payload.source_updated_at,
                    event.event_id,
                    product["canonical_product_id"],
                    Jsonb(normalized.model_dump(mode="json")),
                ),
            ).fetchone()
            if not applied:
                return ProcessResult("stale", method)
            connection.execute(
                """
                INSERT INTO normalization.outbox (event_id, aggregate_key, event_value)
                VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING
                """,
                (normalized.event_id, aggregate_key, Jsonb(normalized.model_dump(mode="json"))),
            )
            return ProcessResult("applied", method)
