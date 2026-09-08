"""Pydantic contract for ``grocery.deals.normalized.v1``.

The fields mirror docs/ARCHITECTURE.md section 3.5. This package contains only
the wire contract; persistence models remain owned by the ranking service.
"""

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


NonEmptyString = Annotated[str, Field(min_length=1, max_length=255)]
Money = Annotated[Decimal, Field(ge=0, max_digits=14, decimal_places=4)]
PositiveQuantity = Annotated[Decimal, Field(gt=0, max_digits=14, decimal_places=4)]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


def _require_utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{field_name} must be UTC")


class StoreLocationV1(StrictModel):
    store_location_id: NonEmptyString
    chain_name: NonEmptyString
    store_name: NonEmptyString
    address_summary: Annotated[str, Field(min_length=1, max_length=500)]
    latitude: Annotated[Decimal, Field(ge=-90, le=90)]
    longitude: Annotated[Decimal, Field(ge=-180, le=180)]


class OfferV1(StrictModel):
    deal_price: Money
    regular_price: Money | None
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    package_quantity: PositiveQuantity | None
    package_unit: Annotated[str, Field(min_length=1, max_length=32)] | None
    valid_from: datetime
    valid_to: datetime

    @model_validator(mode="after")
    def validate_window_and_package(self) -> "OfferV1":
        _require_utc(self.valid_from, "valid_from")
        _require_utc(self.valid_to, "valid_to")
        if self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be later than valid_from")
        if (self.package_quantity is None) != (self.package_unit is None):
            raise ValueError("package_quantity and package_unit must be provided together")
        return self


class NormalizedProductV1(StrictModel):
    canonical_product_id: UUID
    canonical_name: NonEmptyString
    brand: Annotated[str, Field(min_length=1, max_length=255)] | None
    package_quantity: PositiveQuantity | None
    package_unit: Annotated[str, Field(min_length=1, max_length=32)] | None
    category: Annotated[str, Field(min_length=1, max_length=128)] | None
    search_text: Annotated[str, Field(min_length=1, max_length=1000)]
    embedding: Annotated[list[Decimal], Field(min_length=1)]

    @model_validator(mode="after")
    def validate_package(self) -> "NormalizedProductV1":
        if (self.package_quantity is None) != (self.package_unit is None):
            raise ValueError("package_quantity and package_unit must be provided together")
        if any(not value.is_finite() for value in self.embedding):
            raise ValueError("embedding values must be finite")
        return self


class NormalizationMethod(StrEnum):
    ALIAS = "alias"
    VECTOR = "vector"
    LLM = "llm"


class NormalizationAuditV1(StrictModel):
    method: NormalizationMethod
    confidence: Annotated[Decimal, Field(ge=0, le=1)]
    embedding_model: NonEmptyString
    embedding_version: NonEmptyString
    llm_model: NonEmptyString | None
    prompt_version: NonEmptyString | None
    catalog_version: Annotated[int, Field(ge=1)]

    @model_validator(mode="after")
    def validate_method_metadata(self) -> "NormalizationAuditV1":
        has_llm_metadata = self.llm_model is not None and self.prompt_version is not None
        if self.method is NormalizationMethod.LLM and not has_llm_metadata:
            raise ValueError("llm normalization requires llm_model and prompt_version")
        if self.method is not NormalizationMethod.LLM and (
            self.llm_model is not None or self.prompt_version is not None
        ):
            raise ValueError("non-llm normalization cannot contain LLM metadata")
        return self


class NormalizedDealPayloadV1(StrictModel):
    source: NonEmptyString
    source_deal_id: NonEmptyString
    source_updated_at: datetime
    store: StoreLocationV1
    product: NormalizedProductV1
    offer: OfferV1
    normalization: NormalizationAuditV1

    @model_validator(mode="after")
    def validate_source_timestamp(self) -> "NormalizedDealPayloadV1":
        _require_utc(self.source_updated_at, "source_updated_at")
        return self


class NormalizedDealEventV1(StrictModel):
    event_id: UUID
    event_type: Literal["grocery.deal.normalized"]
    schema_version: Literal[1]
    occurred_at: datetime
    producer: NonEmptyString
    correlation_id: UUID
    causation_id: UUID | None
    trace_id: Annotated[str, Field(min_length=1, max_length=255)]
    payload: NormalizedDealPayloadV1

    @model_validator(mode="after")
    def validate_occurred_at(self) -> "NormalizedDealEventV1":
        _require_utc(self.occurred_at, "occurred_at")
        return self
