"""Version 1 Pydantic schemas for public Kafka deal events."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationInfo,
    field_validator,
    model_validator,
)


def _require_utc(value: datetime) -> datetime:
    """Reject naive and non-UTC timestamps without silently converting them."""
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be timezone-aware UTC")
    return value


UtcDateTime = Annotated[datetime, AfterValidator(_require_utc)]
ShortText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=128)
]
Identifier = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=256)
]
DisplayText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=512)
]
SearchText = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=1024)
]
Unit = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=32)
]
Currency = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=3,
        max_length=3,
        pattern=r"^[A-Z]{3}$",
    ),
]
NonNegativeDecimal = Annotated[
    Decimal, Field(ge=Decimal("0"), allow_inf_nan=False)
]
PositiveDecimal = Annotated[
    Decimal, Field(gt=Decimal("0"), allow_inf_nan=False)
]
Confidence = Annotated[
    Decimal,
    Field(ge=Decimal("0"), le=Decimal("1"), allow_inf_nan=False),
]
Latitude = Annotated[
    Decimal,
    Field(ge=Decimal("-90"), le=Decimal("90"), allow_inf_nan=False),
]
Longitude = Annotated[
    Decimal,
    Field(ge=Decimal("-180"), le=Decimal("180"), allow_inf_nan=False),
]
EmbeddingValue = Annotated[Decimal, Field(allow_inf_nan=False)]


class ContractModel(BaseModel):
    """Strict, immutable base for event envelopes and nested contract types."""

    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


class StoreLocation(ContractModel):
    """A retailer's physical store location."""

    store_location_id: Identifier
    chain_name: DisplayText
    store_name: DisplayText
    address_summary: DisplayText
    latitude: Latitude
    longitude: Longitude

    @property
    def retailer(self) -> str:
        """Domain-friendly name for the documented ``chain_name`` wire field."""
        return self.chain_name


class RawProduct(ContractModel):
    raw_name: DisplayText
    raw_brand: DisplayText | None
    raw_size: PositiveDecimal | None
    raw_unit: Unit | None

    @model_validator(mode="after")
    def require_size_and_unit_together(self) -> RawProduct:
        if (self.raw_size is None) != (self.raw_unit is None):
            raise ValueError("raw_size and raw_unit must either both be set or both be null")
        return self


class Offer(ContractModel):
    deal_price: NonNegativeDecimal
    regular_price: NonNegativeDecimal | None
    currency: Currency
    package_quantity: PositiveDecimal | None
    package_unit: Unit | None
    valid_from: UtcDateTime
    valid_to: UtcDateTime

    @model_validator(mode="after")
    def validate_offer(self) -> Offer:
        if (self.package_quantity is None) != (self.package_unit is None):
            raise ValueError(
                "package_quantity and package_unit must either both be set or both be null"
            )
        if self.valid_to <= self.valid_from:
            raise ValueError("valid_to must be later than valid_from")
        return self


class RawDealPayload(ContractModel):
    source: ShortText
    source_deal_id: Identifier
    source_updated_at: UtcDateTime
    store: StoreLocation
    product: RawProduct
    offer: Offer

    @property
    def source_timestamp(self) -> datetime:
        """Domain-friendly name for the documented source timestamp field."""
        return self.source_updated_at


class EventEnvelope(ContractModel):
    event_id: UUID
    event_type: str
    schema_version: int
    occurred_at: UtcDateTime
    producer: ShortText
    correlation_id: UUID
    causation_id: UUID | None
    trace_id: Identifier


class RawDealEvent(EventEnvelope):
    event_type: Literal["grocery.deal.raw"]
    schema_version: Literal[1]
    payload: RawDealPayload


class CanonicalProduct(ContractModel):
    canonical_product_id: UUID
    canonical_name: DisplayText
    brand: DisplayText | None
    package_quantity: PositiveDecimal | None
    package_unit: Unit | None
    category: ShortText | None
    search_text: SearchText
    embedding: Annotated[list[EmbeddingValue], Field(min_length=1, max_length=4096)]

    @model_validator(mode="after")
    def require_package_quantity_and_unit_together(self) -> CanonicalProduct:
        if (self.package_quantity is None) != (self.package_unit is None):
            raise ValueError(
                "package_quantity and package_unit must either both be set or both be null"
            )
        return self


class NormalizationAudit(ContractModel):
    method: Literal["alias", "vector", "llm"]
    confidence: Confidence
    embedding_model: ShortText
    embedding_version: ShortText
    llm_model: ShortText | None
    prompt_version: ShortText | None
    catalog_version: Annotated[int, Field(ge=1)]

    @model_validator(mode="after")
    def validate_method_metadata(self) -> NormalizationAudit:
        has_llm_metadata = self.llm_model is not None and self.prompt_version is not None
        if self.method == "llm" and not has_llm_metadata:
            raise ValueError("llm method requires llm_model and prompt_version")
        if self.method != "llm" and (
            self.llm_model is not None or self.prompt_version is not None
        ):
            raise ValueError("non-llm methods must not include LLM metadata")
        return self


class NormalizedDealPayload(ContractModel):
    source: ShortText
    source_deal_id: Identifier
    source_updated_at: UtcDateTime
    store: StoreLocation
    product: CanonicalProduct
    offer: Offer
    normalization: NormalizationAudit

    @model_validator(mode="after")
    def validate_embedding_dimension(self, info: ValidationInfo) -> NormalizedDealPayload:
        """Apply the consumer's configured model dimension when supplied as context."""
        if not info.context:
            return self

        expected_dimension = info.context.get("embedding_dimension")
        if expected_dimension is None:
            return self
        if isinstance(expected_dimension, bool) or not isinstance(expected_dimension, int):
            raise ValueError("embedding_dimension context must be an integer")
        if expected_dimension < 1:
            raise ValueError("embedding_dimension context must be positive")
        if len(self.product.embedding) != expected_dimension:
            raise ValueError(
                "embedding dimension does not match the configured model dimension"
            )
        return self

    @property
    def source_timestamp(self) -> datetime:
        """Domain-friendly name for the documented source timestamp field."""
        return self.source_updated_at


class NormalizedDealEvent(EventEnvelope):
    event_type: Literal["grocery.deal.normalized"]
    schema_version: Literal[1]
    payload: NormalizedDealPayload


FailedStage = Literal["deserialize", "validate", "normalize", "persist", "project"]
FailureClassification = Literal[
    "terminal_invalid", "terminal_unresolvable", "retries_exhausted"
]
ErrorCode = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=128,
        pattern=r"^[^\r\n]+$",
    ),
]
ErrorMessage = Annotated[
    str,
    StringConstraints(
        strip_whitespace=True,
        min_length=1,
        max_length=2048,
        pattern=r"^[^\r\n]+$",
    ),
]
KafkaTopic = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=249)
]
KafkaKey = Annotated[str, StringConstraints(min_length=1, max_length=512)]
MAX_RAW_DLQ_BYTES = 65_536
MAX_RAW_DLQ_BASE64_CHARS = ((MAX_RAW_DLQ_BYTES + 2) // 3) * 4


class DeadLetterPayload(ContractModel):
    original_topic: KafkaTopic
    original_partition: Annotated[int, Field(ge=0)]
    original_offset: Annotated[int, Field(ge=0)]
    original_key: KafkaKey | None
    original_event: dict[str, Any] | None
    raw_value_base64: Annotated[str, Field(max_length=MAX_RAW_DLQ_BASE64_CHARS)] | None
    failed_stage: FailedStage
    classification: FailureClassification
    attempts: Annotated[int, Field(ge=1)]
    error_code: ErrorCode
    error_message: ErrorMessage
    failed_at: UtcDateTime

    @field_validator("raw_value_base64")
    @classmethod
    def validate_raw_value(cls, value: str | None) -> str | None:
        if value is None:
            return value
        try:
            decoded = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as error:
            raise ValueError("raw_value_base64 must be valid base64") from error
        if len(decoded) > MAX_RAW_DLQ_BYTES:
            raise ValueError(
                f"raw_value_base64 must decode to at most {MAX_RAW_DLQ_BYTES} bytes"
            )
        return value

    @model_validator(mode="after")
    def require_one_original_representation(self) -> DeadLetterPayload:
        has_event = self.original_event is not None
        has_raw_value = self.raw_value_base64 is not None
        if has_event == has_raw_value:
            raise ValueError(
                "exactly one of original_event or raw_value_base64 must be provided"
            )
        return self


class DeadLetterEvent(EventEnvelope):
    event_type: Literal["grocery.deal.dead_lettered"]
    schema_version: Literal[1]
    payload: DeadLetterPayload


# Explicit version aliases support callers that name contracts by their wire version.
RawDealEventV1 = RawDealEvent
NormalizedDealEventV1 = NormalizedDealEvent
DeadLetterEventV1 = DeadLetterEvent

