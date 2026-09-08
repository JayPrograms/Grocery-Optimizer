"""Pydantic contract for ``grocery.deals.dlq.v1``.

This implements the existing wire contract documented in ARCHITECTURE.md section
3.6; it does not add or alter public event fields.
"""

from datetime import datetime
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import Field, model_validator

from .normalized_deal import NonEmptyString, StrictModel, _require_utc


class DeadLetterPayloadV1(StrictModel):
    original_topic: NonEmptyString
    original_partition: Annotated[int, Field(ge=0)]
    original_offset: Annotated[int, Field(ge=0)]
    original_key: Annotated[str, Field(max_length=500)] | None
    original_event: dict[str, Any] | None
    raw_value_base64: str | None
    failed_stage: Literal["deserialize", "validate", "normalize", "persist", "project"]
    classification: Literal[
        "terminal_invalid", "terminal_unresolvable", "retries_exhausted"
    ]
    attempts: Annotated[int, Field(ge=1)]
    error_code: Annotated[str, Field(min_length=1, max_length=128)]
    error_message: Annotated[str, Field(min_length=1, max_length=500)]
    failed_at: datetime

    @model_validator(mode="after")
    def validate_original(self) -> "DeadLetterPayloadV1":
        if (self.original_event is None) == (self.raw_value_base64 is None):
            raise ValueError("exactly one original representation is required")
        _require_utc(self.failed_at, "failed_at")
        return self


class DeadLetterEventV1(StrictModel):
    event_id: UUID
    event_type: Literal["grocery.deal.dead_lettered"]
    schema_version: Literal[1]
    occurred_at: datetime
    producer: NonEmptyString
    correlation_id: UUID
    causation_id: UUID | None
    trace_id: Annotated[str, Field(min_length=1, max_length=255)]
    payload: DeadLetterPayloadV1

    @model_validator(mode="after")
    def validate_occurred_at(self) -> "DeadLetterEventV1":
        _require_utc(self.occurred_at, "occurred_at")
        return self
