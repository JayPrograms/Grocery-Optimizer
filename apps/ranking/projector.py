from dataclasses import dataclass
from enum import StrEnum

from pydantic import ValidationError

from apps.ranking.repository import DealRepository
from packages.event_contracts import NormalizedDealEventV1


class ProjectionOutcome(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class RecordMetadata:
    topic: str
    partition: int
    offset: int


class InvalidNormalizedDeal(ValueError):
    """Terminal input error suitable for the common DLQ."""


class NormalizedDealProjector:
    def __init__(self, repository: DealRepository) -> None:
        self._repository = repository

    async def consume(self, value: bytes, metadata: RecordMetadata) -> ProjectionOutcome:
        try:
            event = NormalizedDealEventV1.model_validate_json(value)
        except ValidationError as error:
            raise InvalidNormalizedDeal("normalized deal does not match schema v1") from error

        applied = await self._repository.project(
            event,
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
        )
        return ProjectionOutcome.APPLIED if applied else ProjectionOutcome.DUPLICATE
