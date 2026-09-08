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

    def __init__(self, message: str, *, error_code: str) -> None:
        super().__init__(message)
        self.error_code = error_code


@dataclass(frozen=True, slots=True)
class EmbeddingCompatibility:
    model: str
    version: str
    dimension: int


class NormalizedDealProjector:
    def __init__(
        self, repository: DealRepository, embedding: EmbeddingCompatibility
    ) -> None:
        self._repository = repository
        self._embedding = embedding

    async def consume(self, value: bytes, metadata: RecordMetadata) -> ProjectionOutcome:
        try:
            event = NormalizedDealEventV1.model_validate_json(value)
        except ValidationError as error:
            raise InvalidNormalizedDeal(
                "normalized deal does not match schema v1",
                error_code="INVALID_NORMALIZED_DEAL_V1",
            ) from error

        audit = event.payload.normalization
        product = event.payload.product
        if (
            audit.embedding_model != self._embedding.model
            or audit.embedding_version != self._embedding.version
            or len(product.embedding) != self._embedding.dimension
        ):
            raise InvalidNormalizedDeal(
                "normalized deal uses an incompatible embedding",
                error_code="INCOMPATIBLE_EMBEDDING",
            )

        applied = await self._repository.project(
            event,
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
        )
        return ProjectionOutcome.APPLIED if applied else ProjectionOutcome.DUPLICATE
