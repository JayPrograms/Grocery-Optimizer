from dataclasses import dataclass
from decimal import Decimal
import os

from psycopg.conninfo import make_conninfo


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    kafka_bootstrap_servers: str
    normalized_deals_topic: str
    dlq_topic: str
    consumer_group: str
    transactional_id: str
    embedding_model: str | None
    embedding_version: str | None
    embedding_dimension: int
    dlq_max_event_bytes: int
    database_retry_base_seconds: float
    database_retry_max_seconds: float
    currency: str = "CAD"
    fuel_price_per_litre: Decimal = Decimal("1.60")
    road_factor: Decimal = Decimal("1.2")

    @classmethod
    def from_env(cls) -> "Settings":
        database_url = os.environ.get("RANKING_DATABASE_URL")
        if database_url is None and all(
            os.environ.get(name)
            for name in ("POSTGRES_HOST", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
        ):
            database_url = make_conninfo(
                host=os.environ["POSTGRES_HOST"],
                port=os.environ.get("POSTGRES_PORT", "5432"),
                dbname=os.environ["POSTGRES_DB"],
                user=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PASSWORD"],
            )
        return cls(
            database_url=database_url
            or "postgresql://ranking:ranking@localhost:5432/grocery",
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            normalized_deals_topic=os.environ.get(
                "NORMALIZED_DEALS_TOPIC", "grocery.deals.normalized.v1"
            ),
            dlq_topic=os.environ.get("DEALS_DLQ_TOPIC", "grocery.deals.dlq.v1"),
            consumer_group=os.environ.get("RANKING_CONSUMER_GROUP", "ranking-projector-v1"),
            transactional_id=os.environ.get(
                "RANKING_PROJECTOR_TRANSACTIONAL_ID", "ranking-projector-dlq-0"
            ),
            embedding_model=os.environ.get("RANKING_EMBEDDING_MODEL"),
            embedding_version=os.environ.get("RANKING_EMBEDDING_VERSION"),
            embedding_dimension=int(os.environ.get("RANKING_EMBEDDING_DIMENSION", "0")),
            dlq_max_event_bytes=int(os.environ.get("DLQ_MAX_EVENT_BYTES", "900000")),
            database_retry_base_seconds=float(
                os.environ.get("DATABASE_RETRY_BASE_SECONDS", "1")
            ),
            database_retry_max_seconds=float(
                os.environ.get("DATABASE_RETRY_MAX_SECONDS", "30")
            ),
            currency=os.environ.get("CURRENCY", "CAD"),
            fuel_price_per_litre=Decimal(
                os.environ.get("FUEL_PRICE_PER_LITRE", "1.60")
            ),
            road_factor=Decimal(os.environ.get("ROAD_FACTOR", "1.2")),
        )

    def validate_recommendations(self) -> None:
        if (
            len(self.currency) != 3
            or not self.currency.isascii()
            or not self.currency.isalpha()
            or not self.currency.isupper()
        ):
            raise ValueError("CURRENCY must be a three-letter uppercase code")
        if not self.fuel_price_per_litre.is_finite() or self.fuel_price_per_litre < 0:
            raise ValueError("FUEL_PRICE_PER_LITRE must be non-negative")
        if not self.road_factor.is_finite() or self.road_factor <= 0:
            raise ValueError("ROAD_FACTOR must be positive")

    def validate_projector(self) -> None:
        if not self.embedding_model or not self.embedding_version:
            raise ValueError(
                "RANKING_EMBEDDING_MODEL and RANKING_EMBEDDING_VERSION are required"
            )
        if self.embedding_dimension != 1536:
            raise ValueError("RANKING_EMBEDDING_DIMENSION must match migration dimension 1536")
        if not 8_192 <= self.dlq_max_event_bytes <= 900_000:
            raise ValueError("DLQ_MAX_EVENT_BYTES must be between 8192 and 900000")
        if not self.transactional_id:
            raise ValueError("RANKING_PROJECTOR_TRANSACTIONAL_ID must not be empty")
        if self.database_retry_base_seconds <= 0:
            raise ValueError("DATABASE_RETRY_BASE_SECONDS must be positive")
        if self.database_retry_max_seconds < self.database_retry_base_seconds:
            raise ValueError(
                "DATABASE_RETRY_MAX_SECONDS must be at least DATABASE_RETRY_BASE_SECONDS"
            )
