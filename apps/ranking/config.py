from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    kafka_bootstrap_servers: str
    normalized_deals_topic: str
    dlq_topic: str
    consumer_group: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.environ.get(
                "RANKING_DATABASE_URL",
                "postgresql://ranking:ranking@localhost:5432/grocery",
            ),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            normalized_deals_topic=os.environ.get(
                "NORMALIZED_DEALS_TOPIC", "grocery.deals.normalized.v1"
            ),
            dlq_topic=os.environ.get("DEALS_DLQ_TOPIC", "grocery.deals.dlq.v1"),
            consumer_group=os.environ.get("RANKING_CONSUMER_GROUP", "ranking-projector-v1"),
        )
