import pytest
from psycopg.conninfo import conninfo_to_dict

from apps.ranking.config import Settings


def test_projector_requires_embedding_compatibility_configuration(monkeypatch) -> None:
    monkeypatch.delenv("RANKING_EMBEDDING_MODEL", raising=False)
    monkeypatch.delenv("RANKING_EMBEDDING_VERSION", raising=False)
    monkeypatch.delenv("RANKING_EMBEDDING_DIMENSION", raising=False)

    with pytest.raises(ValueError, match="RANKING_EMBEDDING_MODEL"):
        Settings.from_env().validate_projector()


def test_transactional_id_is_stable_and_configurable(monkeypatch) -> None:
    monkeypatch.setenv("RANKING_PROJECTOR_TRANSACTIONAL_ID", "ranking-projector-dlq-7")
    monkeypatch.setenv("RANKING_EMBEDDING_MODEL", "fixture-embedding")
    monkeypatch.setenv("RANKING_EMBEDDING_VERSION", "1")
    monkeypatch.setenv("RANKING_EMBEDDING_DIMENSION", "1536")

    first = Settings.from_env()
    second = Settings.from_env()
    first.validate_projector()

    assert first.transactional_id == "ranking-projector-dlq-7"
    assert second.transactional_id == first.transactional_id


def test_database_url_uses_shared_postgres_environment(monkeypatch) -> None:
    monkeypatch.delenv("RANKING_DATABASE_URL", raising=False)
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "grocery_savings")
    monkeypatch.setenv("POSTGRES_USER", "grocery_app")
    monkeypatch.setenv("POSTGRES_PASSWORD", "password with spaces")

    connection = conninfo_to_dict(Settings.from_env().database_url)

    assert connection == {
        "host": "postgres",
        "port": "5432",
        "dbname": "grocery_savings",
        "user": "grocery_app",
        "password": "password with spaces",
    }


def test_recommendation_settings_are_decimal_and_configurable(monkeypatch) -> None:
    monkeypatch.setenv("CURRENCY", "CAD")
    monkeypatch.setenv("FUEL_PRICE_PER_LITRE", "1.749")
    monkeypatch.setenv("ROAD_FACTOR", "1.25")

    settings = Settings.from_env()
    settings.validate_recommendations()

    assert str(settings.fuel_price_per_litre) == "1.749"
    assert str(settings.road_factor) == "1.25"


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CURRENCY", "cad", "CURRENCY"),
        ("FUEL_PRICE_PER_LITRE", "-1", "FUEL_PRICE_PER_LITRE"),
        ("ROAD_FACTOR", "0", "ROAD_FACTOR"),
    ],
)
def test_invalid_recommendation_settings_are_rejected(
    monkeypatch, name: str, value: str, message: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=message):
        Settings.from_env().validate_recommendations()
