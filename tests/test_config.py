import pytest

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
