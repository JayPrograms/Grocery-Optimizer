from __future__ import annotations

import json
from types import ModuleType

import pytest
from fastapi.responses import JSONResponse

from apps.ingestion import main as ingestion
from apps.normalization import main as normalization
from apps.ranking import main as ranking


@pytest.mark.parametrize("module", [ingestion, normalization])
def test_service_exposes_only_health_routes(module: ModuleType) -> None:
    paths = {route.path for route in module.app.routes}

    assert paths == {"/health/live", "/health/ready"}


def test_ranking_service_adds_deal_routes_to_health_routes() -> None:
    paths = {route.path for route in ranking.app.routes}

    assert paths == {
        "/health/live",
        "/health/ready",
        "/v1/deals",
        "/v1/deals/{deal_id}",
        "/v1/optimizations",
    }


@pytest.mark.parametrize("module", [ranking, ingestion, normalization])
def test_liveness_does_not_query_dependencies(module: ModuleType) -> None:
    response = module.live()

    assert response == {
        "status": "ok",
        "service": module.SERVICE_NAME,
        "dependencies": {},
    }


def test_ranking_readiness_reports_postgres_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ranking, "check_postgres", lambda: None)

    response = ranking.ready()

    assert isinstance(response, dict)
    assert response["dependencies"] == {"postgres": "ok"}


def test_ingestion_readiness_reports_kafka_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ingestion, "check_kafka", lambda: None)

    response = ingestion.ready()

    assert isinstance(response, dict)
    assert response["dependencies"] == {"kafka": "ok"}


def test_normalization_readiness_reports_both_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(normalization, "check_kafka", lambda: None)
    monkeypatch.setattr(normalization, "check_postgres", lambda: None)

    response = normalization.ready()

    assert isinstance(response, dict)
    assert response["dependencies"] == {
        "kafka": "ok",
        "postgres": "ok",
    }


@pytest.mark.parametrize(
    ("module", "check_name", "expected_dependencies"),
    [
        (ranking, "check_postgres", {"postgres": "unavailable"}),
        (ingestion, "check_kafka", {"kafka": "unavailable"}),
    ],
)
def test_api_readiness_sanitizes_dependency_failures(
    module: ModuleType,
    check_name: str,
    expected_dependencies: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail() -> None:
        raise RuntimeError("sensitive dependency detail")

    monkeypatch.setattr(module, check_name, fail)

    response = module.ready()

    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    body = json.loads(response.body)
    assert body["dependencies"] == expected_dependencies
    assert b"sensitive" not in response.body


def test_normalization_checks_all_dependencies_when_one_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    def succeed() -> None:
        called.append("kafka")

    def fail() -> None:
        called.append("postgres")
        raise RuntimeError("sensitive dependency detail")

    monkeypatch.setattr(normalization, "check_kafka", succeed)
    monkeypatch.setattr(normalization, "check_postgres", fail)

    response = normalization.ready()

    assert called == ["kafka", "postgres"]
    assert isinstance(response, JSONResponse)
    assert response.status_code == 503
    body = json.loads(response.body)
    assert body["dependencies"] == {
        "kafka": "ok",
        "postgres": "unavailable",
    }
    assert b"sensitive" not in response.body
