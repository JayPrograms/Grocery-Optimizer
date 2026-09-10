from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from typing import Any

import pytest


pytestmark = [
    pytest.mark.smoke,
    pytest.mark.skipif(
        os.getenv("RUN_INFRA_SMOKE") != "1",
        reason="set RUN_INFRA_SMOKE=1 with the Compose stack running",
    ),
]

EXPECTED_SERVICES = {
    "api",
    "ingestion-api",
    "kafka",
    "normalization-worker",
    "postgres",
}


def compose(*arguments: str) -> str:
    result = subprocess.run(
        ["docker", "compose", *arguments],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result.stdout.strip()


def get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=5) as response:
        assert response.status == 200
        return json.load(response)


def test_expected_compose_services_are_running() -> None:
    running = set(compose("ps", "--status", "running", "--services").splitlines())

    assert EXPECTED_SERVICES <= running


def test_kafka_broker_accepts_connections() -> None:
    output = compose(
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-broker-api-versions.sh",
        "--bootstrap-server",
        "127.0.0.1:19092",
    )

    assert "id: 1" in output


def test_postgres_accepts_connections_and_has_pgvector() -> None:
    output = compose(
        "exec",
        "-T",
        "postgres",
        "sh",
        "-ec",
        'psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" '
        '--tuples-only --command "SELECT extversion FROM pg_extension '
        "WHERE extname = 'vector';\"",
    )

    assert output


def test_ranking_api_connects_to_postgres() -> None:
    port = os.getenv("API_PORT", "8000")
    body = get_json(f"http://127.0.0.1:{port}/health/ready")

    assert body["dependencies"] == {"postgres": "ok"}


def test_ingestion_service_connects_to_kafka() -> None:
    port = os.getenv("INGESTION_PORT", "8001")
    body = get_json(f"http://127.0.0.1:{port}/health/ready")

    assert body["dependencies"] == {"kafka": "ok"}


def test_normalization_worker_connects_to_kafka_and_postgres() -> None:
    output = compose(
        "exec",
        "-T",
        "normalization-worker",
        "python",
        "-c",
        "import json, urllib.request; "
        "response=urllib.request.urlopen('http://127.0.0.1:8080/health/ready', "
        "timeout=5); print(json.load(response)['dependencies'])",
    )

    assert "'kafka': 'ok'" in output
    assert "'postgres': 'ok'" in output

