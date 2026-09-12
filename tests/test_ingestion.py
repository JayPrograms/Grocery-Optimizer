from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import HTTPException
import pytest

from apps.ingestion.main import (
    DealBatchRequest,
    _authorize,
    ingest_batch,
)
from event_contracts import RawDealEventV1


FIXTURE = Path(__file__).parents[1] / "fixtures" / "flyer.json"


def _batch() -> DealBatchRequest:
    return DealBatchRequest.model_validate_json(FIXTURE.read_text(encoding="utf-8"))


def test_ingestion_authentication_rejects_missing_and_wrong_tokens(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_TOKEN", "expected")

    for supplied in (None, "Bearer wrong", "expected"):
        with pytest.raises(HTTPException) as raised:
            _authorize(supplied)
        assert raised.value.status_code == 401

    _authorize("Bearer expected")


async def test_batch_is_published_in_one_transaction(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_TOKEN", "token")
    producer = SimpleNamespace(
        begin_transaction=AsyncMock(),
        send_and_wait=AsyncMock(),
        commit_transaction=AsyncMock(),
        abort_transaction=AsyncMock(),
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(producer=producer)))

    response = await ingest_batch(_batch(), request, "Bearer token")

    assert response.accepted == 3
    producer.begin_transaction.assert_awaited_once()
    assert producer.send_and_wait.await_count == 3
    producer.commit_transaction.assert_awaited_once()
    producer.abort_transaction.assert_not_awaited()
    for call in producer.send_and_wait.await_args_list:
        RawDealEventV1.model_validate_json(call.kwargs["value"])


async def test_invalid_batch_publishes_nothing(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_TOKEN", "token")
    batch = _batch()
    invalid = batch.model_dump(mode="json")
    invalid["deals"][1]["offer"]["valid_to"] = invalid["deals"][1]["offer"]["valid_from"]
    producer = SimpleNamespace(begin_transaction=AsyncMock())
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(producer=producer)))

    with pytest.raises(HTTPException) as raised:
        await ingest_batch(DealBatchRequest.model_validate(invalid), request, "Bearer token")

    assert raised.value.status_code == 422
    producer.begin_transaction.assert_not_awaited()


async def test_publish_failure_aborts_transaction(monkeypatch) -> None:
    monkeypatch.setenv("INGESTION_TOKEN", "token")
    producer = SimpleNamespace(
        begin_transaction=AsyncMock(),
        send_and_wait=AsyncMock(side_effect=RuntimeError("broker failure")),
        commit_transaction=AsyncMock(),
        abort_transaction=AsyncMock(),
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(producer=producer)))

    with pytest.raises(HTTPException) as raised:
        await ingest_batch(_batch(), request, "Bearer token")

    assert raised.value.status_code == 503
    producer.abort_transaction.assert_awaited_once()
    producer.commit_transaction.assert_not_awaited()
