import base64
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import psycopg
import pytest

from apps.ranking.consumer import (
    DatabaseFailureDisposition,
    INVALID_SCHEMA_FAILURE,
    _dlq_value,
    _send_to_dlq_and_commit,
    classify_database_error,
    database_retry_delay,
)
from packages.event_contracts import DeadLetterEventV1


def _record(value: bytes) -> SimpleNamespace:
    return SimpleNamespace(
        topic="grocery.deals.normalized.v1",
        partition=1,
        offset=42,
        key=b"source:store:deal",
        value=value,
        headers=[
            ("event_id", b"10000000-0000-0000-0000-000000000001"),
            ("correlation_id", b"20000000-0000-0000-0000-000000000001"),
            ("trace_id", b"trace-1"),
        ],
    )


def test_invalid_object_is_wrapped_in_valid_dlq_contract() -> None:
    encoded, _ = _dlq_value(_record(b'{"bad":"schema"}'))
    event = DeadLetterEventV1.model_validate_json(encoded)

    assert event.event_type == "grocery.deal.dead_lettered"
    assert event.payload.original_event == {"bad": "schema"}
    assert event.payload.raw_value_base64 is None
    assert event.payload.failed_stage == "validate"


def test_malformed_bytes_are_size_safe_in_dlq_event() -> None:
    encoded, _ = _dlq_value(_record(b"\xffnot-json"))
    payload = DeadLetterEventV1.model_validate_json(encoded).payload

    assert payload.original_event is None
    assert base64.b64decode(payload.raw_value_base64 or "") == b"\xffnot-json"


def test_oversized_json_object_is_bounded_and_schema_valid() -> None:
    value = json.dumps({"oversized": "x" * 20_000}).encode()

    encoded, _ = _dlq_value(_record(value), max_event_bytes=2_048)
    event = DeadLetterEventV1.model_validate_json(encoded)

    assert len(encoded) <= 2_048
    assert event.payload.original_event is None
    assert event.payload.raw_value_base64 is not None


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (psycopg.OperationalError("connection lost"), DatabaseFailureDisposition.RETRY),
        (psycopg.errors.SerializationFailure("retry transaction"), DatabaseFailureDisposition.RETRY),
        (psycopg.DataError("bad vector"), DatabaseFailureDisposition.DEAD_LETTER),
        (psycopg.IntegrityError("constraint"), DatabaseFailureDisposition.DEAD_LETTER),
        (psycopg.ProgrammingError("bad SQL"), DatabaseFailureDisposition.FATAL),
    ],
)
def test_database_failure_classification(
    error: psycopg.Error, expected: DatabaseFailureDisposition
) -> None:
    assert classify_database_error(error) is expected


def test_database_retry_delay_is_exponential_and_capped() -> None:
    assert [
        database_retry_delay(attempt, base=1, maximum=5) for attempt in range(1, 6)
    ] == [1, 2, 4, 5, 5]


async def test_dlq_transaction_commits_record_and_source_offset_atomically() -> None:
    producer = SimpleNamespace(
        begin_transaction=AsyncMock(),
        send_and_wait=AsyncMock(),
        send_offsets_to_transaction=AsyncMock(),
        commit_transaction=AsyncMock(),
        abort_transaction=AsyncMock(),
    )

    await _send_to_dlq_and_commit(
        producer,
        SimpleNamespace(),
        _record(b'{"bad":"schema"}'),
        "grocery.deals.dlq.v1",
        "ranking-projector-v1",
        failure=INVALID_SCHEMA_FAILURE,
        max_event_bytes=900_000,
    )

    producer.begin_transaction.assert_awaited_once()
    producer.send_and_wait.assert_awaited_once()
    producer.send_offsets_to_transaction.assert_awaited_once()
    producer.commit_transaction.assert_awaited_once()
    producer.abort_transaction.assert_not_awaited()


async def test_dlq_transaction_aborts_when_publish_fails() -> None:
    producer = SimpleNamespace(
        begin_transaction=AsyncMock(),
        send_and_wait=AsyncMock(side_effect=RuntimeError("broker unavailable")),
        send_offsets_to_transaction=AsyncMock(),
        commit_transaction=AsyncMock(),
        abort_transaction=AsyncMock(),
    )

    with pytest.raises(RuntimeError, match="broker unavailable"):
        await _send_to_dlq_and_commit(
            producer,
            SimpleNamespace(),
            _record(b'{"bad":"schema"}'),
            "grocery.deals.dlq.v1",
            "ranking-projector-v1",
            failure=INVALID_SCHEMA_FAILURE,
            max_event_bytes=900_000,
        )

    producer.abort_transaction.assert_awaited_once()
    producer.commit_transaction.assert_not_awaited()
