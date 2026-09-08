import base64
import json
from types import SimpleNamespace

from apps.ranking.consumer import _dlq_value


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


def test_invalid_object_is_wrapped_in_diagnosable_dlq_event() -> None:
    encoded, _ = _dlq_value(_record(b'{"bad":"schema"}'))
    event = json.loads(encoded)

    assert event["event_type"] == "grocery.deal.dead_lettered"
    assert event["payload"]["original_event"] == {"bad": "schema"}
    assert event["payload"]["raw_value_base64"] is None
    assert event["payload"]["failed_stage"] == "validate"


def test_malformed_bytes_are_size_safe_in_dlq_event() -> None:
    encoded, _ = _dlq_value(_record(b"\xffnot-json"))
    payload = json.loads(encoded)["payload"]

    assert payload["original_event"] is None
    assert base64.b64decode(payload["raw_value_base64"]) == b"\xffnot-json"
