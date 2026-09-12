from __future__ import annotations

import base64
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from event_contracts import (
    DeadLetterEvent,
    NormalizationRetryEvent,
    NormalizedDealEvent,
    RawDealEvent,
)
from event_contracts.models import MAX_RAW_DLQ_BYTES


FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def validate_json(model: type[Any], data: dict[str, Any], **kwargs: Any) -> Any:
    return model.model_validate_json(json.dumps(data), **kwargs)


@pytest.mark.parametrize(
    ("model", "fixture_name", "context"),
    [
        (RawDealEvent, "raw_deal_event_v1.json", None),
        (NormalizationRetryEvent, "normalization_retry_event_v1.json", None),
        (
            NormalizedDealEvent,
            "normalized_deal_event_v1.json",
            {"embedding_dimension": 3},
        ),
        (DeadLetterEvent, "dead_letter_event_v1.json", None),
    ],
)
def test_golden_event_round_trip(
    model: type[Any], fixture_name: str, context: dict[str, Any] | None
) -> None:
    event = model.model_validate_json(
        (FIXTURES / fixture_name).read_text(encoding="utf-8"), context=context
    )

    reparsed = model.model_validate_json(event.model_dump_json(), context=context)

    assert reparsed == event


def test_raw_event_contains_required_business_data() -> None:
    event = validate_json(
        RawDealEvent, load_fixture("raw_deal_event_v1.json")
    )

    assert str(event.event_id) == "11111111-1111-4111-8111-111111111111"
    assert event.payload.store.retailer == "Fresh Market"
    assert event.payload.product.raw_name == "2% Milk 1 L"
    assert event.payload.offer.deal_price == Decimal("3.49")
    assert event.payload.offer.package_unit == "L"
    assert event.payload.offer.valid_from < event.payload.offer.valid_to
    assert event.payload.source_timestamp == event.payload.source_updated_at


def test_normalized_event_contains_match_audit_data() -> None:
    event = validate_json(
        NormalizedDealEvent,
        load_fixture("normalized_deal_event_v1.json"),
        context={"embedding_dimension": 3},
    )

    assert event.payload.product.canonical_name == "2% milk"
    assert event.payload.normalization.method == "llm"
    assert event.payload.normalization.confidence == Decimal("0.94")


def test_decimal_values_serialize_as_json_strings() -> None:
    event = validate_json(
        RawDealEvent, load_fixture("raw_deal_event_v1.json")
    )

    encoded = json.loads(event.model_dump_json())

    assert encoded["payload"]["offer"]["deal_price"] == "3.49"
    assert encoded["payload"]["store"]["latitude"] == "43.6532"


@pytest.mark.parametrize(
    ("fixture_name", "model"),
    [
        ("raw_deal_event_v1.json", RawDealEvent),
        ("normalization_retry_event_v1.json", NormalizationRetryEvent),
        ("normalized_deal_event_v1.json", NormalizedDealEvent),
        ("dead_letter_event_v1.json", DeadLetterEvent),
    ],
)
def test_events_reject_unknown_fields(fixture_name: str, model: type[Any]) -> None:
    data = load_fixture(fixture_name)
    data["unexpected"] = "not allowed"

    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        validate_json(model, data)


@pytest.mark.parametrize(
    "timestamp",
    ["2026-09-02T12:00:00", "2026-09-02T08:00:00-04:00"],
)
def test_events_require_utc_timestamps(timestamp: str) -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data["occurred_at"] = timestamp

    with pytest.raises(ValidationError, match="timezone-aware UTC"):
        validate_json(RawDealEvent, data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_id", "not-a-uuid"),
        ("event_type", "grocery.deal.normalized"),
        ("schema_version", 2),
    ],
)
def test_raw_event_rejects_invalid_envelope_fields(field: str, value: Any) -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data[field] = value

    with pytest.raises(ValidationError):
        validate_json(RawDealEvent, data)


@pytest.mark.parametrize("price", ["-0.01", "NaN", "Infinity"])
def test_raw_event_rejects_invalid_prices(price: str) -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data["payload"]["offer"]["deal_price"] = price

    with pytest.raises(ValidationError):
        validate_json(RawDealEvent, data)


@pytest.mark.parametrize(
    ("field", "value"),
    [("latitude", "90.01"), ("longitude", "-180.01")],
)
def test_raw_event_rejects_invalid_coordinates(field: str, value: str) -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data["payload"]["store"][field] = value

    with pytest.raises(ValidationError):
        validate_json(RawDealEvent, data)


@pytest.mark.parametrize("currency", ["cad", "CA", "CADX"])
def test_raw_event_rejects_invalid_currency(currency: str) -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data["payload"]["offer"]["currency"] = currency

    with pytest.raises(ValidationError):
        validate_json(RawDealEvent, data)


def test_raw_event_rejects_empty_source_deal_id() -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data["payload"]["source_deal_id"] = "   "

    with pytest.raises(ValidationError):
        validate_json(RawDealEvent, data)


def test_offer_requires_increasing_validity_period() -> None:
    data = load_fixture("raw_deal_event_v1.json")
    data["payload"]["offer"]["valid_to"] = data["payload"]["offer"][
        "valid_from"
    ]

    with pytest.raises(ValidationError, match="valid_to must be later"):
        validate_json(RawDealEvent, data)


@pytest.mark.parametrize(
    ("quantity_field", "unit_field"),
    [("raw_size", "raw_unit"), ("package_quantity", "package_unit")],
)
def test_quantity_and_unit_must_be_present_together(
    quantity_field: str, unit_field: str
) -> None:
    data = load_fixture("raw_deal_event_v1.json")
    container = (
        data["payload"]["product"]
        if quantity_field == "raw_size"
        else data["payload"]["offer"]
    )
    container[quantity_field] = None
    assert container[unit_field] is not None

    with pytest.raises(ValidationError, match="must either both be set"):
        validate_json(RawDealEvent, data)


@pytest.mark.parametrize("confidence", ["-0.001", "1.001", "NaN"])
def test_normalized_event_rejects_invalid_confidence(confidence: str) -> None:
    data = load_fixture("normalized_deal_event_v1.json")
    data["payload"]["normalization"]["confidence"] = confidence

    with pytest.raises(ValidationError):
        validate_json(NormalizedDealEvent, data)


def test_normalized_event_enforces_configured_embedding_dimension() -> None:
    data = load_fixture("normalized_deal_event_v1.json")

    with pytest.raises(ValidationError, match="embedding dimension"):
        validate_json(
            NormalizedDealEvent, data, context={"embedding_dimension": 4}
        )


@pytest.mark.parametrize("embedding", [[], ["NaN", "0", "1"]])
def test_normalized_event_rejects_invalid_embedding(
    embedding: list[str],
) -> None:
    data = load_fixture("normalized_deal_event_v1.json")
    data["payload"]["product"]["embedding"] = embedding

    with pytest.raises(ValidationError):
        validate_json(NormalizedDealEvent, data)


def test_llm_method_requires_complete_llm_metadata() -> None:
    data = load_fixture("normalized_deal_event_v1.json")
    data["payload"]["normalization"]["llm_model"] = None

    with pytest.raises(ValidationError, match="requires llm_model"):
        validate_json(NormalizedDealEvent, data)


@pytest.mark.parametrize("method", ["alias", "vector"])
def test_non_llm_methods_reject_llm_metadata(method: str) -> None:
    data = load_fixture("normalized_deal_event_v1.json")
    data["payload"]["normalization"]["method"] = method

    with pytest.raises(ValidationError, match="must not include LLM metadata"):
        validate_json(NormalizedDealEvent, data)


@pytest.mark.parametrize(
    ("original_event", "raw_value"),
    [(None, None), ({"event_id": "original"}, "e30=")],
)
def test_dead_letter_requires_exactly_one_original_representation(
    original_event: dict[str, Any] | None, raw_value: str | None
) -> None:
    data = load_fixture("dead_letter_event_v1.json")
    data["payload"]["original_event"] = original_event
    data["payload"]["raw_value_base64"] = raw_value

    with pytest.raises(ValidationError, match="exactly one"):
        validate_json(DeadLetterEvent, data)


def test_dead_letter_accepts_raw_bytes_instead_of_parsed_event() -> None:
    data = load_fixture("dead_letter_event_v1.json")
    data["payload"]["original_event"] = None
    data["payload"]["raw_value_base64"] = base64.b64encode(b"not-json").decode()

    event = validate_json(DeadLetterEvent, data)

    assert event.payload.raw_value_base64 == "bm90LWpzb24="


@pytest.mark.parametrize("raw_value", ["not base64!", "abcde"])
def test_dead_letter_rejects_invalid_base64(raw_value: str) -> None:
    data = load_fixture("dead_letter_event_v1.json")
    data["payload"]["original_event"] = None
    data["payload"]["raw_value_base64"] = raw_value

    with pytest.raises(ValidationError, match="valid base64"):
        validate_json(DeadLetterEvent, data)


def test_dead_letter_rejects_oversized_raw_bytes() -> None:
    data = load_fixture("dead_letter_event_v1.json")
    data["payload"]["original_event"] = None
    data["payload"]["raw_value_base64"] = base64.b64encode(
        b"x" * (MAX_RAW_DLQ_BYTES + 1)
    ).decode()

    with pytest.raises(ValidationError, match="decode to at most"):
        validate_json(DeadLetterEvent, data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("original_partition", -1),
        ("original_offset", -1),
        ("attempts", 0),
        ("failed_stage", "publish"),
        ("classification", "temporary"),
        ("error_message", "unsafe\nstack trace"),
    ],
)
def test_dead_letter_rejects_invalid_failure_metadata(
    field: str, value: Any
) -> None:
    data = deepcopy(load_fixture("dead_letter_event_v1.json"))
    data["payload"][field] = value

    with pytest.raises(ValidationError):
        validate_json(DeadLetterEvent, data)

