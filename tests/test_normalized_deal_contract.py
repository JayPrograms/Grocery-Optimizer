import json

import pytest
from pydantic import ValidationError

from packages.event_contracts import NormalizedDealEventV1
from tests.conftest import normalized_event_dict


def test_normalized_event_parses_documented_schema() -> None:
    event = NormalizedDealEventV1.model_validate_json(json.dumps(normalized_event_dict()))

    assert event.payload.source_deal_id == "deal-1"
    assert str(event.payload.offer.deal_price) == "3.49"
    assert len(event.payload.product.embedding) == 3


def test_normalized_event_rejects_unknown_fields() -> None:
    value = normalized_event_dict()
    value["unexpected"] = True

    with pytest.raises(ValidationError):
        NormalizedDealEventV1.model_validate_json(json.dumps(value))

def test_llm_method_requires_model_and_prompt_metadata() -> None:
    value = normalized_event_dict()
    value["payload"]["normalization"]["method"] = "llm"

    with pytest.raises(ValidationError):
        NormalizedDealEventV1.model_validate_json(json.dumps(value))
