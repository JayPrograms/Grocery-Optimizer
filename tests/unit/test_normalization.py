from decimal import Decimal

from apps.normalization.pipeline import FixtureLlmProvider
from domain import EMBEDDING_DIMENSION, deterministic_embedding, normalize_product_text


def test_product_text_is_cleaned_deterministically() -> None:
    assert normalize_product_text("  Whole MILK 1 L — Special! ") == "whole milk"


def test_fixture_embedding_is_deterministic_and_fixed_dimension() -> None:
    first = deterministic_embedding("whole milk")
    second = deterministic_embedding("Whole Milk 1 L")

    assert first == second
    assert len(first) == EMBEDDING_DIMENSION
    assert abs(sum(value * value for value in first) - Decimal("1")) < Decimal("0.000001")


def test_structured_llm_fallback_creates_unknown_product() -> None:
    decision = FixtureLlmProvider().decide("dragon fruit")

    assert decision.action == "create"
    assert decision.canonical_name == "dragon fruit"
    assert decision.category == "produce"


def test_structured_llm_fallback_can_return_unresolved() -> None:
    decision = FixtureLlmProvider().decide("unresolvable mystery")

    assert decision.action == "unresolved"
    assert decision.canonical_name is None
