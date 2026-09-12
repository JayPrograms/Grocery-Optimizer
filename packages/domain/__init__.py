"""Stable, infrastructure-free domain logic for the grocery savings MVP."""

from .embeddings import (
    EMBEDDING_DIMENSION,
    EMBEDDING_MODEL,
    EMBEDDING_VERSION,
    deterministic_embedding,
    normalize_product_text,
    vector_literal,
)
from .recommendations import (
    Deal,
    GroceryItem,
    MatchedItem,
    StoreCalculation,
    UnmatchedItem,
    calculate_fuel_consumed,
    calculate_grocery_savings,
    calculate_incremental_fuel_cost,
    calculate_net_savings,
    calculate_one_way_distance,
    calculate_round_trip_distance,
    haversine_distance_km,
    rank_stores,
)

__all__ = [
    "EMBEDDING_DIMENSION",
    "EMBEDDING_MODEL",
    "EMBEDDING_VERSION",
    "Deal",
    "GroceryItem",
    "MatchedItem",
    "StoreCalculation",
    "UnmatchedItem",
    "calculate_fuel_consumed",
    "calculate_grocery_savings",
    "calculate_incremental_fuel_cost",
    "calculate_net_savings",
    "calculate_one_way_distance",
    "calculate_round_trip_distance",
    "deterministic_embedding",
    "haversine_distance_km",
    "normalize_product_text",
    "rank_stores",
    "vector_literal",
]
