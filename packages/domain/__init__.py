"""Stable, infrastructure-free domain logic for the grocery savings MVP."""

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
    "haversine_distance_km",
    "rank_stores",
]
