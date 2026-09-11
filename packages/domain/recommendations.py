"""Pure, deterministic calculations for single-store recommendations.

All quantities, distances, fuel measurements, and money use ``Decimal``. Values
are kept at full input precision here; API presentation is the only rounding
boundary.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING
from math import asin, cos, radians, sin, sqrt


HUNDRED = Decimal("100")
ZERO = Decimal("0")
EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True, slots=True)
class GroceryItem:
    client_item_id: str
    name: str
    quantity: Decimal = Decimal("1")
    unit: str | None = None
    canonical_product_id: str | None = None


@dataclass(frozen=True, slots=True)
class Deal:
    deal_id: str
    store_location_id: str
    canonical_product_id: str
    canonical_name: str
    deal_price: Decimal
    regular_price: Decimal | None
    currency: str
    package_quantity: Decimal | None
    package_unit: str | None


@dataclass(frozen=True, slots=True)
class MatchedItem:
    client_item_id: str
    requested_name: str
    canonical_product_id: str
    canonical_name: str
    deal_id: str
    units_needed: int
    item_savings: Decimal


@dataclass(frozen=True, slots=True)
class UnmatchedItem:
    client_item_id: str
    requested_name: str
    reason: str


@dataclass(frozen=True, slots=True)
class StoreCalculation:
    store_location_id: str
    one_way_distance_km: Decimal
    incremental_round_trip_distance_km: Decimal
    fuel_consumed_litres: Decimal
    grocery_savings: Decimal
    incremental_fuel_cost: Decimal
    net_savings: Decimal
    matched_items: tuple[MatchedItem, ...]
    unmatched_items: tuple[UnmatchedItem, ...]


def _require_non_negative(value: Decimal, field: str) -> None:
    if not value.is_finite() or value < ZERO:
        raise ValueError(f"{field} must be a finite non-negative decimal")


def _require_positive(value: Decimal, field: str) -> None:
    if not value.is_finite() or value <= ZERO:
        raise ValueError(f"{field} must be a finite positive decimal")


def _normalized_text(value: str) -> str:
    return " ".join(value.casefold().split())


def _normalized_unit(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _normalized_text(value)
    aliases = {
        "liter": "l",
        "liters": "l",
        "litre": "l",
        "litres": "l",
        "kilogram": "kg",
        "kilograms": "kg",
        "gram": "g",
        "grams": "g",
    }
    return aliases.get(normalized, normalized)


def calculate_round_trip_distance(one_way_distance_km: Decimal) -> Decimal:
    """Return the incremental distance of a dedicated return trip."""
    _require_non_negative(one_way_distance_km, "one_way_distance_km")
    return one_way_distance_km * Decimal("2")


def haversine_distance_km(
    origin_latitude: Decimal,
    origin_longitude: Decimal,
    store_latitude: Decimal,
    store_longitude: Decimal,
) -> Decimal:
    """Return straight-line kilometres between two WGS84 coordinates."""
    for latitude in (origin_latitude, store_latitude):
        if not latitude.is_finite() or not Decimal("-90") <= latitude <= Decimal("90"):
            raise ValueError("latitude must be between -90 and 90")
    for longitude in (origin_longitude, store_longitude):
        if not longitude.is_finite() or not Decimal("-180") <= longitude <= Decimal("180"):
            raise ValueError("longitude must be between -180 and 180")

    origin_lat = radians(float(origin_latitude))
    store_lat = radians(float(store_latitude))
    latitude_delta = store_lat - origin_lat
    longitude_delta = radians(float(store_longitude - origin_longitude))
    haversine = sin(latitude_delta / 2) ** 2 + (
        cos(origin_lat) * cos(store_lat) * sin(longitude_delta / 2) ** 2
    )
    distance = 2 * EARTH_RADIUS_KM * asin(sqrt(min(1.0, haversine)))
    return Decimal(str(distance))


def calculate_one_way_distance(
    haversine_distance: Decimal, road_factor: Decimal
) -> Decimal:
    """Estimate one-way driving distance from straight-line distance."""
    _require_non_negative(haversine_distance, "haversine_distance_km")
    _require_positive(road_factor, "road_factor")
    return haversine_distance * road_factor


def calculate_fuel_consumed(
    round_trip_distance_km: Decimal,
    fuel_efficiency_l_per_100km: Decimal,
) -> Decimal:
    """Calculate litres consumed for a distance using L/100 km efficiency."""
    _require_non_negative(round_trip_distance_km, "round_trip_distance_km")
    _require_positive(fuel_efficiency_l_per_100km, "fuel_efficiency_l_per_100km")
    return round_trip_distance_km * fuel_efficiency_l_per_100km / HUNDRED


def calculate_incremental_fuel_cost(
    fuel_consumed_litres: Decimal,
    fuel_price_per_litre: Decimal,
) -> Decimal:
    """Calculate fuel cost in the deployment currency."""
    _require_non_negative(fuel_consumed_litres, "fuel_consumed_litres")
    _require_non_negative(fuel_price_per_litre, "fuel_price_per_litre")
    return fuel_consumed_litres * fuel_price_per_litre


def calculate_net_savings(
    grocery_savings: Decimal,
    incremental_fuel_cost: Decimal,
) -> Decimal:
    """Subtract incremental fuel cost from grocery savings."""
    _require_non_negative(grocery_savings, "grocery_savings")
    _require_non_negative(incremental_fuel_cost, "incremental_fuel_cost")
    return grocery_savings - incremental_fuel_cost


def calculate_grocery_savings(
    *,
    requested_quantity: Decimal,
    regular_price: Decimal,
    deal_price: Decimal,
    deal_package_quantity: Decimal,
) -> tuple[int, Decimal]:
    """Return package units required and non-negative savings for one item."""
    _require_positive(requested_quantity, "requested_quantity")
    _require_non_negative(regular_price, "regular_price")
    _require_non_negative(deal_price, "deal_price")
    _require_positive(deal_package_quantity, "deal_package_quantity")
    units_needed = int(
        (requested_quantity / deal_package_quantity).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    per_unit_savings = max(regular_price - deal_price, ZERO)
    return units_needed, per_unit_savings * units_needed


def _matches_product(item: GroceryItem, deal: Deal) -> bool:
    if item.canonical_product_id is not None:
        return item.canonical_product_id == deal.canonical_product_id
    return _normalized_text(item.name) == _normalized_text(deal.canonical_name)


def _savings_for_deal(item: GroceryItem, deal: Deal) -> tuple[int, Decimal] | None:
    if deal.regular_price is None or deal.regular_price <= deal.deal_price:
        return None

    requested_unit = _normalized_unit(item.unit)
    package_unit = _normalized_unit(deal.package_unit)
    if requested_unit is None:
        package_quantity = Decimal("1")
    else:
        if package_unit is None or requested_unit != package_unit:
            return None
        package_quantity = deal.package_quantity or Decimal("1")

    return calculate_grocery_savings(
        requested_quantity=item.quantity,
        regular_price=deal.regular_price,
        deal_price=deal.deal_price,
        deal_package_quantity=package_quantity,
    )


def _best_match(item: GroceryItem, deals: Sequence[Deal]) -> MatchedItem | None:
    candidates: list[MatchedItem] = []
    for deal in deals:
        if not _matches_product(item, deal):
            continue
        result = _savings_for_deal(item, deal)
        if result is None:
            continue
        units_needed, item_savings = result
        if item_savings <= ZERO:
            continue
        candidates.append(
            MatchedItem(
                client_item_id=item.client_item_id,
                requested_name=item.name,
                canonical_product_id=deal.canonical_product_id,
                canonical_name=deal.canonical_name,
                deal_id=deal.deal_id,
                units_needed=units_needed,
                item_savings=item_savings,
            )
        )
    if not candidates:
        return None
    return min(candidates, key=lambda match: (-match.item_savings, match.deal_id))


def rank_stores(
    *,
    grocery_items: Sequence[GroceryItem],
    active_deals: Sequence[Deal],
    one_way_distances_km: Mapping[str, Decimal],
    fuel_efficiency_l_per_100km: Decimal,
    fuel_price_per_litre: Decimal,
    currency: str,
) -> list[StoreCalculation]:
    """Calculate and deterministically rank eligible single stores.

    Stores without a positive, comparable deal for at least one requested item
    are excluded. Negative net savings remain eligible and visible.
    """
    _require_positive(fuel_efficiency_l_per_100km, "fuel_efficiency_l_per_100km")
    _require_non_negative(fuel_price_per_litre, "fuel_price_per_litre")
    if not grocery_items:
        raise ValueError("grocery_items must not be empty")
    for item in grocery_items:
        _require_positive(item.quantity, "grocery item quantity")
    for store_id, distance in one_way_distances_km.items():
        if not store_id:
            raise ValueError("store location IDs must not be empty")
        _require_non_negative(distance, "one-way store distance")

    deals_by_store: dict[str, list[Deal]] = defaultdict(list)
    for deal in active_deals:
        if deal.currency == currency and deal.store_location_id in one_way_distances_km:
            deals_by_store[deal.store_location_id].append(deal)

    calculations: list[StoreCalculation] = []
    for store_id in sorted(deals_by_store):
        store_deals = deals_by_store[store_id]
        matched: list[MatchedItem] = []
        unmatched: list[UnmatchedItem] = []
        for item in grocery_items:
            match = _best_match(item, store_deals)
            if match is None:
                unmatched.append(
                    UnmatchedItem(
                        client_item_id=item.client_item_id,
                        requested_name=item.name,
                        reason="no_savings_eligible_deal",
                    )
                )
            else:
                matched.append(match)

        if not matched:
            continue

        grocery_savings = sum(
            (match.item_savings for match in matched), start=ZERO
        )
        one_way_distance = one_way_distances_km[store_id]
        round_trip_distance = calculate_round_trip_distance(one_way_distance)
        fuel_consumed = calculate_fuel_consumed(
            round_trip_distance, fuel_efficiency_l_per_100km
        )
        fuel_cost = calculate_incremental_fuel_cost(
            fuel_consumed, fuel_price_per_litre
        )
        calculations.append(
            StoreCalculation(
                store_location_id=store_id,
                one_way_distance_km=one_way_distance,
                incremental_round_trip_distance_km=round_trip_distance,
                fuel_consumed_litres=fuel_consumed,
                grocery_savings=grocery_savings,
                incremental_fuel_cost=fuel_cost,
                net_savings=calculate_net_savings(grocery_savings, fuel_cost),
                matched_items=tuple(matched),
                unmatched_items=tuple(unmatched),
            )
        )

    return sorted(
        calculations,
        key=lambda store: (
            -store.net_savings,
            -store.grocery_savings,
            store.one_way_distance_km,
            store.store_location_id,
        ),
    )
