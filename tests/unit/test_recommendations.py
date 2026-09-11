from decimal import Decimal

import pytest

from domain import (
    Deal,
    GroceryItem,
    calculate_fuel_consumed,
    calculate_grocery_savings,
    calculate_incremental_fuel_cost,
    calculate_net_savings,
    calculate_round_trip_distance,
    haversine_distance_km,
    rank_stores,
)


def _deal(
    *,
    deal_id: str = "deal-1",
    store_id: str = "store-1",
    product_id: str = "product-1",
    name: str = "milk",
    deal_price: str = "3.00",
    regular_price: str | None = "5.00",
    package_quantity: str | None = "1",
    package_unit: str | None = "l",
) -> Deal:
    return Deal(
        deal_id=deal_id,
        store_location_id=store_id,
        canonical_product_id=product_id,
        canonical_name=name,
        deal_price=Decimal(deal_price),
        regular_price=None if regular_price is None else Decimal(regular_price),
        currency="CAD",
        package_quantity=(
            None if package_quantity is None else Decimal(package_quantity)
        ),
        package_unit=package_unit,
    )


def _rank(
    items: list[GroceryItem],
    deals: list[Deal],
    distances: dict[str, Decimal] | None = None,
    *,
    efficiency: str = "8",
    fuel_price: str = "1.50",
):
    return rank_stores(
        grocery_items=items,
        active_deals=deals,
        one_way_distances_km=distances or {"store-1": Decimal("5")},
        fuel_efficiency_l_per_100km=Decimal(efficiency),
        fuel_price_per_litre=Decimal(fuel_price),
        currency="CAD",
    )


def test_all_formulas_use_exact_decimal_arithmetic() -> None:
    units, grocery_savings = calculate_grocery_savings(
        requested_quantity=Decimal("2.1"),
        regular_price=Decimal("5.25"),
        deal_price=Decimal("3.00"),
        deal_package_quantity=Decimal("0.75"),
    )
    round_trip = calculate_round_trip_distance(Decimal("12.5"))
    fuel_consumed = calculate_fuel_consumed(round_trip, Decimal("8"))
    fuel_cost = calculate_incremental_fuel_cost(fuel_consumed, Decimal("1.50"))

    assert units == 3
    assert grocery_savings == Decimal("6.75")
    assert round_trip == Decimal("25.0")
    assert fuel_consumed == Decimal("2.0")
    assert fuel_cost == Decimal("3.000")
    assert calculate_net_savings(grocery_savings, fuel_cost) == Decimal("3.750")


def test_grocery_savings_never_goes_below_zero() -> None:
    units, savings = calculate_grocery_savings(
        requested_quantity=Decimal("2"),
        regular_price=Decimal("3"),
        deal_price=Decimal("4"),
        deal_package_quantity=Decimal("1"),
    )

    assert units == 2
    assert savings == 0


@pytest.mark.parametrize(
    ("efficiency", "fuel_price", "expected_cost"),
    [
        ("5", "1.50", "1.500"),
        ("10", "1.50", "3.000"),
        ("10", "2.00", "4.000"),
    ],
)
def test_fuel_cost_changes_with_efficiency_and_price(
    efficiency: str, fuel_price: str, expected_cost: str
) -> None:
    fuel = calculate_fuel_consumed(Decimal("20"), Decimal(efficiency))

    assert calculate_incremental_fuel_cost(fuel, Decimal(fuel_price)) == Decimal(
        expected_cost
    )


def test_multiple_grocery_items_are_summed_and_missing_items_are_reported() -> None:
    items = [
        GroceryItem("milk", "Milk", Decimal("2")),
        GroceryItem("bread", "bread", Decimal("1")),
        GroceryItem("eggs", "eggs", Decimal("1")),
    ]
    deals = [
        _deal(),
        _deal(
            deal_id="deal-2",
            product_id="product-2",
            name="bread",
            deal_price="3",
            regular_price="4",
            package_quantity=None,
            package_unit=None,
        ),
    ]

    result = _rank(items, deals)[0]

    assert result.grocery_savings == Decimal("5")
    assert [item.client_item_id for item in result.matched_items] == ["milk", "bread"]
    assert [(item.client_item_id, item.reason) for item in result.unmatched_items] == [
        ("eggs", "no_savings_eligible_deal")
    ]


def test_zero_distance_has_no_fuel_cost() -> None:
    result = _rank(
        [GroceryItem("milk", "milk")],
        [_deal()],
        {"store-1": Decimal("0")},
    )[0]

    assert result.incremental_round_trip_distance_km == 0
    assert result.fuel_consumed_litres == 0
    assert result.incremental_fuel_cost == 0
    assert result.net_savings == result.grocery_savings == Decimal("2.00")


def test_negative_net_savings_store_remains_eligible() -> None:
    result = _rank(
        [GroceryItem("milk", "milk")],
        [_deal(deal_price="4", regular_price="5")],
        {"store-1": Decimal("10")},
        efficiency="10",
        fuel_price="2",
    )[0]

    assert result.grocery_savings == Decimal("1")
    assert result.incremental_fuel_cost == Decimal("4")
    assert result.net_savings == Decimal("-3")


def test_store_with_no_useful_deals_is_excluded() -> None:
    deals = [
        _deal(regular_price=None),
        _deal(deal_id="deal-2", deal_price="5", regular_price="5"),
    ]

    assert _rank([GroceryItem("milk", "milk")], deals) == []


def test_incompatible_or_missing_products_do_not_contribute_savings() -> None:
    items = [
        GroceryItem("milk", "milk", Decimal("1"), "kg"),
        GroceryItem("eggs", "eggs"),
    ]

    assert _rank(items, [_deal()]) == []


def test_rank_order_uses_net_then_grocery_savings_distance_and_store_id() -> None:
    items = [GroceryItem("milk", "milk")]
    deals = [
        _deal(deal_id="deal-b", store_id="store-b"),
        _deal(deal_id="deal-a", store_id="store-a"),
    ]
    distances = {"store-a": Decimal("3"), "store-b": Decimal("5")}

    ranked = _rank(items, deals, distances, efficiency="10", fuel_price="1")

    assert [store.store_location_id for store in ranked] == ["store-a", "store-b"]


def test_best_deal_per_item_is_selected_deterministically() -> None:
    items = [GroceryItem("milk", "milk")]
    deals = [
        _deal(deal_id="deal-z", deal_price="3"),
        _deal(deal_id="deal-a", deal_price="3"),
        _deal(deal_id="deal-best", deal_price="2"),
    ]

    result = _rank(items, deals)[0]

    assert result.matched_items[0].deal_id == "deal-best"
    assert result.matched_items[0].item_savings == Decimal("3")


def test_haversine_distance_is_zero_for_same_point() -> None:
    assert haversine_distance_km(
        Decimal("43.6532"),
        Decimal("-79.3832"),
        Decimal("43.6532"),
        Decimal("-79.3832"),
    ) == 0
