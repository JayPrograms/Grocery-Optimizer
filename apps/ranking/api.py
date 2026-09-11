from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status

from apps.ranking.config import Settings
from apps.ranking.models import (
    ActiveDeal,
    ActiveDealPage,
    MatchedItemView,
    OptimizationAssumptions,
    OptimizationRequest,
    OptimizationResponse,
    StoreRecommendationView,
    UnmatchedItemView,
)
from apps.ranking.repository import DealRepository, PostgresDealRepository
from domain import (
    Deal,
    GroceryItem,
    calculate_one_way_distance,
    haversine_distance_km,
    rank_stores,
)


ACTIVE_DEAL_PAGE_SIZE = 100
MONEY_QUANTUM = Decimal("0.01")
MEASUREMENT_QUANTUM = Decimal("0.001")


def _repository(request: Request) -> DealRepository:
    return request.app.state.deal_repository


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(MONEY_QUANTUM, rounding=ROUND_HALF_UP)


def _round_measurement(value: Decimal) -> Decimal:
    return value.quantize(MEASUREMENT_QUANTUM, rounding=ROUND_HALF_UP)


async def _all_active_deals(
    repository: DealRepository, *, now: datetime
) -> Sequence[ActiveDeal]:
    deals: list[ActiveDeal] = []
    while True:
        page = await repository.list_active(
            now=now,
            store_location_id=None,
            canonical_product_id=None,
            limit=ACTIVE_DEAL_PAGE_SIZE,
            offset=len(deals),
        )
        deals.extend(page)
        if len(page) < ACTIVE_DEAL_PAGE_SIZE:
            break
    return deals


def _as_domain_deal(deal: ActiveDeal) -> Deal:
    return Deal(
        deal_id=str(deal.id),
        store_location_id=deal.store.store_location_id,
        canonical_product_id=str(deal.product.canonical_product_id),
        canonical_name=deal.product.canonical_name,
        deal_price=deal.offer.deal_price,
        regular_price=deal.offer.regular_price,
        currency=deal.offer.currency,
        package_quantity=deal.offer.package_quantity,
        package_unit=deal.offer.package_unit,
    )


def register_deal_routes(app: FastAPI) -> None:
    @app.get("/v1/deals", response_model=ActiveDealPage)
    async def list_active_deals(
        store_location_id: Annotated[str | None, Query(min_length=1, max_length=255)] = None,
        canonical_product_id: UUID | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
        deal_repository: DealRepository = Depends(_repository),
    ) -> ActiveDealPage:
        """Return only deals active at the server's current UTC time."""
        deals = await deal_repository.list_active(
            now=datetime.now(timezone.utc),
            store_location_id=store_location_id,
            canonical_product_id=canonical_product_id,
            limit=limit,
            offset=offset,
        )
        return ActiveDealPage(deals=list(deals), limit=limit, offset=offset)

    @app.get("/v1/deals/{deal_id}", response_model=ActiveDeal)
    async def get_active_deal(
        deal_id: UUID,
        deal_repository: DealRepository = Depends(_repository),
    ) -> ActiveDeal:
        """Return one active deal; expired and unknown deals are both hidden."""
        deal = await deal_repository.get_active(deal_id, now=datetime.now(timezone.utc))
        if deal is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Deal not found")
        return deal


def register_optimization_route(app: FastAPI) -> None:
    @app.post("/v1/optimizations", response_model=OptimizationResponse)
    async def optimize(
        optimization: OptimizationRequest,
        request: Request,
        deal_repository: DealRepository = Depends(_repository),
    ) -> OptimizationResponse:
        """Return at most one store: the deterministic best single-store choice."""
        calculated_at = request.app.state.clock()
        settings: Settings = request.app.state.settings
        active_deals = await _all_active_deals(deal_repository, now=calculated_at)

        stores = {deal.store.store_location_id: deal.store for deal in active_deals}
        one_way_distances: dict[str, Decimal] = {}
        for store_id, store in stores.items():
            straight_line_distance = haversine_distance_km(
                optimization.origin.latitude,
                optimization.origin.longitude,
                store.latitude,
                store.longitude,
            )
            if straight_line_distance <= optimization.radius_km:
                one_way_distances[store_id] = calculate_one_way_distance(
                    straight_line_distance, settings.road_factor
                )

        grocery_items = [
            GroceryItem(
                client_item_id=item.client_item_id,
                name=item.name,
                quantity=item.quantity,
                unit=item.unit,
            )
            for item in optimization.items
        ]
        calculations = rank_stores(
            grocery_items=grocery_items,
            active_deals=[_as_domain_deal(deal) for deal in active_deals],
            one_way_distances_km=one_way_distances,
            fuel_efficiency_l_per_100km=optimization.fuel_efficiency_l_per_100km,
            fuel_price_per_litre=settings.fuel_price_per_litre,
            currency=settings.currency,
        )

        rankings: list[StoreRecommendationView] = []
        if calculations:
            best = calculations[0]
            rankings.append(
                StoreRecommendationView(
                    rank=1,
                    store=stores[best.store_location_id],
                    one_way_distance_km=_round_measurement(best.one_way_distance_km),
                    incremental_round_trip_distance_km=_round_measurement(
                        best.incremental_round_trip_distance_km
                    ),
                    fuel_consumed_litres=_round_measurement(best.fuel_consumed_litres),
                    matched_items=[
                        MatchedItemView(
                            client_item_id=item.client_item_id,
                            requested_name=item.requested_name,
                            canonical_product_id=UUID(item.canonical_product_id),
                            canonical_name=item.canonical_name,
                            deal_id=UUID(item.deal_id),
                            units_needed=item.units_needed,
                            item_savings=_round_money(item.item_savings),
                        )
                        for item in best.matched_items
                    ],
                    unmatched_items=[
                        UnmatchedItemView(
                            client_item_id=item.client_item_id,
                            requested_name=item.requested_name,
                            reason="no_savings_eligible_deal",
                        )
                        for item in best.unmatched_items
                    ],
                    grocery_savings=_round_money(best.grocery_savings),
                    incremental_fuel_cost=_round_money(best.incremental_fuel_cost),
                    net_savings=_round_money(best.net_savings),
                )
            )

        unmatched_items = (
            rankings[0].unmatched_items
            if rankings
            else [
                UnmatchedItemView(
                    client_item_id=item.client_item_id,
                    requested_name=item.name,
                    reason="no_savings_eligible_deal",
                )
                for item in optimization.items
            ]
        )
        return OptimizationResponse(
            calculated_at=calculated_at,
            assumptions=OptimizationAssumptions(
                currency=settings.currency,
                fuel_price_per_litre=_round_money(settings.fuel_price_per_litre),
                road_factor=settings.road_factor,
                distance_method="haversine_with_road_factor",
            ),
            rankings=rankings,
            unmatched_items=unmatched_items,
        )


def create_app(
    repository: DealRepository | None = None,
    *,
    settings: Settings | None = None,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    app = FastAPI(title="Grocery Savings Ranking API", version="0.1.0")
    settings = settings or Settings.from_env()
    settings.validate_recommendations()
    app.state.settings = settings
    app.state.clock = clock or (lambda: datetime.now(timezone.utc))
    app.state.deal_repository = repository or PostgresDealRepository(settings.database_url)
    register_deal_routes(app)
    register_optimization_route(app)
    return app
