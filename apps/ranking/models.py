from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class StoreView(ApiModel):
    store_location_id: str
    chain_name: str
    store_name: str
    address_summary: str
    latitude: Decimal
    longitude: Decimal


class ProductView(ApiModel):
    canonical_product_id: UUID
    canonical_name: str
    brand: str | None
    package_quantity: Decimal | None
    package_unit: str | None
    category: str | None


class OfferView(ApiModel):
    deal_price: Decimal
    regular_price: Decimal | None
    currency: str
    package_quantity: Decimal | None
    package_unit: str | None
    valid_from: datetime
    valid_to: datetime


class NormalizationView(ApiModel):
    method: Literal["alias", "vector", "llm"]
    confidence: Decimal
    embedding_model: str
    embedding_version: str
    llm_model: str | None
    prompt_version: str | None
    catalog_version: int


class ActiveDeal(ApiModel):
    id: UUID
    source: str
    source_deal_id: str
    source_updated_at: datetime
    store: StoreView
    product: ProductView
    offer: OfferView
    normalization: NormalizationView


class ActiveDealPage(ApiModel):
    deals: list[ActiveDeal]
    limit: int
    offset: int


PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]


class Coordinate(ApiModel):
    latitude: Annotated[Decimal, Field(ge=-90, le=90, allow_inf_nan=False)]
    longitude: Annotated[Decimal, Field(ge=-180, le=180, allow_inf_nan=False)]


class GroceryListItemInput(ApiModel):
    client_item_id: Annotated[str, Field(min_length=1, max_length=128)]
    name: Annotated[str, Field(min_length=1, max_length=512)]
    quantity: PositiveDecimal = Decimal("1")
    unit: Annotated[str, Field(min_length=1, max_length=32)] | None = None


class OptimizationRequest(ApiModel):
    origin: Coordinate
    fuel_efficiency_l_per_100km: Annotated[
        Decimal, Field(gt=0, le=50, allow_inf_nan=False)
    ]
    radius_km: Annotated[Decimal, Field(gt=0, le=50, allow_inf_nan=False)] = Decimal(
        "15"
    )
    items: Annotated[list[GroceryListItemInput], Field(min_length=1, max_length=50)]

    @model_validator(mode="after")
    def require_unique_client_item_ids(self) -> "OptimizationRequest":
        item_ids = [item.client_item_id for item in self.items]
        if len(item_ids) != len(set(item_ids)):
            raise ValueError("client_item_id values must be unique")
        return self


class MatchedItemView(ApiModel):
    client_item_id: str
    requested_name: str
    canonical_product_id: UUID
    canonical_name: str
    deal_id: UUID
    units_needed: int
    item_savings: Decimal


class UnmatchedItemView(ApiModel):
    client_item_id: str
    requested_name: str
    reason: Literal["no_savings_eligible_deal"]


class StoreRecommendationView(ApiModel):
    rank: Literal[1]
    store: StoreView
    one_way_distance_km: Decimal
    incremental_round_trip_distance_km: Decimal
    fuel_consumed_litres: Decimal
    matched_items: list[MatchedItemView]
    unmatched_items: list[UnmatchedItemView]
    grocery_savings: Decimal
    incremental_fuel_cost: Decimal
    net_savings: Decimal


class OptimizationAssumptions(ApiModel):
    currency: str
    fuel_price_per_litre: Decimal
    road_factor: Decimal
    distance_method: Literal["haversine_with_road_factor"]


class OptimizationResponse(ApiModel):
    calculated_at: datetime
    assumptions: OptimizationAssumptions
    rankings: Annotated[list[StoreRecommendationView], Field(max_length=1)]
    unmatched_items: list[UnmatchedItemView]
