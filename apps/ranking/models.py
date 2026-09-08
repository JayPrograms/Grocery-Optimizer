from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from packages.event_contracts import NormalizationMethod


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
    method: NormalizationMethod
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
