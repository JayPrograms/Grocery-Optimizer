export type GroceryItemRequest = {
  client_item_id: string;
  name: string;
  quantity: string;
  unit: string | null;
};

export type OptimizationRequest = {
  origin: { latitude: string; longitude: string };
  fuel_efficiency_l_per_100km: string;
  fuel_price_per_litre: string;
  radius_km: string;
  items: GroceryItemRequest[];
};

export type UnmatchedItem = {
  client_item_id: string;
  requested_name: string;
  reason: "no_savings_eligible_deal";
};

export type MatchedItem = {
  client_item_id: string;
  requested_name: string;
  canonical_product_id: string;
  canonical_name: string;
  deal_id: string;
  units_needed: number;
  item_savings: string;
};

export type StoreRecommendation = {
  rank: 1;
  store: {
    store_location_id: string;
    chain_name: string;
    store_name: string;
    address_summary: string;
    latitude: string;
    longitude: string;
  };
  one_way_distance_km: string;
  incremental_round_trip_distance_km: string;
  fuel_consumed_litres: string;
  matched_items: MatchedItem[];
  unmatched_items: UnmatchedItem[];
  grocery_savings: string;
  incremental_fuel_cost: string;
  net_savings: string;
};

export type OptimizationResponse = {
  calculated_at: string;
  assumptions: {
    currency: string;
    fuel_price_per_litre: string;
    road_factor: string;
    distance_method: "haversine_with_road_factor";
  };
  rankings: StoreRecommendation[];
  unmatched_items: UnmatchedItem[];
};
