# Grocery Savings Optimizer — MVP Specification

## 1. Purpose

The product helps a shopper decide which single nearby grocery store offers the best value for a submitted grocery list after accounting for the incremental fuel cost of a round trip.

The MVP proves four things:

1. flyer deals can enter a reliable event-driven pipeline;
2. differently worded grocery items can be normalized into a shared product catalog;
3. a grocery list can be matched against active nearby deals; and
4. stores can be ranked by estimated net savings.

## 2. MVP user journey

1. An operator or source adapter submits a batch of flyer deals.
2. The ingestion service validates the source payload and emits one raw-deal event per deal.
3. The normalization worker maps each raw product description to a canonical product, using vector similarity first and an LLM only for ambiguous cases.
4. Normalized active deals become available to the ranking service through a Kafka-fed local projection.
5. A shopper submits:
   - their origin coordinates;
   - a grocery list with desired quantities;
   - vehicle fuel efficiency in litres per 100 kilometres; and
   - an optional search radius.
6. The system matches list items to normalized products, calculates the estimated savings at each nearby store, subtracts the incremental round-trip fuel cost, and returns ranked stores.

No user account is required for the MVP.

## 3. Definitions and calculation rules

### 3.1 Canonical product

A normalized grocery concept used for matching, such as `whole milk`, with optional attributes such as brand, package size, and unit. Products that are not safely interchangeable remain separate canonical products.

### 3.2 Active deal

A normalized offer whose validity window contains the request time and which applies to a specific store location. Source adapters must expand a chain-wide flyer into store-location deals before or during ingestion.

### 3.3 Nearby store

A store location whose straight-line distance from the submitted origin is within the requested radius. The default radius is 15 km and the maximum accepted radius is 50 km.

The MVP uses Haversine distance. It does not call a mapping or routing provider. Estimated driving distance is:

```text
one_way_distance_km = haversine_distance_km * configured_road_factor
round_trip_distance_km = one_way_distance_km * 2
```

The default road factor is `1.2` and is deployment configuration.

### 3.4 Deal savings

For a matched list item at a store:

```text
units_needed = ceil(requested_quantity / deal_package_quantity)
item_savings = max(regular_price - deal_price, 0) * units_needed
```

Only comparable quantities and units may be matched. If quantity conversion is unavailable or unsafe, the item is reported as unmatched and contributes zero savings.

Store gross savings is the sum of item savings. Deals with missing regular price cannot establish savings and therefore do not contribute to ranking, though they may remain stored for diagnostics.

### 3.5 Incremental fuel cost and net savings

The deployment has one configured fuel price per litre, returned in the response so the estimate is explainable.

```text
fuel_used_litres = round_trip_distance_km * fuel_efficiency_l_per_100km / 100
incremental_fuel_cost = fuel_used_litres * configured_fuel_price_per_litre
net_savings = gross_savings - incremental_fuel_cost
```

All money is calculated with decimal arithmetic in one configured currency. Monetary values returned by the API are rounded to two decimal places. Stores are sorted by net savings descending, then gross savings descending, then distance ascending, then stable store ID.

The estimate compares a dedicated round trip to each single store. It is not a basket-price comparison and does not model the price of unmatched items.

## 4. Functional requirements

### FR-1: Ingest flyer deals

- Accept a validated batch of deals through an internal HTTP endpoint.
- Require a stable `source_deal_id` scoped to `source` and store location.
- Require store location, product text, deal price, regular price when available, currency, package information when available, and validity dates.
- Return `202 Accepted` once valid events have been published to Kafka; normalization is asynchronous.
- Reject malformed batches without publishing a partial batch.

### FR-2: Publish raw deals

- Publish one versioned Pydantic event per deal to the raw-deal Kafka topic.
- Partition by the stable source/store/deal identity so updates for one deal remain ordered.
- Propagate correlation and trace identifiers.
- Delivery may be at least once; downstream behavior must be correct under duplicates.

### FR-3: Normalize products

- Normalize source text and units deterministically before AI inference.
- Generate an embedding and search the canonical product catalog with pgvector.
- Accept a sufficiently strong, unambiguous vector match without an LLM call.
- Route ambiguous results to an LLM constrained to structured output.
- Permit the LLM to select only from retrieved candidates or explicitly recommend creation of a new canonical product.
- Validate all LLM output with Pydantic and deterministic compatibility rules.
- Store normalization method, model identifiers, confidence, and catalog version for auditability.
- Cache aliases so repeated source descriptions avoid repeated LLM calls.

### FR-4: Persist normalized deals

- Upsert a deal by `(source, store_location_id, source_deal_id)`.
- Preserve the latest source version and active validity window.
- Atomically persist the normalized deal, the consumed-event idempotency record, and an outbox event.
- Publish normalized-deal changes from the outbox to Kafka.

### FR-5: Handle retries and terminal failures

- Classify failures as retryable or terminal.
- Require every Kafka consumer to send terminally invalid input events to the common DLQ before committing their offsets.
- Retry transient dependency failures with bounded, delayed attempts.
- Send exhausted retries, invalid events, and unresolvable terminal normalization failures to the DLQ.
- Include enough original-event and failure metadata in a DLQ record to diagnose and replay it safely.
- Never silently discard a consumed event.

### FR-6: Submit an optimization request

- Accept 1–50 grocery list items, origin coordinates, fuel efficiency, and optional radius.
- Validate that fuel efficiency is greater than zero and no greater than 50 L/100 km.
- Treat the request as stateless; do not persist the shopper's list or exact origin in the MVP.
- Normalize grocery-list text against the same catalog without mutating that catalog during the request.

### FR-7: Rank nearby stores

- Consider only current deals at stores inside the radius.
- Use at most one qualifying deal per requested item per store; choose the deal producing the greatest item savings.
- Return exactly the highest-ranked single store when at least one store has a matched,
  savings-eligible item.
- For each store return distance, matched and unmatched items, gross savings, fuel cost, net savings, assumptions, and calculation timestamp.
- Include stores with negative net savings when they have at least one matched, savings-eligible item so the tradeoff is visible.
- Exclude stores with no matched, savings-eligible items.

### FR-8: Operability

- Expose liveness and readiness endpoints for HTTP services and workers.
- Emit structured logs containing event, correlation, and trace identifiers.
- Record counters for consumed, normalized, retried, duplicate, and DLQ events, plus normalization method and latency.
- Do not log grocery-list contents, exact shopper coordinates, API credentials, or full LLM prompts.

## 5. External API summary

The detailed ownership and payload boundaries are defined in `ARCHITECTURE.md`.

- `POST /internal/v1/deal-batches` — ingest a complete source batch.
- `GET /v1/deals` and `GET /v1/deals/{deal_id}` — query the current active-deal projection.
- `POST /v1/optimizations` — synchronously calculate store rankings from the current projection.
- `GET /health/live` — process liveness.
- `GET /health/ready` — dependency readiness.

## 6. Non-functional requirements

- **Correctness:** duplicate Kafka delivery must not produce duplicate product, deal, or projection state.
- **Consistency:** ranking is eventually consistent with ingestion; the target is that 95% of accepted deals are rankable within 60 seconds under normal local load.
- **Latency:** the optimization endpoint should complete within 2 seconds at p95 for up to 50 items and 100 nearby stores, excluding cold-start model setup.
- **Reliability:** every terminally failed event must be represented in the DLQ.
- **Explainability:** every ranking exposes the formula inputs and every normalization records its inference route.
- **Security:** internal ingestion is protected by a static service token in the MVP; public input is bounded and validated; secrets come from environment or mounted secret files.
- **Testability:** business formulas and consumers are testable without Kafka, PostgreSQL, or external AI calls; integration tests cover real container dependencies.

These are engineering targets for the MVP, not production service-level agreements.

## 7. Explicit non-goals

- Automated web scraping, OCR, or retailer partnerships.
- User accounts, authentication for shoppers, saved lists, or personalization.
- Multi-store baskets, route optimization, traffic-aware distance, transit, or delivery fees.
- Comparing full basket cost or estimating prices for non-deal items.
- Coupons, loyalty programs, taxes, deposits, complex promotions, or inventory availability.
- Purchasing, checkout, payments, or retailer deep links.
- A product-catalog management UI or human-review queue.
- Multiple currencies in one deployment.
- Training or fine-tuning an embedding model or LLM.
- Production-grade Kafka or PostgreSQL high availability.

## 8. MVP acceptance criteria

The MVP is complete when all of the following are demonstrated in Docker Compose:

1. A fixture batch is accepted and produces raw-deal events conforming to the documented schema.
2. Raw deals are normalized through both the vector-match path and mocked LLM-fallback path.
3. Re-delivering the same raw and normalized events leaves exactly one logical deal and one ranking projection row.
4. A transient normalization failure follows the documented retry path and later succeeds without duplicate state.
5. An invalid event and an exhausted retry each produce a diagnosable DLQ record.
6. A grocery-list request returns deterministically ranked nearby stores with per-item explanations.
7. Automated tests verify distance, package quantity, fuel cost, tie-breaking, inactive deals, unmatched items, and negative net savings.
8. The full local stack starts from documented commands and exposes healthy service checks.

## 9. Assumptions to validate after MVP

- Flyer sources can supply stable deal IDs and store applicability.
- Regular prices and package quantities are present often enough to calculate useful savings.
- Straight-line distance with a road factor is sufficiently accurate for an initial decision aid.
- Embedding retrieval plus a constrained LLM fallback reaches acceptable normalization accuracy at acceptable cost.
