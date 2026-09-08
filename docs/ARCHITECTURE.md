# Grocery Savings Optimizer — MVP Architecture

## 1. Architectural overview

The MVP is an event-driven pipeline with three application boundaries and two supporting worker processes:

```text
Source adapter/operator
        |
        v
  Ingestion API
        |
        v
grocery.deals.raw.v1
        |
        v
Normalization worker ----> Embedding provider
        |                         |
        |                         +----> LLM provider (fallback only)
        v
Normalization schema in PostgreSQL + transactional outbox
        |
        v
  Outbox relay
        |
        v
grocery.deals.normalized.v1
        |
        v
Ranking projector ----> Ranking schema in PostgreSQL
                              ^
                              |
Next.js client ------> Ranking API
```

Kafka provides asynchronous decoupling and replay. PostgreSQL is one physical MVP instance with separately owned schemas. No service reads or writes another service's private tables. The normalization service publishes facts; the ranking service builds its own read model.

Delivery is at least once end to end. Correctness comes from stable identities, database uniqueness, inbox/idempotency records, transactional outboxes, and idempotent projections—not from assuming exactly-once delivery across Kafka and PostgreSQL.

## 2. Service boundaries

### 2.1 Ingestion service

**Owns**

- validation and source-to-raw-contract mapping;
- batch acceptance semantics;
- Kafka production to `grocery.deals.raw.v1`;
- source authentication at the internal endpoint.

**Does not own** product normalization, catalog data, deal persistence, or ranking.

The service validates the entire submitted batch before publishing. It publishes the batch in one Kafka transaction using an idempotent producer, so a broker failure cannot expose a partial accepted batch. Repeated accepted submissions are allowed because downstream consumers enforce logical idempotency.

### 2.2 Normalization service

Implemented as one codebase with two runtime processes:

- **normalization worker:** consumes raw and retry events, performs deterministic parsing and AI routing, and writes normalized state;
- **outbox relay:** publishes committed outbox rows to the normalized topic.

**Owns**

- canonical products and aliases;
- embeddings and similarity policy;
- normalized source deals;
- normalization audit metadata;
- raw-event idempotency records;
- normalized-event outbox records;
- failure classification for normalization.

**Does not own** shopper requests, distance/fuel calculation, or store rankings.

### 2.3 Ranking service

Implemented as one codebase with two runtime processes:

- **ranking projector:** consumes normalized-deal events into a query-optimized projection;
- **ranking API:** normalizes submitted list items read-only, finds eligible deals, and calculates rankings synchronously.

**Owns**

- the normalized deal/store projection;
- normalized-event idempotency records;
- list matching and ranking policy;
- distance, savings, and fuel-cost formulas.

The ranking service does not mutate the canonical product catalog during a request. It embeds each cleaned list item with the same configured embedding model and searches a read-only catalog projection carried by normalized deal events. It never uses the LLM fallback on the synchronous request path; list text that cannot be matched with sufficient confidence is returned as unmatched.

### 2.4 Web client

The Next.js client collects the grocery list, origin, fuel efficiency, and radius, then renders the ranking explanation. It contains no authoritative pricing, matching, or calculation logic.

## 3. Kafka topics and event schemas

### 3.1 Topic inventory

| Topic | Key | Producer | Consumer group | Purpose |
|---|---|---|---|---|
| `grocery.deals.raw.v1` | `source:store_location_id:source_deal_id` | ingestion API | `normalization-v1` | Accepted source deal facts |
| `grocery.deals.normalize.retry.1m.v1` | original record key | normalization worker | `normalization-retry-1m-v1` | First delayed retry |
| `grocery.deals.normalize.retry.10m.v1` | original record key | normalization worker | `normalization-retry-10m-v1` | Final delayed retry |
| `grocery.deals.normalized.v1` | `source:store_location_id:source_deal_id` | normalization outbox relay | `ranking-projector-v1` | Current normalized deal upserts |
| `grocery.deals.dlq.v1` | original record key | normalization worker, ranking projector | operational tooling only | Terminal failures |

MVP topics use at least three partitions for raw, normalized, and retry streams so ordering is preserved for one key while unrelated deals can progress independently. Local retention defaults are 7 days for raw/retry, 30 days for normalized, and 30 days for DLQ. The normalized topic may be compacted in addition to time-based retention. Retention is deployment configuration, not a business guarantee.

### 3.2 Event envelope

All Kafka values are UTF-8 JSON serialized from strict Pydantic models. Unknown fields are rejected at service boundaries. Timestamps are UTC RFC 3339 strings; IDs are UUIDs; money is serialized as decimal strings.

```json
{
  "event_id": "uuid",
  "event_type": "grocery.deal.raw",
  "schema_version": 1,
  "occurred_at": "2026-09-02T12:00:00Z",
  "producer": "ingestion-service",
  "correlation_id": "uuid",
  "causation_id": null,
  "trace_id": "string",
  "payload": {}
}
```

- `event_id` identifies one emitted event and drives consumer inbox deduplication.
- The Kafka record key identifies the logical aggregate and preserves its ordering.
- `correlation_id` ties a source batch and all derived events together.
- `causation_id` points to the immediate input event when an event is derived.
- `schema_version` is an integer version of the event type, not the application release.

Headers duplicate `event_id`, `event_type`, `schema_version`, `correlation_id`, and `trace_id` for routing and diagnostics. The JSON value remains authoritative.

### 3.3 Raw deal event

`event_type = grocery.deal.raw`, `schema_version = 1`

```text
payload:
  source: string
  source_deal_id: string
  source_updated_at: datetime
  store:
    store_location_id: string
    chain_name: string
    store_name: string
    address_summary: string
    latitude: decimal
    longitude: decimal
  product:
    raw_name: string
    raw_brand: string | null
    raw_size: decimal | null
    raw_unit: string | null
  offer:
    deal_price: decimal
    regular_price: decimal | null
    currency: three-letter uppercase string
    package_quantity: decimal | null
    package_unit: string | null
    valid_from: datetime
    valid_to: datetime
```

Constraints include non-negative prices, `valid_to > valid_from`, valid coordinates, and a currency matching the deployment currency. Free-form source payloads do not enter Kafka.

### 3.4 Retry event

`event_type = grocery.deal.normalization_retry`, `schema_version = 1`

```text
payload:
  original_event: RawDealEventV1
  attempt: integer                 # 1 for the first retry, 2 for the final retry
  not_before: datetime
  last_error:
    classification: transient_dependency | transient_database | transient_unknown
    code: bounded string
    message: sanitized bounded string
```

The retry consumer pauses only the affected partition until the head record's `not_before` time. Separate delay topics bound head-of-line blocking. The initial attempt plus two delayed retries yields at most three normalization attempts.

### 3.5 Normalized deal event

`event_type = grocery.deal.normalized`, `schema_version = 1`

```text
payload:
  source: string
  source_deal_id: string
  source_updated_at: datetime
  store: StoreLocationV1          # same fields as the raw event
  product:
    canonical_product_id: uuid
    canonical_name: string
    brand: string | null
    package_quantity: decimal | null
    package_unit: string | null
    category: string | null
    search_text: string
    embedding: decimal[]
  offer: OfferV1                  # same fields as the raw event
  normalization:
    method: alias | vector | llm
    confidence: decimal           # 0 through 1
    embedding_model: string
    embedding_version: string
    llm_model: string | null
    prompt_version: string | null
    catalog_version: integer
```

This is an upsert fact. The embedding array has the fixed dimension associated with `embedding_model` and `embedding_version`; consumers reject incompatible dimensions. Carrying it makes the ranking projection independently rebuildable without cross-schema reads or repeated catalog embedding calls. For a logical deal, consumers retain the event with the newest `source_updated_at`; equal timestamps are resolved deterministically by `event_id`.

### 3.6 Dead-letter event

`event_type = grocery.deal.dead_lettered`, `schema_version = 1`

```text
payload:
  original_topic: string
  original_partition: integer
  original_offset: integer
  original_key: string | null
  original_event: object | null
  raw_value_base64: string | null
  failed_stage: deserialize | validate | normalize | persist | project
  classification: terminal_invalid | terminal_unresolvable | retries_exhausted
  attempts: integer
  error_code: bounded string
  error_message: sanitized bounded string
  failed_at: datetime
```

Exactly one of `original_event` or `raw_value_base64` is expected. Raw bytes are size-limited. Secrets and stack traces are never included. A future replay tool must create a new `event_id`, retain the correlation ID, and set causation to the DLQ event; manual offset rewinds are not the replay contract.

## 4. Consumer correctness, retries, and DLQ

### 4.1 Idempotent database consumer algorithm

For each valid event, a database-owning consumer performs one transaction:

1. Insert `event_id` into its `processed_events` table with a unique constraint.
2. If the insert conflicts, treat the event as already applied and commit its Kafka offset.
3. Apply an upsert guarded by source version so an older replay cannot overwrite newer state.
4. If the consumer publishes a downstream fact, insert that event into `outbox_events` in the same transaction.
5. Commit the database transaction.
6. Commit the Kafka offset.

A crash after step 5 and before step 6 causes re-delivery; step 2 makes it harmless. A crash before step 5 leaves no partial state. Inbox records are retained at least as long as source topic retention plus the supported replay window.

### 4.2 Outbox relay algorithm

The relay claims unpublished rows using `FOR UPDATE SKIP LOCKED`, publishes them with an idempotent Kafka producer, and records publication. Publishing and marking cannot be one atomic cross-system transaction, so a crash can publish twice. The stable outbox `event_id` makes those duplicates harmless to compliant consumers.

### 4.3 Failure policy

- **Terminal immediately:** malformed JSON, unsupported schema version, contract validation failure, incompatible units, unsafe/invalid LLM output after one repair attempt, or no defensible canonical product mapping.
- **Retryable:** timeouts, rate limits, temporary AI-provider failures, temporary database failures, and broker/network failures.
- **Programming defect:** treated as transient unknown for bounded retries, then DLQ; alerting must make it visible.

For retryable normalization failures, producing the retry record and committing the consumed offset occur in one Kafka transaction. Terminal records are sent to the DLQ with the same consume-transform-produce transaction. If the broker transaction aborts, the original record remains available.

Database commit failures occur before offset commit and therefore re-deliver from the original topic. Application-level retry topics are used only when waiting/backoff is needed, especially for external AI calls.

### 4.4 Ranking projector failure policy

The ranking projector sends invalid or incompatible normalized events to the common DLQ and commits the input offset atomically with that Kafka write. A PostgreSQL outage is not a defect in the event, so the projector leaves the offset uncommitted, pauses the affected partition, and retries with capped exponential backoff until readiness returns. This prevents a shared infrastructure incident from turning valid projection facts into DLQ noise. Duplicate delivery after recovery is harmless because projection writes use the inbox transaction.

## 5. AI normalization flow

```text
Raw product
  -> deterministic cleanup and unit parsing
  -> exact alias lookup
       -> hit: accept
       -> miss: create embedding
            -> pgvector top-k catalog search
                 -> strong + unambiguous + compatible: accept
                 -> otherwise: constrained LLM decision
                       -> existing compatible candidate: accept
                       -> defensible new product: create product + alias
                       -> invalid/uncertain: terminal failure to DLQ
```

### 5.1 Deterministic preprocessing

- Unicode and whitespace normalization.
- Case-folding for lookup while preserving source text for audit.
- Parsing of common package quantities and units into a controlled unit set.
- Unit conversion only within a compatible dimension (for example, grams to kilograms).
- Removal of flyer-only promotional phrases without removing product-defining attributes.

### 5.2 Retrieval policy

The embedding input is a versioned, deterministic text assembled from normalized name, brand, size, unit, and category hints. pgvector cosine distance retrieves a small top-k candidate set.

Configuration defines:

- an auto-accept similarity threshold;
- a minimum margin between the first and second candidates;
- compatible brand, category, and unit rules.

Auto-accept requires both the score and margin thresholds and deterministic compatibility. Threshold values are calibrated against a checked-in evaluation fixture before release; they are configuration, not hard-coded behavior.

### 5.3 LLM fallback

The LLM receives only the cleaned source description and bounded candidate metadata. It must return a versioned structured response containing:

- decision: `select_existing`, `create_new`, or `unresolved`;
- selected candidate ID when applicable;
- canonical fields for a proposed product when applicable;
- confidence and a short bounded rationale.

Pydantic validates the response. Code then rechecks unit/category compatibility and candidate membership. One structured-output repair call is allowed for syntactically invalid output; semantic uncertainty is not repeatedly prompted. The provider is hidden behind embedding and LLM interfaces so tests use deterministic fakes.

### 5.4 Catalog mutation and races

New catalog products are created only by the asynchronous normalization worker, never by the request API. A normalized fingerprint has a unique constraint. Concurrent attempts either create one row or load the winner, preventing duplicate catalog products. Aliases are keyed by source plus a normalized source-product fingerprint.

## 6. Database entities (PostgreSQL)

PostgreSQL includes the pgvector extension. IDs are UUIDs; timestamps are timezone-aware; monetary and quantity columns use fixed-precision numeric types rather than floating point.

### 6.1 `normalization` schema

| Entity | Important fields and constraints |
|---|---|
| `products` | `id`, canonical fields, unique normalized fingerprint, `embedding vector(n)`, embedding model/version, `catalog_version`, timestamps |
| `product_aliases` | `id`, `source`, normalized source fingerprint, `product_id`, method, confidence, model/prompt metadata; unique `(source, fingerprint)` |
| `deals` | `id`, source identity, source version, last source event ID, store snapshot, `product_id`, offer fields, normalization audit fields; unique `(source, store_location_id, source_deal_id)` |
| `processed_events` | `event_id` primary key, source topic/partition/offset, processed timestamp |
| `outbox_events` | stable `event_id`, aggregate key, event type/version, JSON payload, created/published timestamps, publish attempts and last error |

The product vector dimension is fixed by migration for the selected embedding model. Changing dimensions requires a versioned re-embedding migration or a new embedding column/table.

### 6.2 `ranking` schema

| Entity | Important fields and constraints |
|---|---|
| `store_locations` | stable store location ID, display fields, coordinates, latest source version and event ID |
| `deal_projection` | source deal identity, store ID, canonical product snapshot, offer fields, source version, last event ID, normalization metadata; unique `(source, store_location_id, source_deal_id)` |
| `catalog_projection` | canonical product ID, searchable canonical fields, embedding/model version, catalog version and last event ID |
| `processed_events` | `event_id` primary key, source topic/partition/offset, processed timestamp |

The normalized-deal event carries sufficient store and product data to update all ranking projections without joining the normalization schema. Shopper lists, origins, and results are not persisted in the MVP.

## 7. API boundaries

### 7.1 Ingestion API

#### `POST /internal/v1/deal-batches`

Authentication: static bearer service token supplied through deployment secrets.

Request:

```text
source: string
batch_id: string
deals: RawDealInput[1..500]
```

`RawDealInput` contains the raw event payload fields except `source`, which comes from the batch. Response is `202` with `batch_id`, `correlation_id`, and `accepted_count` only after the batch Kafka transaction commits. Contract errors return `422`; authentication failures return `401`; unavailable Kafka returns `503`. The endpoint does not wait for normalization.

### 7.2 Ranking API

#### `GET /v1/deals`

Returns the ranking service's active normalized-deal projection. Optional
`store_location_id` and `canonical_product_id` filters are applied before pagination;
limit is capped at 100 and offset at 10,000. The service evaluates activity using its current UTC time;
future and expired offers are never returned.

#### `GET /v1/deals/{deal_id}`

Returns one active projected deal. Unknown, future, and expired deals return `404` so
the endpoint cannot be used to bypass the active-deal boundary.

#### `POST /v1/optimizations`

Request:

```text
origin: { latitude, longitude }
fuel_efficiency_l_per_100km: decimal
radius_km: decimal = 15
limit: integer = 10
items[1..50]:
  client_item_id: string
  name: string
  quantity: decimal = 1
  unit: string | null
```

Response:

```text
calculated_at: datetime
assumptions:
  currency: string
  fuel_price_per_litre: decimal
  road_factor: decimal
  distance_method: haversine_with_road_factor
rankings:
  rank: integer
  store: StoreLocation
  one_way_distance_km: decimal
  matched_items: [...]
  unmatched_items: [...]
  gross_savings: decimal
  incremental_fuel_cost: decimal
  net_savings: decimal
```

Invalid input returns `422`. No usable list matches returns `200` with an empty ranking and item-level unmatched reasons. A stale projection does not make the API unavailable; readiness fails only when PostgreSQL or required model assets are unavailable.

### 7.3 Health boundaries

- `/health/live` verifies the process event loop.
- `/health/ready` verifies required Kafka/PostgreSQL connectivity for the specific process and availability of required model configuration.
- Health responses expose no secrets or dependency credentials.

## 8. Docker Compose architecture

The local stack contains:

| Container | Responsibility |
|---|---|
| `postgres` | PostgreSQL with pgvector; one instance, separate owned schemas |
| `kafka` | Single KRaft-mode Kafka broker for local development |
| `topic-init` | One-shot idempotent creation/configuration of required topics |
| `migrate` | One-shot database migrations before application readiness |
| `ingestion-api` | FastAPI ingestion process |
| `normalization-worker` | Raw/retry consumer and AI routing |
| `normalization-outbox` | Normalization outbox publisher |
| `ranking-projector` | Normalized event consumer |
| `ranking-api` | FastAPI optimization process |
| `web` | Next.js client |

The worker and outbox containers share the normalization image; the API and projector containers share the ranking image. This preserves independently scalable runtime roles without multiplying codebases.

Only the web and intended API ports are exposed to the host. Kafka and PostgreSQL remain on the Compose network. Health checks and `depends_on` readiness conditions prevent workers from starting before topics and migrations are ready. Named volumes preserve Kafka and PostgreSQL data locally. AI provider credentials are supplied at runtime and never baked into images.

The Compose topology is for development and acceptance testing. It deliberately does not claim production availability, TLS, multi-broker durability, autoscaling, or secret-management completeness.

## 9. Code/module boundaries

Planned repository shape:

```text
apps/
  ingestion/
  normalization/
  ranking/
  web/
packages/
  event_contracts/     # Pydantic Kafka schemas only
  domain/              # value objects/formulas with no infrastructure imports
tests/
  contract/
  integration/
  acceptance/
infra/
  migrations/
  kafka/
```

Shared packages contain contracts and genuinely stable domain primitives only. They do not become a shared service layer or permit cross-schema database access. Each application owns its adapters, persistence mappings, and configuration.

## 10. Observability and security

- JSON logs include service, event ID, topic/partition/offset, correlation ID, attempt, route (`alias`, `vector`, `llm`), duration, and outcome.
- Metrics include Kafka lag, inbox duplicates, outbox age, retry/DLQ counts, AI calls/errors/latency, vector confidence distribution, API latency, and match coverage.
- Trace context is propagated in HTTP and Kafka metadata.
- LLM prompts exclude shopper data and exact store coordinates; only ingested deal-product normalization fields are sent. Grocery item text may be sent individually to the configured embedding provider, without the full list, origin, or request identity.
- Logs redact tokens and truncate provider error bodies and source text.
- HTTP request sizes, batch sizes, string lengths, and numeric ranges are bounded by Pydantic models.

## 11. Testing strategy

- **Unit tests:** Pydantic validation, preprocessing, compatibility rules, retry classification, distance/fuel/savings formulas, best-deal selection, and tie-breaking.
- **Contract tests:** golden JSON fixtures for every event version; producer fixtures must parse with consumer models.
- **Consumer tests:** duplicate delivery, crash-boundary behavior, stale source version, outbox duplicate publication, retry exhaustion, and DLQ payloads.
- **Integration tests:** PostgreSQL/pgvector queries, Kafka partitioning and consumer behavior, outbox relay, and projections using Docker dependencies.
- **AI evaluation tests:** fixed labeled product pairs test thresholds; provider calls use fakes in the default suite.
- **Acceptance test:** ingest fixtures, observe normalized projections, submit a grocery list, and verify the ranked calculation end to end.

No test in the default suite depends on a paid or nondeterministic external AI provider.
