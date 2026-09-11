# Grocery Savings Optimizer — Architecture Decisions

This file records decisions that constrain the MVP. New decisions are appended. A changed decision is marked superseded and links to its replacement; history is not rewritten.

## ADR-001: Use three application boundaries

- **Status:** Accepted
- **Decision:** Separate ingestion, normalization, and ranking responsibilities. The web client is a presentation boundary. Normalization worker/outbox and ranking API/projector may run as separate processes from the same service codebase.
- **Why:** These parts scale and fail differently, while three codebases keep the MVP understandable. Product catalog mutation stays isolated from synchronous ranking.
- **Consequences:** Services communicate by documented events. Shared packages are limited to event contracts and stable domain primitives. No service queries another service's private schema.

## ADR-002: Use Kafka with at-least-once delivery

- **Status:** Accepted
- **Decision:** Do not claim cross-system exactly-once semantics. Use stable event IDs, aggregate keys, idempotent Kafka producers, consumer inbox tables, version-guarded upserts, and transactional database outboxes.
- **Why:** Kafka transactions cannot atomically commit PostgreSQL state. At-least-once delivery with explicit idempotency is simpler to reason about and test.
- **Consequences:** Duplicate events are expected. Every consumer must include duplicate and stale-event tests. Outbox publication can repeat the same event ID.

## ADR-003: Use versioned Pydantic JSON event contracts

- **Status:** Accepted
- **Decision:** Kafka values are strict JSON generated and validated by shared Pydantic models. Event type and integer schema version are present in the envelope. Topic names carry the major version.
- **Why:** The selected Python stack already uses Pydantic, and code-level schemas keep the first version small without a separate schema registry.
- **Consequences:** Unknown fields are rejected. Backward-compatible additions require optional/defaulted fields and fixture updates. Breaking changes require a new schema version, normally a new topic/consumer group, a migration plan, and documentation here and in `ARCHITECTURE.md`.

## ADR-004: Partition deal streams by logical deal identity

- **Status:** Accepted
- **Decision:** Key deal records by `source:store_location_id:source_deal_id`.
- **Why:** Kafka then preserves update order for a logical store deal while allowing unrelated deals to process in parallel.
- **Consequences:** Source adapters must supply stable identities. Consumers still guard on `source_updated_at` because replays and cross-topic retries can arrive out of order.

## ADR-005: Use bounded delayed retry topics and a terminal DLQ

- **Status:** Accepted
- **Decision:** Classify failures. Send transient normalization failures through 1-minute and 10-minute retry topics, for three total attempts. Send invalid, unresolvable, or exhausted events to one versioned DLQ.
- **Why:** Immediate retries amplify provider or database incidents; unlimited retries poison a partition. Two delay stages are enough for the MVP and make failure behavior inspectable.
- **Consequences:** Retry events carry attempt and `not_before`. Consumers pause the affected retry partition until due. The ranking projector sends terminally invalid input to the same DLQ but retains valid input during transient database outages. DLQ replay is manual and creates a new event ID with traceable causation. Production-scale retry scheduling is deferred.

## ADR-006: Normalize with deterministic rules, vector retrieval, then LLM fallback

- **Status:** Accepted
- **Decision:** Perform parsing and alias lookup first. Use embedding similarity against pgvector candidates next. Call an LLM only when retrieval is ambiguous; constrain it to structured selection, explicit new-product recommendation, or unresolved.
- **Why:** This route limits latency, cost, and nondeterminism while still handling messy flyer text.
- **Consequences:** Thresholds require a labeled evaluation fixture. Model, embedding, prompt, confidence, method, and catalog version are audited. Provider interfaces must be replaceable and fakeable. LLM output never bypasses deterministic validation.

## ADR-007: Allow catalog creation only in the asynchronous normalizer

- **Status:** Accepted
- **Decision:** The normalization worker may create a canonical product after validated LLM fallback. The ranking request path is read-only and returns uncertain list items as unmatched.
- **Why:** Synchronous catalog mutation would make request latency and catalog consistency dependent on user phrasing and concurrent requests.
- **Consequences:** A newly worded list item may remain unmatched until the catalog learns it from deal ingestion. Unique normalized fingerprints resolve concurrent catalog creation.

## ADR-008: Use one PostgreSQL instance with owned schemas

- **Status:** Accepted
- **Decision:** Use PostgreSQL plus pgvector, with `normalization` and `ranking` schemas and separate database roles. The ranking projection is populated from Kafka rather than cross-schema queries.
- **Why:** One instance keeps Docker Compose small; logical ownership maintains a future extraction path and prevents hidden coupling.
- **Consequences:** Migrations and permissions enforce ownership. A physical database outage affects both services in the MVP. Independent database clusters are deferred.

## ADR-009: Publish normalized state through a transactional outbox

- **Status:** Accepted
- **Decision:** Persist a normalized deal, processed-event record, and downstream event in one PostgreSQL transaction. A relay publishes the outbox event.
- **Why:** Directly publishing after a database commit can lose an event; publishing before commit can expose nonexistent state.
- **Consequences:** Publication is eventually consistent and may duplicate. Outbox age is monitored. Downstream consumers deduplicate by event ID.

## ADR-010: Rank one store at a time with an explicit fuel formula

- **Status:** Accepted
- **Decision:** Rank single stores by savings on matched deals minus the estimated fuel cost of a dedicated round trip. Use Haversine distance multiplied by a configured road factor. Use a deployment fuel price and shopper-supplied L/100 km.
- **Why:** This directly answers the MVP question without a mapping dependency or combinatorial multi-store routing.
- **Consequences:** Results are estimates, not route quotes or full-basket comparisons. The response must display fuel price, road factor, distance, match coverage, and unmatched items. Multi-store and live routing are out of scope.

## ADR-011: Keep optimization requests stateless

- **Status:** Accepted
- **Decision:** Do not store grocery lists, origins, or ranking results in the MVP.
- **Why:** Persistence is unnecessary for the core proof and would create privacy, retention, identity, and deletion requirements.
- **Consequences:** Requests cannot be retrieved or compared later. Operational telemetry must not contain list text or exact origin coordinates.

## ADR-012: Use decimal arithmetic and one deployment currency

- **Status:** Accepted
- **Decision:** Represent money and measurable quantities with fixed-precision decimals. Reject deals whose currency differs from deployment configuration.
- **Why:** Binary floating point is inappropriate for money, and currency conversion is outside MVP scope.
- **Consequences:** JSON money values use decimal strings. Rounding occurs at documented presentation boundaries, not during intermediate calculations.

## ADR-013: Use synchronous ranking over an event-built local projection

- **Status:** Accepted
- **Decision:** The HTTP ranking request reads the ranking service's PostgreSQL projection and calculates results synchronously. Deal ingestion and projection remain asynchronous.
- **Why:** The computation is bounded and users need an immediate answer; a separate job workflow adds no MVP value.
- **Consequences:** Results can lag recently accepted flyers. Responses include `calculated_at`; projection lag is monitored. The latency target constrains request and radius limits.

## ADR-014: Do not add a schema registry in the MVP

- **Status:** Accepted
- **Decision:** Enforce contracts through Pydantic packages, golden JSON fixtures, and CI compatibility tests.
- **Why:** A registry would add infrastructure before multiple languages or many independently deployed producers justify it.
- **Consequences:** CI and review must enforce compatibility. A registry should be reconsidered when non-Python producers, more teams, or broader event reuse appears.

## ADR-015: Keep external AI behind provider-neutral interfaces

- **Status:** Accepted
- **Decision:** Select embedding and LLM implementations through configuration and internal interfaces; do not encode provider-specific response types in domain or event schemas.
- **Why:** Availability, price, dimensions, and model lifetimes change. Tests must remain deterministic and offline.
- **Consequences:** The chosen embedding dimension is still fixed by a database migration and model change requires re-embedding. Default tests use fakes; a separately enabled smoke test may call the configured provider.

## ADR-016: Treat normalized events as upsert facts, not commands

- **Status:** Accepted
- **Decision:** `grocery.deals.normalized.v1` contains the complete projection data for a logical deal. Consumers select the newest source version and can rebuild their projection by replay.
- **Why:** Self-contained facts reduce temporal coupling and cross-service lookups.
- **Consequences:** Events duplicate bounded store/product data. Deactivation is represented by an updated validity window in the MVP; explicit deletion/tombstone behavior is deferred until a source requires it.

## ADR-017: Carry catalog search vectors in normalized deal events

- **Status:** Accepted
- **Decision:** Include canonical search text, embedding values, and embedding model/version in each normalized deal event.
- **Why:** The ranking service must rebuild its catalog projection and match list items without querying normalization-owned tables or embedding every catalog product again.
- **Consequences:** Deal events are larger and duplicate the product vector across store deals. Consumers validate vector dimension/model compatibility. If event volume makes this material, a separate compacted catalog-product topic is the preferred post-MVP evolution.

## ADR-018: Allow a stateless fuel-price request override

- **Status:** Accepted
- **Decision:** `POST /v1/optimizations` may receive a non-negative fuel price per litre. When omitted, the deployment-configured price remains the default. The effective price is returned in the response assumptions.
- **Why:** Fuel prices vary by shopper and location, and the web MVP must let the shopper use the price they expect to pay without moving fuel-cost calculations into the presentation layer.
- **Consequences:** Existing API clients remain compatible. The override is used only for the synchronous calculation and is not persisted. Currency remains deployment-configured.
