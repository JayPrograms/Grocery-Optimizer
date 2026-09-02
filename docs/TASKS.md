# Grocery Savings Optimizer — MVP Task Plan

Each task is intended to fit in one small change and must be independently verifiable. A task is done only when its listed verification passes and relevant documentation is updated. IDs indicate ordering, not ownership.

## Phase 0: Contracts and project skeleton

### T001 — Create the Python workspace skeleton

- **Depends on:** none
- **Deliverable:** Package layout for ingestion, normalization, ranking, shared event contracts, domain primitives, and tests; lint/test configuration only.
- **Verification:** A clean environment can install the workspace, import each package, and run an empty `pytest` suite successfully.

### T002 — Define shared event envelope types

- **Depends on:** T001
- **Deliverable:** Strict Pydantic envelope and metadata models for UUIDs, UTC timestamps, correlation, causation, trace, event type, and schema version.
- **Verification:** Unit tests accept a golden valid envelope and reject unknown fields, naive timestamps, invalid UUIDs, and incorrect event type/version.

### T003 — Define raw-deal event v1

- **Depends on:** T002
- **Deliverable:** Pydantic models and golden JSON fixture for `grocery.deal.raw` v1.
- **Verification:** Contract tests round-trip the fixture and reject negative prices, invalid coordinates, invalid currency, empty IDs, and reversed validity windows.

### T004 — Define retry and DLQ event v1 contracts

- **Depends on:** T002, T003
- **Deliverable:** Strict Pydantic retry and dead-letter models with bounded error metadata.
- **Verification:** Golden fixtures round-trip; tests reject invalid attempts, missing original payload/bytes, oversized errors, and unsupported classifications.

### T005 — Define normalized-deal event v1

- **Depends on:** T002, T003
- **Deliverable:** Strict Pydantic normalized deal and normalization audit models.
- **Verification:** Golden fixture round-trips; tests require canonical product identity, correctly dimensioned embedding data, confidence in `[0,1]`, valid method-specific model metadata, and compatible offer fields.

### T006 — Add producer/consumer contract compatibility tests

- **Depends on:** T003, T004, T005
- **Deliverable:** Tests loading all versioned golden event fixtures through the exact consumer models.
- **Verification:** One command proves every producer fixture parses under the corresponding consumer contract and that an intentional breaking fixture fails.

## Phase 1: Local infrastructure

### T007 — Add PostgreSQL/pgvector Compose service

- **Depends on:** T001
- **Deliverable:** Pinned PostgreSQL image with pgvector, health check, named volume, and local-only credentials.
- **Verification:** Compose starts the container healthy and a check confirms the vector extension is available.

### T008 — Add Kafka KRaft Compose service

- **Depends on:** T001
- **Deliverable:** Pinned single-broker Kafka service with health check and internal listener.
- **Verification:** Compose starts Kafka healthy and a temporary client can obtain broker metadata.

### T009 — Provision Kafka topics idempotently

- **Depends on:** T008
- **Deliverable:** One-shot `topic-init` configuration for raw, normalized, two retry, and DLQ topics with documented partitions and retention.
- **Verification:** Running topic initialization twice succeeds and topic inspection matches the architecture document.

### T010 — Create owned database schemas and roles

- **Depends on:** T007
- **Deliverable:** Migration bootstrap for `normalization` and `ranking` schemas with separate least-privilege roles.
- **Verification:** Each role can mutate its own schema and a test proves it cannot read or mutate the other private schema.

## Phase 2: Ingestion

### T011 — Implement ingestion request validation

- **Depends on:** T003
- **Deliverable:** FastAPI request model for a 1–500 deal batch and mapping to raw events, without Kafka wiring.
- **Verification:** API tests cover valid mapping, whole-batch rejection, size limits, coordinate/price/date validation, and correlation ID generation.

### T012 — Publish accepted deals to Kafka

- **Depends on:** T009, T011
- **Deliverable:** Transactional idempotent Kafka producer that emits one raw event per deal with the documented key and headers.
- **Verification:** An integration test submits a batch, receives `202`, consumes the expected event count, validates each event, and confirms all keys and correlation IDs; an injected publish failure exposes none of the batch to read-committed consumers.

### T013 — Protect the ingestion endpoint

- **Depends on:** T011
- **Deliverable:** Constant-time static bearer-token authentication loaded from runtime configuration.
- **Verification:** API tests show missing/wrong tokens return `401`, the correct token succeeds, and neither logs nor error bodies expose the token.

## Phase 3: Normalization persistence and idempotency

### T014 — Migrate normalization tables

- **Depends on:** T010
- **Deliverable:** Products, aliases, deals, processed events, and outbox tables with documented keys and constraints.
- **Verification:** Migration applies from empty, rolls back in a disposable database, and schema inspection confirms uniqueness, precision, vector dimension, and ownership.

### T015 — Implement the normalization inbox transaction

- **Depends on:** T014
- **Deliverable:** Transaction helper that records an event once and safely identifies duplicates.
- **Verification:** Database tests deliver the same event concurrently and prove exactly one transaction receives permission to apply it.

### T016 — Implement version-guarded deal upsert with outbox write

- **Depends on:** T005, T014, T015
- **Deliverable:** One transaction that upserts a normalized deal, records processing, and inserts a stable normalized-event outbox row.
- **Verification:** Tests prove duplicate delivery leaves one deal/outbox event, an older source version cannot overwrite a newer one, and rollback leaves none of the three records.

### T017 — Implement the outbox relay

- **Depends on:** T009, T016
- **Deliverable:** Worker that claims outbox rows safely, publishes normalized events, and tracks publication attempts.
- **Verification:** Integration tests run two relay instances without double-claiming and simulate publish-before-mark failure to prove a repeated stable event ID is possible and expected.

## Phase 4: Product normalization and AI routing

### T018 — Implement deterministic product preprocessing

- **Depends on:** T001
- **Deliverable:** Pure functions for text cleanup, promotional phrase removal, quantity parsing, controlled units, compatible conversions, and normalized fingerprints.
- **Verification:** Table-driven tests cover representative names, Unicode/whitespace, multipacks, mass/volume/count units, and incompatible conversions.

### T019 — Implement alias lookup and race-safe alias persistence

- **Depends on:** T014, T018
- **Deliverable:** Source/fingerprint alias lookup plus unique, concurrent-safe persistence.
- **Verification:** Database tests prove repeat and concurrent lookups resolve to one alias/product mapping without an AI call.

### T020 — Define provider-neutral embedding and LLM interfaces

- **Depends on:** T001
- **Deliverable:** Typed internal interfaces, configuration, deterministic fakes, and structured LLM response model.
- **Verification:** Unit tests run normalization orchestration entirely with fakes and reject malformed or semantically invalid LLM responses.

### T021 — Implement pgvector candidate retrieval

- **Depends on:** T014, T018, T020
- **Deliverable:** Versioned embedding text builder, product embedding persistence, top-k cosine retrieval, threshold/margin policy, and compatibility filters.
- **Verification:** pgvector integration fixtures show strong unambiguous compatible matches are accepted and weak, close, or incompatible matches are not auto-accepted.

### T022 — Add the labeled normalization evaluation fixture

- **Depends on:** T018, T021
- **Deliverable:** Small checked-in dataset of positive, ambiguous, and incompatible grocery product pairs plus a threshold evaluation test/report.
- **Verification:** The test reports precision/coverage for configured thresholds and fails when the agreed minimum precision is not met.

### T023 — Implement constrained LLM fallback

- **Depends on:** T020, T021
- **Deliverable:** Fallback orchestration limited to retrieved candidates, new-product proposal, or unresolved; includes one syntactic repair attempt and deterministic post-validation.
- **Verification:** Fake-provider tests cover select-existing, create-new, unresolved, invalid candidate ID, incompatible unit, malformed-first/valid-second, and repeated malformed output.

### T024 — Implement race-safe canonical product creation

- **Depends on:** T014, T018, T023
- **Deliverable:** Catalog insertion by unique normalized fingerprint with embedding/model/catalog metadata.
- **Verification:** Concurrent database test proves two equivalent proposals result in one canonical product and both callers receive its ID.

### T025 — Assemble the normalization routing policy

- **Depends on:** T019, T021, T023, T024
- **Deliverable:** Orchestrator implementing alias → vector → LLM, with auditable route/confidence/model data.
- **Verification:** Unit tests prove each route short-circuits later routes, repeated text uses the alias cache, and unresolved results return a typed terminal outcome.

## Phase 5: Normalization consumption, retries, and DLQ

### T026 — Implement failure classification

- **Depends on:** T004, T025
- **Deliverable:** Explicit mapping from validation, compatibility, provider, broker, database, and unknown failures to terminal or retryable outcomes.
- **Verification:** Table-driven tests cover every documented category and fail on an unclassified exception type.

### T027 — Consume and normalize raw deals

- **Depends on:** T009, T016, T025, T026
- **Deliverable:** Raw topic consumer connecting validation, routing, transactional persistence, and offset commit.
- **Verification:** Integration test consumes a valid deal, writes one normalized deal/outbox record, commits its offset, and handles redelivery without extra state.

### T028 — Implement delayed retry production and consumption

- **Depends on:** T004, T009, T026, T027
- **Deliverable:** Kafka-transactional handoff to 1-minute and 10-minute retry topics, attempt propagation, `not_before` handling, and partition pause/resume.
- **Verification:** With test-shortened delays, integration tests observe both delayed handoffs, no early consumption, correct attempt/header propagation, a typed exhausted outcome after the third attempt, and no committed-message loss across an aborted Kafka transaction.

### T029 — Implement terminal DLQ handling

- **Depends on:** T004, T009, T026, T028
- **Deliverable:** Kafka-transactional DLQ production for immediate terminal and exhausted retry outcomes, including bounded sanitized context.
- **Verification:** Tests send malformed bytes, invalid schema, unresolved normalization, and an exhausted transient failure; each produces a valid DLQ event, the exhausted case has exactly three total attempts, and the source offset advances only with the DLQ write.

## Phase 6: Ranking projection

### T030 — Migrate ranking projection tables

- **Depends on:** T010
- **Deliverable:** Store, catalog, deal projection, and processed-event tables with documented keys and source-version fields.
- **Verification:** Migration and schema tests confirm ownership, uniqueness, fixed-precision types, and required query indexes.

### T031 — Implement the idempotent ranking projector

- **Depends on:** T004, T005, T009, T030
- **Deliverable:** Normalized-topic consumer that transactionally updates store/catalog/deal projections and records event IDs, with common-DLQ handoff for terminal input and partition backoff for transient database failures.
- **Verification:** Integration tests prove duplicate events create one logical projection, older source versions are ignored, equal-version conflicts resolve deterministically, and a replay rebuilds equivalent state; an invalid event reaches the DLQ atomically, while a database outage leaves the valid record uncommitted until recovery.

## Phase 7: Ranking domain and API

### T032 — Implement distance and fuel-cost functions

- **Depends on:** T001
- **Deliverable:** Pure decimal-based Haversine, road-factor, round-trip fuel, and monetary rounding functions.
- **Verification:** Unit tests cover known coordinate distances, zero distance, boundary coordinates, configured assumptions, and rounding without binary floats.

### T033 — Implement package-aware item savings

- **Depends on:** T018
- **Deliverable:** Pure matching calculation for compatible quantities, packages needed, regular/deal price savings, and unmatched reasons.
- **Verification:** Tests cover unit conversion, partial packages/ceiling, deal price above regular price, missing regular price, missing quantity, and incompatible dimensions.

### T034 — Implement read-only grocery-list matching

- **Depends on:** T020, T021, T030, T033
- **Deliverable:** Bounded per-item embedding and vector matching against the ranking catalog projection without LLM calls or catalog mutation.
- **Verification:** Tests return stable matches/unmatched reasons and prove the request leaves catalog tables unchanged.

### T035 — Implement store ranking policy

- **Depends on:** T032, T033, T034
- **Deliverable:** Active/radius filtering, best deal per item/store, gross savings, fuel cost, net savings, deterministic ordering, and result limits.
- **Verification:** Unit tests cover inactive deals, radius boundary, duplicate offers, negative net savings, no matches, ties, and top-N limits.

### T036 — Implement `POST /v1/optimizations`

- **Depends on:** T030, T035
- **Deliverable:** FastAPI request/response boundary with documented validation and explainable output.
- **Verification:** API tests cover valid requests, 1/50/51 items, fuel/radius/limit boundaries, empty rankings, unmatched items, and returned calculation assumptions.

### T037 — Add health and readiness endpoints

- **Depends on:** T012, T027, T031, T036
- **Deliverable:** Liveness/readiness checks appropriate to each API and worker process.
- **Verification:** Tests show liveness remains up during dependency loss, readiness fails for required unavailable dependencies, and responses reveal no credentials.

## Phase 8: Web, observability, and acceptance

### T038 — Build the minimal optimization form

- **Depends on:** T036
- **Deliverable:** Next.js page for list items, origin coordinates, vehicle efficiency, and radius with accessible validation.
- **Verification:** Component/browser tests submit a valid request and block invalid list, coordinate, and fuel inputs.

### T039 — Render explainable ranked results

- **Depends on:** T038
- **Deliverable:** Results view showing store order, distance, matched/unmatched coverage, gross savings, fuel cost, net savings, and assumptions.
- **Verification:** Component tests render positive and negative net savings, empty ranking, unmatched items, and API error states.

### T040 — Add structured logging and metrics

- **Depends on:** T027, T029, T031, T036
- **Deliverable:** Consistent structured event/request logs and the metrics listed in `ARCHITECTURE.md`, with redaction.
- **Verification:** Tests/captured output confirm correlation fields and outcomes are present while tokens, full prompts, list text, and exact shopper coordinates are absent.

### T041 — Complete the Docker Compose application topology

- **Depends on:** T009, T010, T017, T027, T031, T036, T038
- **Deliverable:** Build definitions, one-shot migration/init jobs, health-gated startup, internal networking, and documented environment template for the entire MVP.
- **Verification:** From an empty disposable state, one documented command builds and starts all services healthy without a paid AI call.

### T042 — Add end-to-end happy-path acceptance test

- **Depends on:** T041
- **Deliverable:** Fixture-driven test that ingests deals through HTTP, waits for the projection, submits a grocery list, and asserts ranked calculations.
- **Verification:** The test passes from a clean Compose environment and exercises alias/vector plus mocked LLM routes.

### T043 — Add idempotency/retry/DLQ acceptance test

- **Depends on:** T041
- **Deliverable:** End-to-end failure-path test covering duplicate raw/normalized delivery, transient recovery, exhausted retry, and invalid input DLQ.
- **Verification:** The test proves one logical deal/projection after duplicates and valid, diagnosable DLQ records after terminal failures.

### T044 — Run the MVP release checklist

- **Depends on:** T006, T022, T037, T039, T040, T042, T043
- **Deliverable:** Recorded test commands/results and a documentation review against the MVP acceptance criteria.
- **Verification:** Unit, contract, integration, web, and acceptance suites pass; every requirement maps to at least one test; no feature outside `SPEC.md` was introduced.

## Definition of done for every implementation task

- Behavior is inside the scope of `SPEC.md` and consistent with `ARCHITECTURE.md` and `DECISIONS.md`.
- New behavior has focused automated tests, including duplicate/failure cases where relevant.
- Public event changes include updated Pydantic models, golden fixtures, compatibility tests, and documentation.
- Relevant tests pass locally.
- Logs and fixtures contain no secrets or sensitive shopper inputs.
- The task's verification can be repeated by another developer from documented commands.
