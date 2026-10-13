# Grocery Savings Optimizer — resume notes

Repository: https://github.com/JayPrograms/Grocery-Optimizer

## Project entry

**Grocery Savings Optimizer** | Python, FastAPI, Kafka, PostgreSQL/pgvector,
Pydantic, Next.js, Docker Compose

- Built an event-driven grocery recommendation MVP that ingests flyer deals,
  normalizes product descriptions, and recommends a single nearby store based on
  deal savings minus estimated round-trip fuel cost.
- Implemented versioned event contracts, idempotent consumers, source-version guards,
  a transactional outbox, bounded retries, and a dead-letter queue to handle duplicate
  delivery and terminal failures.
- Developed a Next.js interface and a FastAPI calculation engine with decimal money
  arithmetic, unit conversion, distance estimates, and explainable recommendations.

Use the bullets that reflect the work you can explain and defend in an interview.
The repository includes implementation, tests, architecture decisions, and limitations.

## Evidence and interview discussion

- `packages/event_contracts/models.py`: strict Pydantic event schemas.
- `apps/ingestion/main.py`: batch validation, service-token authentication, and Kafka
  transactional publication.
- `apps/normalization/pipeline.py`, `worker.py`, and `outbox.py`: normalization-owned
  persistence, duplicate/stale handling, retry/DLQ policy, and outbox publication.
- `apps/ranking/projector.py`, `consumer.py`, and `repository.py`: event-fed projection
  and consumer failure handling.
- `packages/domain/recommendations.py`: package-aware savings, compatible dimensions,
  fuel calculations, and deterministic ranking.
- `tests/`: contracts, consumers, domain calculations, database/Kafka integration, and
  a full fixture-to-frontend acceptance flow.
- `.github/workflows/ci.yml`: automated Python/frontend checks and Compose acceptance.

Read [STATUS.md](STATUS.md) and the current CI run before quoting test counts.

## Claims the implementation does not support

- Production usage, measured user savings, or retailer partnerships.
- Real LLM accuracy, learned embeddings, or model training. Providers are offline fakes.
- Exactly-once delivery across Kafka and PostgreSQL. Delivery is at least once, with
  application-level idempotency.
- Live navigation, full-basket price comparison, or multi-store optimization.
- Achieved latency/throughput targets without a benchmark. SPEC.md states targets,
  not measured results.

Useful interview tradeoffs include why the outbox is needed, what happens after a
database commit but before Kafka offset commit, why aliases reduce normalization work,
why the ranking request does not mutate the catalog, and why incomplete deal coverage
cannot establish the cheapest complete basket.
