# MVP Release Report

Historical verification from September 11, 2026. For current checks and remaining
implementation gaps, see [docs/STATUS.md](docs/STATUS.md) and the latest GitHub CI run.

Date: 2026-09-11  
Overall release status: **PASS**

The required fixture-to-frontend flow passes in the local Docker Compose stack. No
release-blocking checks remain.

## 1. Tests and commands executed

### Dependencies

```powershell
& '.\.venv\Scripts\python.exe' -m pip install -e '.[infrastructure,dev]'
Push-Location apps/web
npm ci
Pop-Location
```

- Python editable installation: passed on Python 3.12.13.
- npm clean installation: 131 packages installed, 0 vulnerabilities.
- The first sandboxed pip attempt could not access the package index. The identical
  command passed when network access was permitted; this was an execution-environment
  restriction, not a dependency-resolution failure.

### Compose and migrations

```powershell
docker compose run --rm migrate
docker compose up --build --detach --wait
docker compose ps --all
```

- All four SQL migrations applied idempotently.
- Kafka and PostgreSQL are healthy.
- Ranking API, ingestion API, normalization worker, and web are healthy.
- Normalization outbox and ranking projector are running.
- Kafka topic initialization and database migration jobs exited with code 0.
- One intermediate restart command used the nonexistent service name `normalization`;
  it failed immediately and was corrected to the declared `normalization-worker`
  service. The final documented full-stack command passed.

### Backend, infrastructure, and end-to-end tests

```powershell
$env:RUN_INFRA_SMOKE = '1'
$env:RUN_ACCEPTANCE = '1'
$env:TEST_RANKING_DATABASE_URL = 'postgresql://grocery_app:replace_me_for_local_development@127.0.0.1:55432/grocery_savings'
$env:TEST_KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
& '.\.venv\Scripts\python.exe' -m pytest
```

- Final result: **148 passed in 49.23s**.
- The dedicated live acceptance test also passed independently: **1 passed in
  21.02s**.
- The acceptance test verifies strict raw/retry/DLQ Pydantic events, both retry topics,
  vector and LLM routes, normalization and ranking persistence, duplicate raw event
  IDs, logical duplicate submissions, expired filtering, frontend proxying, and exact
  recommendation totals.
- During development, an assertion that a particular milk deal must be the vector
  event failed because the same batch intentionally contains active and expired copies
  of that description, which may process on different partitions. A durable audit
  table was added and the test now asserts that the batch exercised the vector route;
  it does not infer the route from whichever logical deal wins the race.

### Frontend and browser verification

```powershell
Push-Location apps/web
npm test
npm run build
Pop-Location
```

- Vitest: **2 files passed, 7 tests passed**.
- Next.js 16.3.5 production build: passed, including TypeScript and static page
  generation.
- A live browser submission rendered **Fixture Market Downtown**, grocery savings
  `$1.50`, round-trip distance `0.0 km`, fuel cost `$0.00`, and net savings `$1.50`.
- The first browser probe ran after database integration tests had cleaned their
  fixtures and correctly returned no match. Resubmitting `fixtures/flyer.json` restored
  active fixture data and the browser check passed.

### Static checks

```powershell
& '.\.venv\Scripts\python.exe' -m compileall -q apps packages tests
git diff --check
```

- Python compilation passed.
- `git diff --check` passed; Git emitted only Windows LF-to-CRLF conversion warnings.

## 2. Passed checks

- [x] Project dependencies install successfully.
- [x] Docker Compose starts all required infrastructure and application processes.
- [x] Kafka pipeline works from raw through normalized topics.
- [x] Fixture ingestion works through the authenticated batch endpoint.
- [x] Duplicate raw and normalized messages are handled idempotently.
- [x] Malformed, unresolvable, and retry-exhausted events reach the common DLQ.
- [x] Embedding normalization and pgvector matching work.
- [x] Structured mocked LLM fallback and alias caching work.
- [x] Normalized deals, audit data, inbox records, and outbox rows persist correctly.
- [x] Ranking deal and optimization endpoints work.
- [x] Expired deals are filtered at the current UTC boundary.
- [x] Recommendation distance, quantity, savings, fuel, ordering, and rounding math work.
- [x] Frontend can call the backend through its server-side proxy.
- [x] Frontend displays the recommended store and full savings breakdown.
- [x] `python -m pytest` passes with all live gates enabled.
- [x] Frontend tests pass.
- [x] Frontend production build passes.
- [x] README setup and complete-MVP instructions match the verified commands.

## 3. Failed checks

None in the final release gate.

## 4. Fixes made

- Implemented authenticated, whole-batch validation and transactional/idempotent raw
  Kafka publication in the ingestion API.
- Added the fixture flyer used by the documented local flow.
- Implemented deterministic text/embedding normalization, pgvector candidate matching,
  structured offline LLM fallback, alias caching, and audit metadata.
- Added normalization-owned product, alias, deal, inbox, audit, and outbox tables.
- Added version-guarded logical-deal upserts and stable event identities.
- Implemented raw and delayed-retry consumers, transactional Kafka handoffs, bounded
  retries, and terminal invalid/unresolvable/exhausted DLQ records.
- Added the documented Pydantic normalization-retry event and golden contract fixture.
- Added an outbox relay and propagated normalized-event correlation/trace headers.
- Added request-time shared embeddings and pgvector catalog matching to ranking.
- Wired topic initialization, migrations, ingestion, normalization worker, outbox,
  ranking projector, APIs, and web into Compose.
- Added live infrastructure and complete fixture-to-frontend acceptance coverage.
- Corrected README setup, fixture submission, test, and build instructions.
- Reduced Docker build context by excluding frontend dependency/build caches.

No product feature outside `docs/SPEC.md` was added, and existing v1 event schemas were
not changed. The already-documented retry v1 schema was added to the shared contract
package.

## 5. Known limitations

- The local embedding and LLM providers are deterministic offline fakes. They prove the
  routing and persistence contracts but do not measure real-provider quality.
- `.env.example` accelerates the documented 1-minute/10-minute retry stages to one
  second and enables source-deal-ID-based failure injection for acceptance testing.
  Disable `NORMALIZATION_FIXTURE_FAILURES` and use 60/600 seconds outside local release
  verification.
- Compose is a single-node Kafka/PostgreSQL development deployment, as permitted by the
  MVP non-goals; it is not production high availability.
- Structured event metrics and dedicated HTTP health servers for the non-HTTP outbox
  and ranking-projector processes remain operational hardening work. Compose verifies
  those processes are running, while their dependency behavior is integration-tested.

## 6. Architecture deviations

- Compose currently uses the general `grocery_app` database role for both private
  schemas instead of enforcing separate normalization/ranking owner roles. Service code
  does not perform cross-schema reads, so the logical boundary is preserved, but the
  database permission boundary is not yet enforced.
- The outbox relay is configured as one local replica and scans pending rows without
  `FOR UPDATE SKIP LOCKED`. Stable event IDs and downstream inboxes preserve duplicate
  safety, but multiple concurrent relay replicas are not supported by this MVP setup.
- The normalization worker exposes HTTP liveness/readiness; the outbox relay and ranking
  projector rely on process state in Compose rather than the worker HTTP endpoints
  described in the architecture.

These deviations do not block the verified single-node local MVP flow, but they must be
resolved before treating the stack as a horizontally scaled or production deployment.

## 7. Exact instructions to run the MVP locally

Prerequisites: Docker Desktop with Compose, Python 3.11+, and Node/npm.

```powershell
Copy-Item .env.example .env
python -m venv .venv
& '.\.venv\Scripts\python.exe' -m pip install -e '.[infrastructure,dev]'

Push-Location apps/web
npm ci
Pop-Location

docker compose up --build --detach --wait
docker compose ps --all

curl.exe -X POST http://127.0.0.1:8001/internal/v1/deal-batches `
  -H "Authorization: Bearer local-development-token" `
  -H "Content-Type: application/json" `
  --data-binary '@fixtures/flyer.json'
```

Open `http://127.0.0.1:3000` and submit `bread` from latitude `43.6532`, longitude
`-79.3832`. Ingestion is asynchronous, so allow a few seconds after the `202` response.

Run every release gate:

```powershell
$env:RUN_INFRA_SMOKE = '1'
$env:RUN_ACCEPTANCE = '1'
$env:TEST_RANKING_DATABASE_URL = 'postgresql://grocery_app:replace_me_for_local_development@127.0.0.1:55432/grocery_savings'
$env:TEST_KAFKA_BOOTSTRAP_SERVERS = '127.0.0.1:9092'
& '.\.venv\Scripts\python.exe' -m pytest
Remove-Item Env:RUN_INFRA_SMOKE
Remove-Item Env:RUN_ACCEPTANCE
Remove-Item Env:TEST_RANKING_DATABASE_URL
Remove-Item Env:TEST_KAFKA_BOOTSTRAP_SERVERS

Push-Location apps/web
npm test
npm run build
Pop-Location
```

Stop without deleting data:

```powershell
docker compose down
```

## Final git status

The worktree contains the intended release fixes and report, all uncommitted. See the
final handoff response for the exact `git status --short` listing.
