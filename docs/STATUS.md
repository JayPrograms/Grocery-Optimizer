# Current verification and implementation status

Prepared October 12, 2026 for publishing the existing local MVP to GitHub.

## Implementation

The repository contains ingestion, normalization, outbox, projection, ranking API,
and Next.js client implementations, plus Docker Compose and migrations. The normal
application path uses Kafka and PostgreSQL/pgvector. The embedding and LLM providers
are deterministic fixtures rather than external AI integrations.

The README screenshot was captured from the actual frontend and ranking engine with
a temporary in-memory sample projection. That temporary demo harness is not part of
the normal application, and the screenshot does not demonstrate Kafka ingestion or
database persistence.

## Local verification

- Python before the startup regression addition: **133 passed, 15 skipped**. The skipped tests require Kafka/PostgreSQL or the
  full Compose stack.
- The full local stack could not be rerun because Docker Desktop fails during startup
  with a stale `dockerInference` runtime socket error. Project volumes were not reset.
- The September 11 report records **148 tests passing with live gates enabled** and
  **7 frontend tests passing**, plus a production build. Those are historical results,
  not a new full-stack verification.

Current frontend build, tests, and dependency audit results are recorded in the
GitHub publishing update below. GitHub Actions provides per-commit checks, including
a separate Compose job; consult its actual result rather than assuming success.

## Remaining gaps

- Enforce separate least-privilege database roles for the two owned schemas.
- Finish structured event metrics and operational logging coverage.
- Add dedicated health/readiness endpoints to the outbox and ranking projector.
- Coordinate outbox claims before running multiple relay replicas.
- Add real provider adapters, deterministic compatibility checks for ambiguous
  normalization, and labeled evaluation evidence before claiming AI matching quality.
- Benchmark the specification's latency and eventual-consistency targets.

These gaps are documented rather than represented as completed features. See
[RELEASE_REPORT.md](../RELEASE_REPORT.md) for the historical implementation report and
[TASKS.md](TASKS.md) for the original plan.

## GitHub publishing update

- Updated Next.js from 16.3.5 to 16.3.8 and refreshed vulnerable transitive dependencies.
- Frontend: **7 passed across 2 test files**.
- `npm run build`: passed, including TypeScript and production page generation.
- `npm audit --audit-level=high`: passed, with **0 reported vulnerabilities** at the
  time of this check.
- `docker compose config --quiet` and `git diff --check`: passed.
- Added a GitHub Actions workflow for Python, frontend, and live Compose checks.
- Preserved the GitHub repository's initial commit when joining it to local history.
- The first Compose CI run exposed concurrent catalog seeding failing on the product
  primary key. Seed inserts now handle either unique identity conflict, with a live
  PostgreSQL regression covering six concurrent seeders and a later repeat startup.

Live infrastructure verification for the publishing commit is provided by the linked
GitHub Actions run. Its outcome may differ from the local non-container checks above.
