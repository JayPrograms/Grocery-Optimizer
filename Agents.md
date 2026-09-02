# Project Rules

Read before making changes:
- docs/SPEC.md
- docs/ARCHITECTURE.md
- docs/DECISIONS.md

Tech stack:
- Python + FastAPI
- Kafka
- PostgreSQL + pgvector
- Pydantic
- Next.js
- Docker Compose
- pytest

Engineering rules:
- Kafka events must use Pydantic schemas.
- Consumers must be idempotent.
- Failed terminal events go to the DLQ.
- Do not change public event schemas without documenting it.
- Add tests for new behavior.
- Run relevant tests before finishing.

MVP scope is defined in SPEC.md.
Do not add functionality outside that scope.