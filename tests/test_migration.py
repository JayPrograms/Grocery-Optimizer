from pathlib import Path


def test_ranking_migration_has_required_idempotency_constraints_and_indexes() -> None:
    migration = Path("infra/migrations/001_ranking_projection.sql").read_text()

    assert "event_id uuid PRIMARY KEY" in migration
    assert "UNIQUE (source, store_location_id, source_deal_id)" in migration
    assert "deal_projection_active_idx" in migration
    assert "embedding vector NOT NULL" in migration
