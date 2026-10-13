import asyncio
import os
from threading import Barrier
from uuid import NAMESPACE_URL, uuid4, uuid5

import psycopg
import pytest

from apps.normalization import pipeline


DATABASE_URL = os.environ.get("TEST_RANKING_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    DATABASE_URL is None,
    reason="set TEST_RANKING_DATABASE_URL to run PostgreSQL normalization tests",
)


async def test_concurrent_catalog_seeding_is_idempotent(monkeypatch) -> None:
    """Raw and retry consumers must be able to seed a fresh catalog together."""
    assert DATABASE_URL is not None
    product_name = f"startup seed {uuid4().hex}"
    monkeypatch.setattr(pipeline, "SEED_PRODUCTS", ((product_name, "grocery"),))
    real_connect = psycopg.connect
    start_barrier = Barrier(6)

    def concurrent_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        try:
            start_barrier.wait(timeout=15)
        except BaseException:
            connection.close()
            raise
        return connection

    monkeypatch.setattr(pipeline.psycopg, "connect", concurrent_connect)
    repositories = [pipeline.NormalizationRepository(DATABASE_URL) for _ in range(6)]
    await asyncio.gather(*(repository.ensure_seed_catalog() for repository in repositories))
    # Use the saved adapter because the startup barrier is only for concurrent writers.
    with real_connect(DATABASE_URL) as connection:
        rows = connection.execute(
            "SELECT canonical_product_id, fingerprint FROM normalization.products "
            "WHERE fingerprint = %s",
            (pipeline.normalize_text(product_name),),
        ).fetchall()
    fingerprint = pipeline.normalize_text(product_name)
    assert rows == [(uuid5(NAMESPACE_URL, f"grocery-product:{fingerprint}"), fingerprint)]

    # A later startup must also leave the same row intact.
    monkeypatch.setattr(pipeline.psycopg, "connect", real_connect)
    await repositories[0].ensure_seed_catalog()
    with real_connect(DATABASE_URL) as connection:
        assert connection.execute(
            "SELECT count(*) FROM normalization.products WHERE fingerprint = %s",
            (fingerprint,),
        ).fetchone() == (1,)
