BEGIN;

ALTER TABLE ranking.catalog_projection
    ALTER COLUMN embedding TYPE vector(1536)
    USING embedding::vector(1536);

ALTER TABLE ranking.deal_projection
    ALTER COLUMN id DROP DEFAULT;

COMMIT;
