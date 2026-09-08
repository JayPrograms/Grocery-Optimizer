BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE SCHEMA IF NOT EXISTS ranking;

CREATE TABLE IF NOT EXISTS ranking.processed_events (
    event_id uuid PRIMARY KEY,
    source_topic text NOT NULL,
    source_partition integer NOT NULL CHECK (source_partition >= 0),
    source_offset bigint NOT NULL CHECK (source_offset >= 0),
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ranking.store_locations (
    store_location_id text PRIMARY KEY,
    chain_name text NOT NULL,
    store_name text NOT NULL,
    address_summary text NOT NULL,
    latitude numeric(9, 6) NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude numeric(9, 6) NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    source_updated_at timestamptz NOT NULL,
    last_event_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ranking.catalog_projection (
    canonical_product_id uuid PRIMARY KEY,
    canonical_name text NOT NULL,
    brand text,
    package_quantity numeric(14, 4) CHECK (package_quantity > 0),
    package_unit text,
    category text,
    search_text text NOT NULL,
    embedding vector NOT NULL,
    embedding_model text NOT NULL,
    embedding_version text NOT NULL,
    catalog_version integer NOT NULL CHECK (catalog_version >= 1),
    last_event_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((package_quantity IS NULL) = (package_unit IS NULL))
);

CREATE TABLE IF NOT EXISTS ranking.deal_projection (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    source text NOT NULL,
    store_location_id text NOT NULL REFERENCES ranking.store_locations(store_location_id),
    source_deal_id text NOT NULL,
    canonical_product_id uuid NOT NULL REFERENCES ranking.catalog_projection(canonical_product_id),
    source_updated_at timestamptz NOT NULL,
    last_event_id uuid NOT NULL,
    deal_price numeric(14, 4) NOT NULL CHECK (deal_price >= 0),
    regular_price numeric(14, 4) CHECK (regular_price >= 0),
    currency char(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    package_quantity numeric(14, 4) CHECK (package_quantity > 0),
    package_unit text,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL,
    normalization_method text NOT NULL CHECK (normalization_method IN ('alias', 'vector', 'llm')),
    normalization_confidence numeric(6, 5) NOT NULL CHECK (normalization_confidence BETWEEN 0 AND 1),
    embedding_model text NOT NULL,
    embedding_version text NOT NULL,
    llm_model text,
    prompt_version text,
    catalog_version integer NOT NULL CHECK (catalog_version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, store_location_id, source_deal_id),
    CHECK (valid_to > valid_from),
    CHECK ((package_quantity IS NULL) = (package_unit IS NULL)),
    CHECK (
        (normalization_method = 'llm' AND llm_model IS NOT NULL AND prompt_version IS NOT NULL)
        OR (normalization_method <> 'llm' AND llm_model IS NULL AND prompt_version IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS deal_projection_active_idx
    ON ranking.deal_projection (valid_to, valid_from);
CREATE INDEX IF NOT EXISTS deal_projection_store_active_idx
    ON ranking.deal_projection (store_location_id, valid_to, valid_from);
CREATE INDEX IF NOT EXISTS deal_projection_product_active_idx
    ON ranking.deal_projection (canonical_product_id, valid_to, valid_from);

COMMIT;
