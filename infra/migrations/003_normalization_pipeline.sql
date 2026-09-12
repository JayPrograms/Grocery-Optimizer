BEGIN;

CREATE SCHEMA IF NOT EXISTS normalization;

CREATE TABLE IF NOT EXISTS normalization.processed_events (
    event_id uuid PRIMARY KEY,
    source_topic text NOT NULL,
    source_partition integer NOT NULL CHECK (source_partition >= 0),
    source_offset bigint NOT NULL CHECK (source_offset >= 0),
    processed_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS normalization.products (
    canonical_product_id uuid PRIMARY KEY,
    fingerprint text NOT NULL UNIQUE,
    canonical_name text NOT NULL,
    brand text,
    package_quantity numeric(14, 4) CHECK (package_quantity > 0),
    package_unit text,
    category text,
    search_text text NOT NULL,
    embedding vector(1536) NOT NULL,
    embedding_model text NOT NULL,
    embedding_version text NOT NULL,
    catalog_version integer NOT NULL CHECK (catalog_version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((package_quantity IS NULL) = (package_unit IS NULL))
);

CREATE TABLE IF NOT EXISTS normalization.aliases (
    source text NOT NULL,
    fingerprint text NOT NULL,
    canonical_product_id uuid NOT NULL REFERENCES normalization.products(canonical_product_id),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, fingerprint)
);

CREATE TABLE IF NOT EXISTS normalization.deals (
    id uuid PRIMARY KEY,
    source text NOT NULL,
    store_location_id text NOT NULL,
    source_deal_id text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    last_raw_event_id uuid NOT NULL,
    canonical_product_id uuid NOT NULL REFERENCES normalization.products(canonical_product_id),
    normalized_event jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source, store_location_id, source_deal_id)
);

CREATE TABLE IF NOT EXISTS normalization.outbox (
    event_id uuid PRIMARY KEY,
    aggregate_key text NOT NULL,
    event_value jsonb NOT NULL,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS normalization_outbox_pending_idx
    ON normalization.outbox (created_at) WHERE published_at IS NULL;

COMMIT;
