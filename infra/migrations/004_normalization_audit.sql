BEGIN;

CREATE TABLE IF NOT EXISTS normalization.normalization_audit (
    event_id uuid PRIMARY KEY,
    source text NOT NULL,
    source_deal_id text NOT NULL,
    normalization_method text NOT NULL
        CHECK (normalization_method IN ('alias', 'vector', 'llm')),
    confidence numeric NOT NULL,
    embedding_model text NOT NULL,
    embedding_version integer NOT NULL,
    llm_model text,
    prompt_version text,
    catalog_version integer NOT NULL,
    normalized_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS normalization_audit_source_deal_idx
    ON normalization.normalization_audit (source, source_deal_id);

COMMIT;
