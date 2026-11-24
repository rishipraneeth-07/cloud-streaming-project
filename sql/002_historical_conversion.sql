CREATE TABLE IF NOT EXISTS historical_conversion_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_bucket TEXT NOT NULL,
    source_key TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    target_bucket TEXT NOT NULL,
    target_key TEXT NOT NULL,
    target_sha256 TEXT,
    row_count BIGINT,
    schema_json JSONB,
    status TEXT NOT NULL CHECK (status IN ('SUCCESS', 'FAILED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source_bucket, source_key, source_sha256)
);
