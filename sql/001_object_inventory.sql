CREATE TABLE IF NOT EXISTS object_inventory (
    bucket              TEXT NOT NULL,
    object_name         TEXT NOT NULL,
    size_bytes          BIGINT NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    etag                TEXT,
    last_modified       TIMESTAMPTZ,
    inferred_source     TEXT NOT NULL DEFAULT 'unknown',
    current_tier        TEXT NOT NULL,
    compression_codec   TEXT,
    original_size_bytes BIGINT CHECK (original_size_bytes IS NULL OR original_size_bytes >= 0),
    compressed_size_bytes BIGINT CHECK (compressed_size_bytes IS NULL OR compressed_size_bytes >= 0),
    checksum_sha256     TEXT,
    estimated_monthly_cost_usd NUMERIC(18, 8) NOT NULL DEFAULT 0,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (bucket, object_name)
);

CREATE INDEX IF NOT EXISTS idx_object_inventory_tier
    ON object_inventory(current_tier);

CREATE INDEX IF NOT EXISTS idx_object_inventory_last_modified
    ON object_inventory(last_modified);

CREATE TABLE IF NOT EXISTS object_optimization_history (
    id              BIGSERIAL PRIMARY KEY,
    bucket          TEXT NOT NULL,
    object_name     TEXT NOT NULL,
    operation       TEXT NOT NULL CHECK (operation IN ('COMPRESSION_BENCHMARK', 'COMPRESSION_APPLIED', 'TIER_MIGRATION')),
    source_codec    TEXT,
    target_codec    TEXT,
    before_bytes    BIGINT,
    after_bytes     BIGINT,
    bytes_saved     BIGINT,
    status          TEXT NOT NULL,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_object_optimization_object
    ON object_optimization_history(bucket, object_name, created_at DESC);
