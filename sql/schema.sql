
CREATE TABLE IF NOT EXISTS object_metadata (
    object_key TEXT PRIMARY KEY,
    original_created_at TIMESTAMPTZ,
    timestamp_source TEXT NOT NULL DEFAULT 'unknown',
    timestamp_is_original_creation_time BOOLEAN NOT NULL DEFAULT FALSE,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_moved_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS migration_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_bucket TEXT NOT NULL,
    source_key TEXT NOT NULL,
    destination_bucket TEXT NOT NULL,
    destination_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('PENDING', 'COMPLETED', 'FAILED')
    ),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    run_type TEXT NOT NULL CHECK (
        run_type IN ('BATCH', 'STREAMING', 'TIERING')
    ),
    status TEXT NOT NULL CHECK (
        status IN ('RUNNING', 'SUCCESS', 'FAILED')
    ),
    records_processed BIGINT NOT NULL DEFAULT 0
        CHECK (records_processed >= 0),
    records_failed BIGINT NOT NULL DEFAULT 0
        CHECK (records_failed >= 0),
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT
);
