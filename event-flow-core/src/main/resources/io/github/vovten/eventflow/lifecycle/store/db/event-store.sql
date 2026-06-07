-- ============================================================================
-- Event Store schema — for use with Flyway, Liquibase, or manual setup.
--
-- This script is shipped as a classpath resource. To disable automatic
-- schema initialization via application code, set:
--   event-flow.publisher.persistent.auto-init-schema: false
-- and manage this DDL through your regular migration tooling.
--
-- NOTE: The UUID column types below are for PostgreSQL (native UUID).
-- For other databases:
--   MySQL / SQL Server → BINARY(16)
--   Oracle             → RAW(16)
--   SQLite             → BLOB
-- The application auto-detects the database and adjusts DDL accordingly
-- when auto-init-schema is enabled.
-- ============================================================================

CREATE TABLE IF NOT EXISTS event_store (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(512) NOT NULL,
    service         VARCHAR(255),
    status          CHAR(1) NOT NULL DEFAULT 'U',
    payload         TEXT NOT NULL,
    process_id      UUID,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    retry_count     INT DEFAULT 0 NOT NULL,
    error_details   TEXT
);

CREATE INDEX IF NOT EXISTS idx_event_store_status
    ON event_store(status, updated_at);

COMMENT ON TABLE event_store IS 'Event store for persistent event lifecycle tracking';
COMMENT ON COLUMN event_store.event_id IS 'Unique event identifier';
COMMENT ON COLUMN event_store.event_type IS 'Simple event class name (for display and queries)';
COMMENT ON COLUMN event_store.service IS 'Originating service name for service-specific queries';
COMMENT ON COLUMN event_store.payload IS 'JSON-serialized event body';
COMMENT ON COLUMN event_store.process_id IS 'Correlation or process identifier';
COMMENT ON COLUMN event_store.status IS 'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, H=HANDLED, F=FAILED';
COMMENT ON COLUMN event_store.retry_count IS 'Number of retry attempts for failed events';
COMMENT ON COLUMN event_store.created_at IS 'Timestamp when the event was first stored';
COMMENT ON COLUMN event_store.updated_at IS 'Timestamp of the last status update';
COMMENT ON COLUMN event_store.error_details IS 'Error description for failed events';
