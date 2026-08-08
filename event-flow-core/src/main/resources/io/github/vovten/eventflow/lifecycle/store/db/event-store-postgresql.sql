-- ============================================================================
-- Event Store DDL — PostgreSQL (also CockroachDB, YugabyteDB)
-- ============================================================================
-- To disable automatic schema initialization via application code, set:
--   event-flow.publisher.lifecycle.store.auto-init-schema: false
-- and manage this DDL through Flyway, Liquibase, or your regular migration
-- tooling.
-- ============================================================================

CREATE TABLE IF NOT EXISTS event_store (
    event_id        UUID PRIMARY KEY,
    event_type      VARCHAR(512) NOT NULL,
    service         VARCHAR(255),
    status          CHAR(1) NOT NULL DEFAULT 'U',
    payload         TEXT NOT NULL,
    channels        TEXT,
    process_id      UUID,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    retry_count     INT DEFAULT 0 NOT NULL,
    retry           BOOLEAN DEFAULT FALSE NOT NULL,
    error_details   TEXT
);

CREATE INDEX IF NOT EXISTS idx_event_store_status
    ON event_store(status, updated_at);

CREATE INDEX IF NOT EXISTS idx_event_store_service
    ON event_store(service);

COMMENT ON TABLE event_store IS 'Event store for persistent event lifecycle tracking';
COMMENT ON COLUMN event_store.event_id IS 'Unique event identifier';
COMMENT ON COLUMN event_store.event_type IS 'Simple event class name (for display and queries)';
COMMENT ON COLUMN event_store.service IS 'Service that published the event';
COMMENT ON COLUMN event_store.payload IS 'JSON-serialized event body';
COMMENT ON COLUMN event_store.channels IS 'Comma-separated channel class names (local routing metadata for retry)';
COMMENT ON COLUMN event_store.process_id IS 'Correlation or process identifier';
COMMENT ON COLUMN event_store.status IS 'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, H=HANDLED, F=FAILED';
COMMENT ON COLUMN event_store.retry_count IS 'Number of retry attempts for failed events';
COMMENT ON COLUMN event_store.retry IS 'Manual retry flag: when TRUE, the event is eligible for retry regardless of lifecycle status';
COMMENT ON COLUMN event_store.created_at IS 'Timestamp when the event was first stored';
COMMENT ON COLUMN event_store.updated_at IS 'Timestamp of the last status update';
COMMENT ON COLUMN event_store.error_details IS 'Error description for failed events';
