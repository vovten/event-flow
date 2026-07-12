-- ============================================================================
-- Event Store DDL — Oracle 12c+
-- ============================================================================
-- To disable automatic schema initialization via application code, set:
--   event-flow.publisher.lifecycle.store.auto-init-schema: false
-- and manage this DDL through Flyway, Liquibase, or your regular migration
-- tooling.
-- ============================================================================
-- Notes:
--   - UUID is stored as RAW(16) and converted via application code.
--   - LOB columns use CLOB for large text payloads.
--   - COMMENT ON COLUMN syntax is identical to PostgreSQL.
-- ============================================================================

CREATE TABLE event_store (
    event_id        RAW(16) PRIMARY KEY,
    event_type      VARCHAR2(512) NOT NULL,
    service         VARCHAR2(255),
    status          CHAR(1) NOT NULL DEFAULT 'U',
    payload         CLOB NOT NULL,
    process_id      RAW(16),
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    retry_count     INTEGER DEFAULT 0 NOT NULL,
    retry           NUMBER(1) DEFAULT 0 NOT NULL,
    error_details   CLOB
);

CREATE INDEX idx_event_store_status
    ON event_store(status, updated_at);

CREATE INDEX idx_event_store_service
    ON event_store(service);

COMMENT ON TABLE event_store IS 'Event store for persistent event lifecycle tracking';
COMMENT ON COLUMN event_store.event_id IS 'Unique event identifier';
COMMENT ON COLUMN event_store.event_type IS 'Simple event class name (for display and queries)';
COMMENT ON COLUMN event_store.service IS 'Service that published the event';
COMMENT ON COLUMN event_store.payload IS 'JSON-serialized event body';
COMMENT ON COLUMN event_store.process_id IS 'Correlation or process identifier';
COMMENT ON COLUMN event_store.status IS 'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, H=HANDLED, F=FAILED';
COMMENT ON COLUMN event_store.retry_count IS 'Number of retry attempts for failed events';
COMMENT ON COLUMN event_store.retry IS 'Manual retry flag: when TRUE, the event is eligible for retry regardless of lifecycle status';
COMMENT ON COLUMN event_store.created_at IS 'Timestamp when the event was first stored';
COMMENT ON COLUMN event_store.updated_at IS 'Timestamp of the last status update';
COMMENT ON COLUMN event_store.error_details IS 'Error description for failed events';
