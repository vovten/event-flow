-- ============================================================================
-- Event Store DDL — MySQL 8+ / MariaDB 10+
-- ============================================================================
-- To disable automatic schema initialization via application code, set:
--   event-flow.publisher.lifecycle.store.auto-init-schema: false
-- and manage this DDL through Flyway, Liquibase, or your regular migration
-- tooling.
-- ============================================================================
-- Notes:
--   - UUID is stored as BINARY(16) and converted via application code.
--   - If you prefer human-readable CHAR(36), replace BINARY(16) with
--     CHAR(36) and adjust the UUID mapping in JdbcEventStore.
-- ============================================================================

CREATE TABLE IF NOT EXISTS event_store (
    event_id        BINARY(16) PRIMARY KEY COMMENT 'Unique event identifier',
    event_type      VARCHAR(512) NOT NULL COMMENT 'Simple event class name (for display and queries)',
    service         VARCHAR(255) COMMENT 'Originating service name for service-specific queries',
    status          CHAR(1) NOT NULL DEFAULT 'U' COMMENT 'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, H=HANDLED, F=FAILED',
    payload         TEXT NOT NULL COMMENT 'JSON-serialized event body',
    process_id      BINARY(16) COMMENT 'Correlation or process identifier',
    created_at      TIMESTAMP NOT NULL COMMENT 'Timestamp when the event was first stored',
    updated_at      TIMESTAMP NOT NULL COMMENT 'Timestamp of the last status update',
    retry_count     INT DEFAULT 0 NOT NULL COMMENT 'Number of retry attempts for failed events',
    retry           BOOLEAN DEFAULT FALSE NOT NULL COMMENT 'Manual retry flag: when TRUE, the event is eligible for retry regardless of lifecycle status',
    error_details   TEXT COMMENT 'Error description for failed events'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Event store for persistent event lifecycle tracking';

-- Note: MySQL does not support IF NOT EXISTS for indexes.
-- If the index already exists, run: DROP INDEX idx_event_store_status ON event_store;
CREATE INDEX idx_event_store_status
    ON event_store(status, updated_at);

-- If the index already exists, run: DROP INDEX idx_event_store_service ON event_store;
CREATE INDEX idx_event_store_service
    ON event_store(service);
