-- ============================================================================
-- Durable Retry Queue DDL — MySQL 8+ / MariaDB 10+
-- ============================================================================
-- UUID is stored as BINARY(16) and converted by application code.

CREATE TABLE IF NOT EXISTS event_retry (
    event_id        BINARY(16) NOT NULL,
    service         VARCHAR(255) NOT NULL,
    retry_kind      VARCHAR(16) NOT NULL,
    attempt         INT NOT NULL,
    state           VARCHAR(16) NOT NULL,
    reason          VARCHAR(64) NOT NULL,
    next_attempt_at TIMESTAMP NOT NULL,
    last_error_type VARCHAR(512),
    last_error      TEXT,
    policy_name     VARCHAR(255),
    claimed_by      VARCHAR(255),
    claimed_until   TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, retry_kind)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Durable retry queue for lifecycle events';

-- MySQL does not support IF NOT EXISTS for indexes.
CREATE INDEX idx_event_retry_due
    ON event_retry (retry_kind, state, next_attempt_at);

CREATE INDEX idx_event_retry_service_due
    ON event_retry (service, retry_kind, state, next_attempt_at);
