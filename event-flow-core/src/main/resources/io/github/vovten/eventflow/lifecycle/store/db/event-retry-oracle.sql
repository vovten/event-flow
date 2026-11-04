-- ============================================================================
-- Durable Retry Queue DDL — Oracle 12c+
-- ============================================================================
-- UUID is stored as RAW(16) and converted by application code.

CREATE TABLE event_retry (
    event_id        RAW(16) NOT NULL,
    service         VARCHAR2(255) NOT NULL,
    retry_kind      VARCHAR2(16) NOT NULL,
    attempt         INTEGER NOT NULL,
    state           VARCHAR2(16) NOT NULL,
    reason          VARCHAR2(64) NOT NULL,
    next_attempt_at TIMESTAMP NOT NULL,
    last_error_type VARCHAR2(512),
    last_error      CLOB,
    policy_name     VARCHAR2(255),
    claimed_by      VARCHAR2(255),
    claimed_until   TIMESTAMP,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    PRIMARY KEY (event_id, retry_kind)
);

CREATE INDEX idx_event_retry_due
    ON event_retry (retry_kind, state, next_attempt_at);

CREATE INDEX idx_event_retry_service_due
    ON event_retry (service, retry_kind, state, next_attempt_at);

COMMENT ON TABLE event_retry IS 'Durable retry queue for lifecycle events';
COMMENT ON COLUMN event_retry.event_id IS 'Event identifier';
COMMENT ON COLUMN event_retry.service IS 'Service that owns the retry row';
COMMENT ON COLUMN event_retry.retry_kind IS 'Retry domain: DELIVERY or PROCESSING';
COMMENT ON COLUMN event_retry.attempt IS '1-based attempt number';
COMMENT ON COLUMN event_retry.state IS 'Retry state: PENDING, IN_PROGRESS or DEAD';
COMMENT ON COLUMN event_retry.reason IS 'Reason for the current retry or terminal state';
COMMENT ON COLUMN event_retry.next_attempt_at IS 'Earliest time the retry may be claimed';
COMMENT ON COLUMN event_retry.last_error_type IS 'Last error class name';
COMMENT ON COLUMN event_retry.last_error IS 'Last error message';
COMMENT ON COLUMN event_retry.policy_name IS 'Effective processing policy name';
COMMENT ON COLUMN event_retry.claimed_by IS 'Scheduler instance that owns the row';
COMMENT ON COLUMN event_retry.claimed_until IS 'Lease expiration time';
COMMENT ON COLUMN event_retry.created_at IS 'When the retry row was created';
COMMENT ON COLUMN event_retry.updated_at IS 'When the retry row was last updated';
