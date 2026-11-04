-- ============================================================================
-- Durable Retry Queue DDL — Microsoft SQL Server 2012+
-- ============================================================================
-- UUID is stored as BINARY(16) and converted by application code.
-- TIMESTAMP is a reserved word in SQL Server; DATETIME2 is used instead.

CREATE TABLE event_retry (
    event_id        BINARY(16) NOT NULL,
    service         NVARCHAR(255) NOT NULL,
    retry_kind      NVARCHAR(16) NOT NULL,
    attempt         INT NOT NULL,
    state           NVARCHAR(16) NOT NULL,
    reason          NVARCHAR(64) NOT NULL,
    next_attempt_at DATETIME2 NOT NULL,
    last_error_type NVARCHAR(512),
    last_error      NVARCHAR(MAX),
    policy_name     NVARCHAR(255),
    claimed_by      NVARCHAR(255),
    claimed_until   DATETIME2,
    created_at      DATETIME2 NOT NULL,
    updated_at      DATETIME2 NOT NULL,
    PRIMARY KEY (event_id, retry_kind)
);

CREATE INDEX idx_event_retry_due
    ON event_retry (retry_kind, state, next_attempt_at);

CREATE INDEX idx_event_retry_service_due
    ON event_retry (service, retry_kind, state, next_attempt_at);
