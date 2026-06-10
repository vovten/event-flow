-- ============================================================================
-- Event Store DDL — Microsoft SQL Server 2012+
-- ============================================================================
-- To disable automatic schema initialization via application code, set:
--   event-flow.publisher.lifecycle.store.auto-init-schema: false
-- and manage this DDL through Flyway, Liquibase, or your regular migration
-- tooling.
-- ============================================================================
-- Notes:
--   - UUID is stored as BINARY(16) and converted via application code.
--     For a human-readable alternative use UNIQUEIDENTIFIER.
--   - Large text columns use NVARCHAR(MAX) (nvarchar(max)).
--   - TIMESTAMP is a reserved word in SQL Server (row version type);
--     the application uses DATETIME2 instead.
--   - Column comments use the sp_addextendedproperty system procedure.
-- ============================================================================

CREATE TABLE event_store (
    event_id        BINARY(16) PRIMARY KEY,
    event_type      NVARCHAR(512) NOT NULL,
    service         NVARCHAR(255),
    status          CHAR(1) NOT NULL DEFAULT 'U',
    payload         NVARCHAR(MAX) NOT NULL,
    process_id      BINARY(16),
    created_at      DATETIME2 NOT NULL,
    updated_at      DATETIME2 NOT NULL,
    retry_count     INT DEFAULT 0 NOT NULL,
    error_details   NVARCHAR(MAX)
);

CREATE INDEX idx_event_store_status
    ON event_store(status, updated_at);

CREATE INDEX idx_event_store_service
    ON event_store(service);

-- ============================================================================
-- Extended properties (column/table comments for SQL Server)
-- Adjust the schema name (@level0name) if it differs from 'dbo'.
-- ============================================================================
EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Event store for persistent event lifecycle tracking',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Unique event identifier',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'event_id';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Simple event class name (for display and queries)',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'event_type';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Originating service name for service-specific queries',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'service';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'JSON-serialized event body',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'payload';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Correlation or process identifier',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'process_id';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, H=HANDLED, F=FAILED',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'status';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Number of retry attempts for failed events',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'retry_count';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Timestamp when the event was first stored',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'created_at';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Timestamp of the last status update',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'updated_at';

EXEC sys.sp_addextendedproperty
    @name=N'MS_Description', @value=N'Error description for failed events',
    @level0type=N'SCHEMA', @level0name=N'dbo', @level1type=N'TABLE', @level1name=N'event_store',
    @level2type=N'COLUMN', @level2name=N'error_details';
