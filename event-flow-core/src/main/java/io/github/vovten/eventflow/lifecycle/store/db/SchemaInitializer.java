package io.github.vovten.eventflow.lifecycle.store.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * Manages database schema initialization for the event store table.
 * <p>
 * Handles table creation, index creation, and optional column comments.
 * Supports auto-detection of the UUID storage strategy and graceful
 * handling of concurrent schema initialization across multiple instances.
 * <p>
 * This class is package-private and used exclusively by {@link JdbcEventStore}.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.2
 */
class SchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    private static final String COMMENT_ON_COLUMNS = """
            COMMENT ON TABLE %s IS 'Event store for persistent event lifecycle tracking';
            COMMENT ON COLUMN %s.event_id IS 'Unique event identifier';
            COMMENT ON COLUMN %s.event_type IS 'Event class name';
            COMMENT ON COLUMN %s.service IS 'Originating service name for service-specific queries';
            COMMENT ON COLUMN %s.payload IS 'JSON-serialized event body';
            COMMENT ON COLUMN %s.process_id IS 'Correlation or process identifier';
            COMMENT ON COLUMN %s.status IS 'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, H=HANDLED, F=FAILED';
            COMMENT ON COLUMN %s.retry_count IS 'Number of retry attempts for failed events';
            COMMENT ON COLUMN %s.created_at IS 'Timestamp when the event was first stored';
            COMMENT ON COLUMN %s.updated_at IS 'Timestamp of the last status update';
            COMMENT ON COLUMN %s.error_details IS 'Error description for failed events';
            """;

    private static final String CREATE_INDEX = """
            CREATE INDEX %s
            ON %s(status, updated_at)
            """;

    private static final String CREATE_INDEX_SERVICE = """
            CREATE INDEX %s
            ON %s(service)
            """;

    private static final String INDEX_NAME = "idx_%s_status";

    private static final String INDEX_SERVICE_NAME = "idx_%s_service";

    private final DataSource dataSource;
    private final String tableName;
    private final UuidType uuidType;

    /**
     * Creates a new SchemaInitializer.
     *
     * @param dataSource the JDBC DataSource
     * @param tableName  the name of the event store table
     * @param uuidType   the UUID storage strategy
     */
    SchemaInitializer(DataSource dataSource, String tableName, UuidType uuidType) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.uuidType = uuidType;
    }

    /**
     * Ensures the event store table exists, creating it if necessary.
     * <p>
     * If concurrent schema initialization is detected (another instance
     * created the table between our check and DDL execution), the error
     * is silently recovered.
     *
     * @throws IllegalStateException if schema initialization fails and was not caused
     *                               by concurrent table creation
     */
    public void ensureSchema() {
        try (Connection conn = dataSource.getConnection()) {
            if (!tableExists(conn, tableName)) {
                String ddl = buildCreateTableSql(uuidType).formatted(tableName);
                String baseName = tableBase(tableName);
                String statusIndexName = INDEX_NAME.formatted(baseName);
                String createStatusIndexSql = CREATE_INDEX.formatted(statusIndexName, tableName);
                String serviceIndexName = INDEX_SERVICE_NAME.formatted(baseName);
                String createServiceIndexSql = CREATE_INDEX_SERVICE.formatted(serviceIndexName, tableName);
                conn.setAutoCommit(false);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                    stmt.execute(createStatusIndexSql);
                    stmt.execute(createServiceIndexSql);
                    conn.commit();
                } catch (SQLException e) {
                    try {
                        conn.rollback();
                    } catch (SQLException ex) {
                        log.warn("Failed to rollback DDL for table '{}'", tableName, ex);
                    }
                    throw e;
                } finally {
                    try {
                        conn.setAutoCommit(true);
                    } catch (SQLException ex) {
                        log.warn("Failed to reset auto-commit for table '{}'", tableName, ex);
                    }
                }
                // Column comments are best-effort metadata outside the DDL transaction
                try (Statement commentStmt = conn.createStatement()) {
                    addColumnComments(commentStmt);
                } catch (SQLException e) {
                    log.warn("Failed to add column comments for table '{}'", tableName, e);
                }
                log.info("Event store schema initialized for table '{}' with UUID strategy {}",
                        tableName, uuidType);
            } else {
                log.debug("Event store table '{}' already exists, skipping schema creation", tableName);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to initialize event store schema for table '" + tableName + "'", e);
        }
    }

    /**
     * Verifies that the event store table exists.
     *
     * @throws IllegalStateException if the table does not exist or verification fails
     */
    public void verifyTableExists() {
        try (Connection conn = dataSource.getConnection()) {
            if (!tableExists(conn, tableName)) {
                throw new IllegalStateException(
                        "Event store table '" + tableName + "' does not exist. " +
                        "Either set auto-init-schema: true to allow automatic table creation, " +
                        "or create the table manually using the DDL script at: " +
                        "io/github/vovten/eventflow/lifecycle/store/event-store.sql");
            }
            log.info("Event store table '{}' exists, auto-init-schema is disabled", tableName);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to verify event store table '" + tableName + "' existence", e);
        }
    }

    private void addColumnComments(Statement stmt) {
        String[] lines = COMMENT_ON_COLUMNS.formatted(
                tableName, tableName, tableName, tableName, tableName,
                tableName, tableName, tableName, tableName, tableName,
                tableName
        ).split(";");
        for (String line : lines) {
            String sql = line.trim();
            if (sql.isEmpty()) continue;
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                log.warn("Failed to add comment (not supported by this database): {}", e.getMessage());
            }
        }
    }

    private static String buildCreateTableSql(UuidType type) {
        String uuidDdl = type == UuidType.NATIVE ? "UUID" : "BINARY(16)";
        return """
                CREATE TABLE %%s (
                    event_id        %s PRIMARY KEY,
                    event_type      VARCHAR(512) NOT NULL,
                    service         VARCHAR(255),
                    status          CHAR(1) NOT NULL DEFAULT 'U',
                    payload         TEXT NOT NULL,
                    process_id      %s,
                    created_at      TIMESTAMP NOT NULL,
                    updated_at      TIMESTAMP NOT NULL,
                    retry_count     INT DEFAULT 0 NOT NULL,
                    error_details   TEXT
                )
                """.formatted(uuidDdl, uuidDdl);
    }

    private boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        String baseName = tableBase(tableName);
        try (ResultSet rs = meta.getTables(null, null, baseName, null)) {
            if (rs.next()) return true;
        }
        try (ResultSet rs = meta.getTables(null, null, baseName.toUpperCase(), null)) {
            if (rs.next()) return true;
        }
        try (ResultSet rs = meta.getTables(null, null, baseName.toLowerCase(), null)) {
            return rs.next();
        }
    }

    private static String tableBase(String fullName) {
        int dot = fullName.indexOf('.');
        return dot > 0 ? fullName.substring(dot + 1) : fullName;
    }
}
