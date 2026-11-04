package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Manages database schema initialization for the {@code event_retry} table.
 * <p>
 * Creates the table, its indexes and best-effort column comments. Supports the
 * same dialect and UUID conventions as the event store schema.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetrySchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(RetrySchemaInitializer.class);

    private static final String COMMENT_ON_COLUMNS = """
            COMMENT ON TABLE %s IS 'Durable retry queue for lifecycle events';
            COMMENT ON COLUMN %s.event_id IS 'Event identifier';
            COMMENT ON COLUMN %s.service IS 'Service that owns the retry row';
            COMMENT ON COLUMN %s.retry_kind IS 'Retry domain: DELIVERY or PROCESSING';
            COMMENT ON COLUMN %s.attempt IS '1-based attempt number';
            COMMENT ON COLUMN %s.state IS 'Retry state: PENDING, IN_PROGRESS or DEAD';
            COMMENT ON COLUMN %s.reason IS 'Reason for the current retry or terminal state';
            COMMENT ON COLUMN %s.next_attempt_at IS 'Earliest time the retry may be claimed';
            COMMENT ON COLUMN %s.last_error_type IS 'Last error class name';
            COMMENT ON COLUMN %s.last_error IS 'Last error message';
            COMMENT ON COLUMN %s.policy_name IS 'Effective processing policy name';
            COMMENT ON COLUMN %s.claimed_by IS 'Scheduler instance that owns the row';
            COMMENT ON COLUMN %s.claimed_until IS 'Lease expiration time';
            COMMENT ON COLUMN %s.created_at IS 'When the retry row was created';
            COMMENT ON COLUMN %s.updated_at IS 'When the retry row was last updated';
            """;

    private final DataSource dataSource;
    private final String tableName;
    private final SqlDialect sqlDialect;
    private final UuidType uuidType;

    /**
     * Creates a new schema initializer.
     *
     * @param dataSource the JDBC DataSource
     * @param tableName  the retry table name
     * @param sqlDialect the SQL dialect
     * @param uuidType   the UUID storage strategy
     */
    public RetrySchemaInitializer(DataSource dataSource, String tableName,
                                  SqlDialect sqlDialect, UuidType uuidType) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.sqlDialect = sqlDialect;
        this.uuidType = uuidType;
    }

    /**
     * Creates the retry table and indexes when they do not exist.
     *
     * @throws IllegalStateException if schema initialization fails
     */
    public void ensureSchema() {
        try (Connection conn = dataSource.getConnection()) {
            if (tableExists(conn, tableName)) {
                log.debug("Retry table '{}' already exists, skipping schema creation", tableName);
                return;
            }
            String baseName = tableBase(tableName);
            String createTable = buildCreateTableSql().formatted(tableName);
            String dueIndex = "CREATE INDEX idx_%s_due ON %s(retry_kind, state, next_attempt_at)"
                    .formatted(baseName, tableName);
            String serviceIndex =
                    "CREATE INDEX idx_%s_service_due ON %s(service, retry_kind, state, next_attempt_at)"
                            .formatted(baseName, tableName);
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(createTable);
                stmt.execute(dueIndex);
                stmt.execute(serviceIndex);
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackError) {
                    log.warn("Failed to rollback DDL for retry table '{}'", tableName, rollbackError);
                }
                throw e;
            } finally {
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                    log.warn("Failed to reset auto-commit for retry table '{}'", tableName, e);
                }
            }
            try (Statement stmt = conn.createStatement()) {
                addColumnComments(stmt);
            } catch (SQLException e) {
                log.warn("Failed to add retry table comments for '{}'", tableName, e);
            }
            log.info("Retry schema initialized for table '{}' with UUID strategy {}", tableName, uuidType);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to initialize retry schema for table '" + tableName + "'", e);
        }
    }

    /**
     * Verifies that the retry table exists.
     *
     * @throws IllegalStateException if the table does not exist
     */
    public void verifyTableExists() {
        try (Connection conn = dataSource.getConnection()) {
            if (!tableExists(conn, tableName)) {
                throw new IllegalStateException(
                        "Retry table '" + tableName + "' does not exist. " +
                                "Either set auto-init-schema: true or create it manually using the " +
                                "dialect-specific event-retry SQL script.");
            }
        } catch (SQLException e) {
            throw new IllegalStateException(
                    "Failed to verify retry table '" + tableName + "' existence", e);
        }
    }

    private String buildCreateTableSql() {
        String uuidDdl = uuidType == UuidType.NATIVE ? "UUID" : "BINARY(16)";
        String textDdl = sqlDialect.textType();
        String tsDdl = sqlDialect.timestampType();
        return """
                CREATE TABLE %%s (
                    event_id        %s NOT NULL,
                    service         VARCHAR(255) NOT NULL,
                    retry_kind      VARCHAR(16) NOT NULL,
                    attempt         INT NOT NULL,
                    state           VARCHAR(16) NOT NULL,
                    reason          VARCHAR(64) NOT NULL,
                    next_attempt_at %s NOT NULL,
                    last_error_type VARCHAR(512),
                    last_error      %s,
                    policy_name     VARCHAR(255),
                    claimed_by      VARCHAR(255),
                    claimed_until   %s,
                    created_at      %s NOT NULL,
                    updated_at      %s NOT NULL,
                    PRIMARY KEY (event_id, retry_kind)
                )
                """.formatted(uuidDdl, tsDdl, textDdl, tsDdl, tsDdl, tsDdl);
    }

    private void addColumnComments(Statement stmt) {
        String[] lines = COMMENT_ON_COLUMNS.formatted(
                tableName, tableName, tableName, tableName, tableName, tableName, tableName,
                tableName, tableName, tableName, tableName, tableName, tableName, tableName,
                tableName
        ).split(";");
        for (String line : lines) {
            String sql = line.trim();
            if (sql.isEmpty()) {
                continue;
            }
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                log.warn("Failed to add retry column comment (not supported by this database): {}",
                        e.getMessage());
            }
        }
    }

    private boolean tableExists(Connection conn, String name) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        String baseName = tableBase(name);
        try (ResultSet rs = meta.getTables(null, null, baseName, null)) {
            if (rs.next()) {
                return true;
            }
        }
        try (ResultSet rs = meta.getTables(null, null, baseName.toUpperCase(), null)) {
            if (rs.next()) {
                return true;
            }
        }
        try (ResultSet rs = meta.getTables(null, null, baseName.toLowerCase(), null)) {
            return rs.next();
        }
    }

    private String tableBase(String fullName) {
        int dot = fullName.indexOf('.');
        return dot > 0 ? fullName.substring(dot + 1) : fullName;
    }
}
