package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Manages database schema initialization for the event store table.
 * <p>
 * Handles table creation, index creation, and optional column comments.
 * Supports auto-detection of the database dialect and UUID storage strategy,
 * and graceful handling of concurrent schema initialization across multiple instances.
 * <p>
 * The DDL is idempotent where the database supports {@code IF NOT EXISTS}; on the
 * other databases a concurrent initialization is detected by re-checking the
 * table after a failed {@code CREATE TABLE}. Index creation is also tolerant to
 * objects created by a rival instance.
 * <p>
 * This class is package-private and used exclusively by {@link JdbcEventStore}.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class SchemaInitializer {

    private static final Logger log = LoggerFactory.getLogger(SchemaInitializer.class);

    private static final String COMMENT_ON_COLUMNS = """
            COMMENT ON TABLE %s IS 'Event store for persistent event lifecycle tracking';
            COMMENT ON COLUMN %s.event_id IS 'Unique event identifier';
            COMMENT ON COLUMN %s.event_type IS 'Event class name';
            COMMENT ON COLUMN %s.service IS 'Service that published the event';
            COMMENT ON COLUMN %s.payload IS 'JSON-serialized event body';
            COMMENT ON COLUMN %s.channels IS 'Comma-separated channel class names (local routing metadata for retry)';
            COMMENT ON COLUMN %s.process_id IS 'Correlation or process identifier';
            COMMENT ON COLUMN %s.status IS 'Lifecycle status: U=UNDEFINED, N=NEW, P=PUBLISHED, A=ACCEPTED, H=HANDLED, F=FAILED, D=DEAD';
            COMMENT ON COLUMN %s.retry_count IS 'Number of retry attempts for failed events';
            COMMENT ON COLUMN %s.retry IS 'Manual retry flag: when TRUE, the event is eligible for retry regardless of lifecycle status';
            COMMENT ON COLUMN %s.created_at IS 'Timestamp when the event was first stored';
            COMMENT ON COLUMN %s.updated_at IS 'Timestamp of the last status update';
            COMMENT ON COLUMN %s.error_details IS 'Error description for failed events';
            """;

    private static final String INDEX_NAME = "idx_%s_status";

    private static final String INDEX_SERVICE_NAME = "idx_%s_service";

    private final DataSource dataSource;
    private final String tableName;
    private final SqlDialect sqlDialect;
    private final UuidType uuidType;

    /**
     * Creates a new SchemaInitializer.
     *
     * @param dataSource the JDBC DataSource
     * @param tableName  the name of the event store table
     * @param sqlDialect the SQL dialect for DDL type mapping
     * @param uuidType   the UUID storage strategy
     */
    public SchemaInitializer(DataSource dataSource, String tableName, SqlDialect sqlDialect, UuidType uuidType) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.sqlDialect = sqlDialect;
        this.uuidType = uuidType;
    }

    /**
     * Ensures the event store table exists, creating it if necessary.
     * <p>
     * If concurrent schema initialization is detected (another instance
     * created the table between our check and DDL execution), the error
     * is silently recovered: the table is re-checked and treated as created.
     *
     * @throws IllegalStateException if schema initialization fails and was not caused
     *                               by concurrent table creation
     */
    public void ensureSchema() {
        try (Connection conn = dataSource.getConnection()) {
            if (SchemaSupport.tableExists(conn, tableName)) {
                log.debug("Event store table '{}' already exists, skipping schema creation", tableName);
                return;
            }
            String baseName = SchemaSupport.tableBase(tableName);
            String createTableSql = buildCreateTableSql(sqlDialect, uuidType).formatted(tableName);
            String indexKeyword = sqlDialect.supportsIfNotExistsForIndex() ? "IF NOT EXISTS " : "";
            String statusIndexName = INDEX_NAME.formatted(baseName);
            String serviceIndexName = INDEX_SERVICE_NAME.formatted(baseName);
            String createStatusIndexSql = "CREATE INDEX " + indexKeyword + statusIndexName
                    + " ON " + tableName + "(status, updated_at)";
            String createServiceIndexSql = "CREATE INDEX " + indexKeyword + serviceIndexName
                    + " ON " + tableName + "(service)";
            conn.setAutoCommit(false);
            try {
                SchemaSupport.executeDdl(conn, tableName, createTableSql,
                        List.of(new SchemaSupport.IndexStatement(createStatusIndexSql, statusIndexName),
                                new SchemaSupport.IndexStatement(createServiceIndexSql, serviceIndexName)), log);
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
            if (!SchemaSupport.tableExists(conn, tableName)) {
                throw new IllegalStateException(
                        "Event store table '" + tableName + "' does not exist. " +
                        "Either set auto-init-schema: true to allow automatic table creation, " +
                        "or create the table manually using the dialect-specific DDL script " +
                        "at: io/github/vovten/eventflow/lifecycle/store/db/event-store-<dialect>.sql");
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
                tableName, tableName, tableName
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

    private String buildCreateTableSql(SqlDialect sqlDialect, UuidType uuidType) {
        String tableKeyword = sqlDialect.supportsIfNotExistsForTable() ? "IF NOT EXISTS " : "";
        String uuidDdl = sqlDialect.uuidDdlType(uuidType);
        String textDdl = sqlDialect.textType();
        String tsDdl = sqlDialect.timestampType();
        return """
                CREATE TABLE %s%%s (
                    event_id        %s PRIMARY KEY,
                    event_type      VARCHAR(512) NOT NULL,
                    service         VARCHAR(255),
                    status          CHAR(1) NOT NULL DEFAULT 'U',
                    payload         %s NOT NULL,
                    channels        %s,
                    process_id      %s,
                    created_at      %s NOT NULL,
                    updated_at      %s NOT NULL,
                    retry_count     INT DEFAULT 0 NOT NULL,
                    retry           %s DEFAULT %s NOT NULL,
                    error_details   %s
                )
                """.formatted(tableKeyword, uuidDdl, textDdl, textDdl, uuidDdl, tsDdl, tsDdl,
                sqlDialect.booleanType(), sqlDialect.booleanLiteral(false), textDdl);
    }
}
