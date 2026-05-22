package io.github.vovten.eventflow.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC implementation of {@link EventStore} backed by a relational database.
 * <p>
 * Optionally executes DDL on construction to ensure the event store table exists.
 * Designed to work with any {@link DataSource} (PostgreSQL, H2, MySQL, etc.).
 * <p>
 * The table name defaults to {@value #DEFAULT_TABLE_NAME} and can be customized
 * via the {@link #JdbcEventStore(DataSource, String)} constructor.
 * Automatic schema initialization can be disabled via the
 * {@link #JdbcEventStore(DataSource, String, boolean)} constructor.
 * A ready-to-use DDL script is available as a classpath resource at
 * {@code io/github/vovten/eventflow/store/event-store.sql}.
 * <p>
 * The {@code status} column uses {@code SMALLINT} with codes defined in {@link EventStatus}.
 * The {@code payload} column uses {@code TEXT} for JSON-serialized event data.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class JdbcEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventStore.class);

    private static final String CREATE_TABLE = """
            CREATE TABLE %s (
                event_id        UUID PRIMARY KEY,
                event_type      VARCHAR(512) NOT NULL,
                payload         TEXT NOT NULL,
                process_id      UUID,
                status          SMALLINT NOT NULL DEFAULT 0,
                retry_count     INT DEFAULT 0 NOT NULL,
                created_at      TIMESTAMP NOT NULL,
                updated_at      TIMESTAMP NOT NULL,
                error_details   TEXT
            )
            """;

    private static final String COMMENT_ON_COLUMNS = """
            COMMENT ON TABLE %s IS 'Event store for persistent event lifecycle tracking';
            COMMENT ON COLUMN %s.event_id IS 'Unique event identifier';
            COMMENT ON COLUMN %s.event_type IS 'Fully qualified event class name';
            COMMENT ON COLUMN %s.payload IS 'JSON-serialized event body';
            COMMENT ON COLUMN %s.process_id IS 'Correlation or process identifier';
            COMMENT ON COLUMN %s.status IS 'Lifecycle status: 0=NEW, 1=PUBLISHED, 2=HANDLED, 3=PUBLISH_FAILED, 4=HANDLE_FAILED';
            COMMENT ON COLUMN %s.retry_count IS 'Number of retry attempts for failed events';
            COMMENT ON COLUMN %s.created_at IS 'Timestamp when the event was first stored';
            COMMENT ON COLUMN %s.updated_at IS 'Timestamp of the last status update';
            COMMENT ON COLUMN %s.error_details IS 'Error description for failed events';
            """;

    private static final String CREATE_INDEX = """
            CREATE INDEX %s
            ON %s(status, updated_at)
            """;

    private static final String INSERT = """
            INSERT INTO %s
                (event_id, event_type, payload, process_id, status,
                 retry_count, created_at, updated_at, error_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_STATUS = """
            SELECT event_id, event_type, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE status = ? AND updated_at < ?
            ORDER BY updated_at ASC
            """;

    private static final String SELECT_BY_ID = """
            SELECT event_id, event_type, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE event_id = ?
            """;

    private static final String UPDATE_STATUS_ONLY =
            "UPDATE %s SET status = ?, error_details = ?, updated_at = ? WHERE event_id = ?";

    private static final String UPDATE_STATUS_WITH_RETRY =
            "UPDATE %s SET status = ?, error_details = ?, updated_at = ?, retry_count = retry_count + 1 WHERE event_id = ?";

    private static final String INDEX_NAME = "idx_%s_status";

    static final String DEFAULT_TABLE_NAME = "event_store";

    private final DataSource dataSource;
    private final String tableName;
    private final String insertSql;
    private final String selectByStatusSql;
    private final String selectByIdSql;
    private final String updateStatusOnlySql;
    private final String updateStatusWithRetrySql;

    /**
     * Creates a new JdbcEventStore with the default table name {@value #DEFAULT_TABLE_NAME}
     * and automatic schema initialization enabled.
     *
     * @param dataSource the JDBC DataSource (must not be null)
     */
    public JdbcEventStore(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE_NAME, true);
    }

    /**
     * Creates a new JdbcEventStore with a custom table name
     * and automatic schema initialization enabled.
     *
     * @param dataSource the JDBC DataSource (must not be null)
     * @param tableName  the name of the event store table (must not be null)
     */
    public JdbcEventStore(DataSource dataSource, String tableName) {
        this(dataSource, tableName, true);
    }

    /**
     * Creates a new JdbcEventStore with full control over table name
     * and schema initialization.
     * <p>
     * When {@code autoInitSchema} is {@code false}, the caller is responsible
     * for creating the table beforehand. A ready-to-use DDL script is available
     * at {@code io/github/vovten/eventflow/store/event-store.sql} in the classpath.
     *
     * @param dataSource     the JDBC DataSource (must not be null)
     * @param tableName      the name of the event store table (must not be null)
     * @param autoInitSchema whether to automatically create the table if it does not exist
     */
    public JdbcEventStore(DataSource dataSource, String tableName, boolean autoInitSchema) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.insertSql = INSERT.formatted(tableName);
        this.selectByStatusSql = SELECT_BY_STATUS.formatted(tableName);
        this.selectByIdSql = SELECT_BY_ID.formatted(tableName);
        this.updateStatusOnlySql = UPDATE_STATUS_ONLY.formatted(tableName);
        this.updateStatusWithRetrySql = UPDATE_STATUS_WITH_RETRY.formatted(tableName);
        if (autoInitSchema) {
            initSchema();
        }
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection()) {
            if (!tableExists(conn, tableName)) {
                String createTableSql = CREATE_TABLE.formatted(tableName);
                String createIndexSql = CREATE_INDEX.formatted(INDEX_NAME.formatted(tableName), tableName);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(createTableSql);
                    stmt.execute(createIndexSql);
                    addColumnComments(stmt);
                    log.info("Event store schema initialized for table '{}'", tableName);
                }
            } else {
                log.debug("Event store table '{}' already exists, skipping schema creation", tableName);
            }
        } catch (SQLException e) {
            if (wasCreatedConcurrently()) {
                log.warn("Table '{}' was created concurrently by another instance, proceeding", tableName);
            } else {
                throw new IllegalStateException(
                        "Failed to initialize event store schema for table '" + tableName + "'", e);
            }
        }
    }

    private boolean wasCreatedConcurrently() {
        try (Connection conn = dataSource.getConnection()) {
            return tableExists(conn, tableName);
        } catch (SQLException e) {
            log.warn("Failed to verify table existence after schema error", e);
            return false;
        }
    }

    private void addColumnComments(Statement stmt) {
        String[] lines = COMMENT_ON_COLUMNS.formatted(
                tableName, tableName, tableName, tableName, tableName,
                tableName, tableName, tableName, tableName, tableName
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

    private static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        // Some databases (e.g., PostgreSQL) store names in lowercase,
        // others (e.g., Oracle) in uppercase. Try both.
        try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
            if (rs.next()) return true;
        }
        try (ResultSet rs = meta.getTables(null, null, tableName.toUpperCase(), null)) {
            if (rs.next()) return true;
        }
        try (ResultSet rs = meta.getTables(null, null, tableName.toLowerCase(), null)) {
            return rs.next();
        }
    }

    @Override
    public void save(StoredEvent event) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setObject(1, event.eventId());
            ps.setString(2, event.eventType());
            ps.setString(3, event.payload());
            if (event.processId() != null) {
                ps.setObject(4, event.processId());
            } else {
                ps.setNull(4, Types.OTHER);
            }
            ps.setInt(5, event.status().getCode());
            ps.setInt(6, event.retryCount());
            ps.setTimestamp(7, Timestamp.from(event.createdAt()));
            ps.setTimestamp(8, Timestamp.from(event.updatedAt()));
            if (event.errorDetails() != null) {
                ps.setString(9, event.errorDetails());
            } else {
                ps.setNull(9, Types.VARCHAR);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            if (isDuplicateKey(e)) {
                throw new IllegalArgumentException(
                        "Event with ID " + event.eventId() + " already exists", e);
            }
            throw new RuntimeException("Failed to save event: " + event.eventId(), e);
        }
    }

    @Override
    public void updateStatus(UUID eventId, EventStatus status, String errorDetails) {
        String sql = (status == EventStatus.NEW) ? updateStatusWithRetrySql : updateStatusOnlySql;
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, status.getCode());
            if (errorDetails != null) {
                ps.setString(2, errorDetails);
            } else {
                ps.setNull(2, Types.VARCHAR);
            }
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            ps.setObject(4, eventId);
            int affected = ps.executeUpdate();
            if (affected == 0) {
                throw new NoSuchElementException("Event not found: " + eventId);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update status for event: " + eventId, e);
        }
    }

    @Override
    public List<StoredEvent> findByStatus(EventStatus status, Instant before) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectByStatusSql)) {
            ps.setInt(1, status.getCode());
            ps.setTimestamp(2, Timestamp.from(before));
            try (ResultSet rs = ps.executeQuery()) {
                List<StoredEvent> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events by status: " + status, e);
        }
    }

    @Override
    public Optional<StoredEvent> findById(UUID eventId) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectByIdSql)) {
            ps.setObject(1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
                return Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find event: " + eventId, e);
        }
    }

    private StoredEvent mapRow(ResultSet rs) throws SQLException {
        UUID processId = rs.getObject("process_id", UUID.class);
        return new StoredEvent(
                rs.getObject("event_id", UUID.class),
                rs.getString("event_type"),
                rs.getString("payload"),
                processId,
                EventStatus.fromCode(rs.getInt("status")),
                rs.getInt("retry_count"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getString("error_details")
        );
    }

    private static boolean isDuplicateKey(SQLException e) {
        String sqlState = e.getSQLState();
        // SQLState 23505 = PostgreSQL unique violation
        // SQLState 23000 = MySQL, H2 unique constraint violation
        return "23505".equals(sqlState) || "23000".equals(sqlState);
    }
}
