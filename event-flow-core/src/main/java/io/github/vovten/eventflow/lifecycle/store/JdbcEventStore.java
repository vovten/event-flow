package io.github.vovten.eventflow.lifecycle.store;

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
import java.nio.ByteBuffer;
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
 * {@code io/github/vovten/eventflow/lifecycle/store/event-store.sql}.
 * <p>
 * The {@code event_id} and {@code process_id} column types adapt to the database:
 * <ul>
 *   <li><b>PostgreSQL / H2</b> — native {@code UUID} type</li>
 *   <li><b>Other databases</b> (MySQL, Oracle, SQL Server, SQLite, etc.) — {@code BINARY(16)}</li>
 * </ul>
 * Detection is automatic via {@link DatabaseMetaData#getDatabaseProductName()}.
 * <p>
 * The {@code status} column uses {@code CHAR(1)} with single-character codes (U, N, P, H, F)
 * defined in {@link EventStatus}.
 * The {@code payload} column uses {@code TEXT} for JSON-serialized event data.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class JdbcEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventStore.class);

    private static final String COMMENT_ON_COLUMNS = """
            COMMENT ON TABLE %s IS 'Event store for persistent event lifecycle tracking';
            COMMENT ON COLUMN %s.event_id IS 'Unique event identifier';
            COMMENT ON COLUMN %s.event_type IS 'Simple event class name (for display and queries)';
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

    /**
     * UUID column storage strategy: {@link #NATIVE} uses DB-native UUID type,
     * {@link #BINARY} uses {@code BINARY(16)} with byte[] JDBC binding.
     */
    enum UuidType {
        NATIVE, BINARY
    }

    private final DataSource dataSource;
    private final String tableName;
    private final UuidType uuidType;
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
     * at {@code io/github/vovten/eventflow/lifecycle/store/event-store.sql} in the classpath.
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
        this.uuidType = detectUuidType();
        if (autoInitSchema) {
            initSchema();
        }
    }

    /**
     * Creates a JdbcEventStore with an explicit UUID storage strategy, bypassing auto-detection.
     * This constructor is package-private for testing purposes.
     *
     * @param dataSource the JDBC DataSource (must not be null)
     * @param tableName  the name of the event store table (must not be null)
     * @param uuidType   the UUID strategy to use (NATIVE or BINARY)
     */
    JdbcEventStore(DataSource dataSource, String tableName, UuidType uuidType) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.insertSql = INSERT.formatted(tableName);
        this.selectByStatusSql = SELECT_BY_STATUS.formatted(tableName);
        this.selectByIdSql = SELECT_BY_ID.formatted(tableName);
        this.updateStatusOnlySql = UPDATE_STATUS_ONLY.formatted(tableName);
        this.updateStatusWithRetrySql = UPDATE_STATUS_WITH_RETRY.formatted(tableName);
        this.uuidType = Objects.requireNonNull(uuidType, "uuidType must not be null");
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection()) {
            if (!tableExists(conn, tableName)) {
                String ddl = buildCreateTableSql(uuidType).formatted(tableName);
                String createIndexSql = CREATE_INDEX.formatted(INDEX_NAME.formatted(tableBase(tableName)), tableName);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(ddl);
                    stmt.execute(createIndexSql);
                    addColumnComments(stmt);
                    log.info("Event store schema initialized for table '{}' with UUID strategy {}",
                            tableName, uuidType);
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

    private static String buildCreateTableSql(UuidType type) {
        String uuidDdl = type == UuidType.NATIVE ? "UUID" : "BINARY(16)";
        return """
                CREATE TABLE %%s (
                    event_id        %s PRIMARY KEY,
                    event_type      VARCHAR(512) NOT NULL,
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

    private UuidType detectUuidType() {
        try (Connection conn = dataSource.getConnection()) {
            return detectUuidType(conn);
        } catch (SQLException e) {
            log.warn("Failed to detect database type, defaulting to BINARY(16) for UUID columns", e);
            return UuidType.BINARY;
        }
    }

    private static UuidType detectUuidType(Connection conn) throws SQLException {
        String name = conn.getMetaData().getDatabaseProductName().toLowerCase();
        if (name.contains("postgresql") || name.contains("h2")) {
            return UuidType.NATIVE;
        }
        return UuidType.BINARY;
    }

    private static byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    private static UUID bytesToUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }

    private void setUuid(PreparedStatement ps, int index, UUID uuid) throws SQLException {
        if (uuidType == UuidType.NATIVE) {
            ps.setObject(index, uuid);
        } else {
            ps.setBytes(index, uuidToBytes(uuid));
        }
    }

    private void setUuidNullable(PreparedStatement ps, int index, UUID uuid) throws SQLException {
        if (uuid != null) {
            setUuid(ps, index, uuid);
        } else {
            ps.setNull(index, uuidType == UuidType.NATIVE ? Types.OTHER : Types.BINARY);
        }
    }

    private UUID getUuid(ResultSet rs, String column) throws SQLException {
        if (uuidType == UuidType.NATIVE) {
            return rs.getObject(column, UUID.class);
        }
        byte[] bytes = rs.getBytes(column);
        return bytes != null ? bytesToUuid(bytes) : null;
    }

    private String tableBase(String fullName) {
        int dot = fullName.indexOf('.');
        return dot > 0 ? fullName.substring(dot + 1) : fullName;
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

    @Override
    public void save(StoredEvent event) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(insertSql)) {
            setUuid(ps, 1, event.eventId());
            ps.setString(2, event.eventType());
            ps.setString(3, event.payload());
            setUuidNullable(ps, 4, event.processId());
            ps.setString(5, String.valueOf(event.status().getCode()));
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
            ps.setString(1, String.valueOf(status.getCode()));
            if (errorDetails != null) {
                ps.setString(2, errorDetails);
            } else {
                ps.setNull(2, Types.VARCHAR);
            }
            ps.setTimestamp(3, Timestamp.from(Instant.now()));
            setUuid(ps, 4, eventId);
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
            ps.setString(1, String.valueOf(status.getCode()));
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
            setUuid(ps, 1, eventId);
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
        return new StoredEvent(
                getUuid(rs, "event_id"),
                rs.getString("event_type"),
                rs.getString("payload"),
                getUuid(rs, "process_id"),
                EventStatus.fromCode(rs.getString("status").charAt(0)),
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
