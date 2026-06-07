package io.github.vovten.eventflow.lifecycle.store.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.util.*;

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
 * {@code io/github/vovten/eventflow/lifecycle/store/db/event-store.sql}.
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

    private static final String INSERT = """
            INSERT INTO %s
                (event_id, event_type, service, payload, process_id, status,
                 retry_count, created_at, updated_at, error_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_STATUS = """
            SELECT event_id, event_type, service, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE status = ? AND updated_at < ?
            ORDER BY updated_at ASC
            """;

    private static final String SELECT_BY_STATUSES = """
            SELECT event_id, event_type, service, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE status IN (%s) AND updated_at < ?
            ORDER BY updated_at ASC
            """;

    private static final String SELECT_BY_ID = """
            SELECT event_id, event_type, service, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE event_id = ?
            """;

    private static final String UPDATE_STATUS_ONLY =
            "UPDATE %s SET status = ?, error_details = ?, updated_at = ? WHERE event_id = ?";

    private static final String UPDATE_STATUS_WITH_RETRY =
            "UPDATE %s SET status = ?, error_details = ?, updated_at = ?, retry_count = retry_count + 1 WHERE event_id = ?";

    static final String DEFAULT_TABLE_NAME = "event_store";

    private final DataSource dataSource;
    private final String tableName;
    private final UuidType uuidType;
    private final SchemaInitializer schemaInitializer;
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
        this.schemaInitializer = new SchemaInitializer(dataSource, tableName, uuidType);
        if (autoInitSchema) {
            schemaInitializer.ensureSchema();
        } else {
            schemaInitializer.verifyTableExists();
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
        this.schemaInitializer = new SchemaInitializer(dataSource, tableName, uuidType);
        schemaInitializer.ensureSchema();
    }

    @Override
    public String getType() {
        return "db";
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

    @Override
    public void save(StoredEvent event) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(insertSql)) {
            setUuid(ps, 1, event.eventId());
            ps.setString(2, event.eventType());
            if (event.service() != null) {
                ps.setString(3, event.service());
            } else {
                ps.setNull(3, Types.VARCHAR);
            }
            ps.setString(4, event.payload());
            setUuidNullable(ps, 5, event.processId());
            ps.setString(6, String.valueOf(event.status().getCode()));
            ps.setInt(7, event.retryCount());
            ps.setTimestamp(8, Timestamp.from(event.createdAt()));
            ps.setTimestamp(9, Timestamp.from(event.updatedAt()));
            if (event.errorDetails() != null) {
                ps.setString(10, event.errorDetails());
            } else {
                ps.setNull(10, Types.VARCHAR);
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
    public List<StoredEvent> findByStatuses(List<EventStatus> statuses, Instant before) {
        if (statuses.isEmpty()) {
            return List.of();
        }
        String placeholders = statuses.stream()
                .map(s -> "?")
                .collect(java.util.stream.Collectors.joining(", "));
        String sql = SELECT_BY_STATUSES.formatted(tableName, placeholders);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            int i = 1;
            for (EventStatus status : statuses) {
                ps.setString(i++, String.valueOf(status.getCode()));
            }
            ps.setTimestamp(i, Timestamp.from(before));
            try (ResultSet rs = ps.executeQuery()) {
                List<StoredEvent> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapRow(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events by statuses: " + statuses, e);
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
                rs.getString("service"),
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
