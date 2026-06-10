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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * JDBC implementation of {@link EventStore} backed by a relational database.
 * <p>
 * Optionally executes DDL on construction to ensure the event store table exists.
 * Designed to work with any {@link DataSource} (PostgreSQL, H2, MySQL, Oracle, MS SQL Server, etc.).
 * <p>
 * The table name defaults to {@value #DEFAULT_TABLE_NAME} and can be customized
 * via the {@link #JdbcEventStore(DataSource, String)} constructor.
 * Automatic schema initialization can be disabled via the
 * {@link #JdbcEventStore(DataSource, String, boolean)} constructor.
 * Dialect-specific DDL scripts are available as classpath resources at
 * {@code io/github/vovten/eventflow/lifecycle/store/db/event-store-&lt;dialect&gt;.sql}.
 * <p>
 * The {@code event_id} and {@code process_id} column types adapt to the database:
 * <ul>
 *   <li><b>PostgreSQL / H2</b> — native {@code UUID} type</li>
 *   <li><b>Other databases</b> (MySQL, Oracle, SQL Server, SQLite, etc.) — {@code BINARY(16)}</li>
 * </ul>
 * Detection is automatic via {@link DatabaseMetaData#getDatabaseProductName()}.
 * <p>
 * SQL syntax adapts to the database dialect:
 * <ul>
 *   <li><b>PostgreSQL / MySQL / H2</b> — uses {@code LIMIT ?}</li>
 *   <li><b>Oracle / SQL Server</b> — uses {@code OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY}</li>
 * </ul>
 * <p>
 * The {@code status} column uses {@code CHAR(1)} with single-character codes (U, N, P, H, F)
 * defined in {@link EventStatus}.
 * The {@code payload} column uses {@code TEXT} (or {@code CLOB} / {@code NVARCHAR(MAX)} depending on dialect)
 * for JSON-serialized event data.
 *
 * @author Vladimir Aleshkov
 * @since 1.3.0
 */
public class JdbcEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventStore.class);

    private static final String SQLSTATE_UNIQUE_VIOLATION_POSTGRES = "23505";
    private static final String SQLSTATE_UNIQUE_VIOLATION_MYSQL = "23000";

    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private static final String INSERT = """
            INSERT INTO %s (event_id, event_type, service, payload, process_id,
                            status, retry_count, created_at, updated_at, error_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_STATUS = """
            SELECT event_id, event_type, service, payload, process_id,
                   status, retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE status = ? AND updated_at < ?
            ORDER BY updated_at ASC
            """;

    private static final String SELECT_BY_STATUSES_TEMPLATE = """
            SELECT event_id, event_type, service, payload, process_id,
                   status, retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE status IN (%s) AND updated_at < ?
            ORDER BY updated_at ASC
            %s
            """;

    /**
     * DELETE with subquery — used for PostgreSQL, Oracle, MSSQL, H2.
     * Single-level subquery, no table alias needed.
     */
    private static final String DELETE_BY_STATUSES = """
            DELETE FROM %s
            WHERE event_id IN (
                SELECT event_id
                FROM %s
                WHERE status IN (%s) AND updated_at < ?
                ORDER BY updated_at ASC
                %s
            )
            """;

    /**
     * DELETE with subquery — used for MySQL.
     * MySQL forbids referencing the target table directly in a subquery,
     * so we wrap in another level with an alias.
     */
    private static final String DELETE_BY_STATUSES_MYSQL = """
            DELETE FROM %s
            WHERE event_id IN (
                SELECT event_id FROM (
                    SELECT event_id
                    FROM %s
                    WHERE status IN (%s) AND updated_at < ?
                    ORDER BY updated_at ASC
                    %s
                ) AS cleanup_ids
            )
            """;

    private static final String SELECT_BY_ID = """
            SELECT event_id, event_type, service, payload, process_id,
                   status, retry_count, created_at, updated_at, error_details
            FROM %s
            WHERE event_id = ?
            """;

    private static final String UPDATE_STATUS_ONLY = """
            UPDATE %s
            SET status = ?, error_details = ?, updated_at = ?
            WHERE event_id = ?
            """;

    private static final String UPDATE_STATUS_WITH_RETRY = """
            UPDATE %s
            SET status = ?, error_details = ?, updated_at = ?, retry_count = retry_count + 1
            WHERE event_id = ?
            """;

    static final String DEFAULT_TABLE_NAME = "event_store";

    private final DataSource dataSource;
    private final String tableName;
    private final DatabaseDialect dialect;
    private final UuidType uuidType;
    private final String insertSql;
    private final String selectByStatusSql;
    private final String selectByIdSql;
    private final String updateStatusOnlySql;
    private final String updateStatusWithRetrySql;
    private final Map<Set<EventStatus>, String> selectByStatusesCache = new ConcurrentHashMap<>();
    private final Map<Set<EventStatus>, String> deleteByStatusesCache = new ConcurrentHashMap<>();

    /**
     * Creates a new JdbcEventStore with the default table name {@value #DEFAULT_TABLE_NAME}
     * and automatic schema initialization enabled.
     */
    public JdbcEventStore(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE_NAME, true);
    }

    /**
     * Creates a new JdbcEventStore with a custom table name
     * and automatic schema initialization enabled.
     */
    public JdbcEventStore(DataSource dataSource, String tableName) {
        this(dataSource, tableName, true);
    }

    /**
     * Creates a new JdbcEventStore with full control over table name
     * and schema initialization.
     */
    public JdbcEventStore(DataSource dataSource, String tableName, boolean autoInitSchema) {
        this(dataSource, tableName, detectDialect(dataSource), UuidType.fromDialect(detectDialect(dataSource)), autoInitSchema);
    }

    /**
     * Creates a JdbcEventStore with an explicit UUID storage strategy, bypassing auto-detection.
     * Dialect is auto-detected from the DataSource.
     * Package-private for testing purposes.
     */
    JdbcEventStore(DataSource dataSource, String tableName, UuidType uuidType) {
        this(dataSource, tableName, detectDialect(dataSource), uuidType, true);
    }

    /**
     * Creates a JdbcEventStore with explicit dialect and UUID strategy.
     * Package-private for testing purposes.
     */
    JdbcEventStore(DataSource dataSource, String tableName, DatabaseDialect dialect, UuidType uuidType) {
        this(dataSource, tableName, dialect, uuidType, true);
    }

    private JdbcEventStore(DataSource dataSource, String tableName, DatabaseDialect dialect,
                            UuidType uuidType, boolean autoInitSchema) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.dialect = Objects.requireNonNull(dialect, "dialect must not be null");
        this.uuidType = Objects.requireNonNull(uuidType, "uuidType must not be null");
        this.insertSql = INSERT.formatted(tableName);
        this.selectByStatusSql = SELECT_BY_STATUS.formatted(tableName);
        this.selectByIdSql = SELECT_BY_ID.formatted(tableName);
        this.updateStatusOnlySql = UPDATE_STATUS_ONLY.formatted(tableName);
        this.updateStatusWithRetrySql = UPDATE_STATUS_WITH_RETRY.formatted(tableName);
        var schemaInitializer = new SchemaInitializer(dataSource, tableName, dialect, uuidType);
        if (autoInitSchema) {
            schemaInitializer.ensureSchema();
        } else {
            schemaInitializer.verifyTableExists();
        }
    }

    @Override
    public String getType() {
        return "db";
    }

    private static DatabaseDialect detectDialect(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection()) {
            return DatabaseDialect.detect(conn);
        } catch (SQLException e) {
            log.warn("Failed to detect database dialect, defaulting to PostgreSQL", e);
            return DatabaseDialect.POSTGRESQL;
        }
    }

    private static void setOptionalString(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            ps.setString(index, value);
        } else {
            ps.setNull(index, Types.VARCHAR);
        }
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
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(insertSql)) {
            setInsertParameters(ps, event);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw mapSaveError(e, event);
        }
    }

    private void setInsertParameters(PreparedStatement ps, StoredEvent event) throws SQLException {
        setUuid(ps, 1, event.eventId());
        ps.setString(2, event.eventType());
        setOptionalString(ps, 3, event.service());
        ps.setString(4, event.payload());
        setUuidNullable(ps, 5, event.processId());
        ps.setString(6, String.valueOf(event.status().getCode()));
        ps.setInt(7, event.retryCount());
        ps.setTimestamp(8, Timestamp.from(event.createdAt()), UTC);
        ps.setTimestamp(9, Timestamp.from(event.updatedAt()), UTC);
        setOptionalString(ps, 10, event.errorDetails());
    }

    private RuntimeException mapSaveError(SQLException e, StoredEvent event) {
        if (isDuplicateKey(e)) {
            return new IllegalArgumentException(
                    "Event with ID " + event.eventId() + " already exists", e);
        }
        return new RuntimeException("Failed to save event: " + event.eventId(), e);
    }

    @Override
    public void updateStatus(UUID eventId, EventStatus status, String errorDetails) {
        String sql = selectUpdateSql(status);
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
            setUpdateParameters(ps, eventId, status, errorDetails);
            int affected = ps.executeUpdate();
            if (affected == 0) {
                throw new NoSuchElementException("Event not found: " + eventId);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to update status for event: " + eventId, e);
        }
    }

    private String selectUpdateSql(EventStatus status) {
        return status == EventStatus.NEW ? updateStatusWithRetrySql : updateStatusOnlySql;
    }

    private void setUpdateParameters(PreparedStatement ps, UUID eventId, EventStatus status,
                                      String errorDetails) throws SQLException {
        ps.setString(1, String.valueOf(status.getCode()));
        setOptionalString(ps, 2, errorDetails);
        ps.setTimestamp(3, Timestamp.from(Instant.now()), UTC);
        setUuid(ps, 4, eventId);
    }

    @Override
    public List<StoredEvent> findByStatus(EventStatus status, Instant before) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectByStatusSql)) {
            ps.setString(1, String.valueOf(status.getCode()));
            setBeforeTimestamp(ps, 2, before);
            return mapResultList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events by status: " + status, e);
        }
    }

    @Override
    public List<StoredEvent> findByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
        if (statuses.isEmpty() || batchSize <= 0) {
            return List.of();
        }
        String sql = selectByStatusesCache.computeIfAbsent(Set.copyOf(statuses), key -> {
            String placeholders = key.stream()
                    .map(s -> "?")
                    .collect(Collectors.joining(", "));
            return SELECT_BY_STATUSES_TEMPLATE.formatted(tableName, placeholders, dialect.limitClause());
        });
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            setStatusCodes(ps, statuses);
            setBeforeTimestamp(ps, statuses.size() + 1, before);
            ps.setInt(statuses.size() + 2, batchSize);
            return mapResultList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events by statuses: " + statuses, e);
        }
    }

    private void setStatusCodes(PreparedStatement ps, List<EventStatus> statuses) throws SQLException {
        int i = 1;
        for (EventStatus status : statuses) {
            ps.setString(i++, String.valueOf(status.getCode()));
        }
    }

    private void setBeforeTimestamp(PreparedStatement ps, int index, Instant before) throws SQLException {
        warnIfFutureTimestamp(before);
        ps.setTimestamp(index, Timestamp.from(before), UTC);
    }

    private void warnIfFutureTimestamp(Instant before) {
        if (before.isAfter(Instant.now())) {
            log.warn("Looking for events with future timestamp: {}", before);
        }
    }

    @Override
    public int deleteByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
        if (statuses.isEmpty() || batchSize <= 0) {
            return 0;
        }
        String sql = deleteByStatusesCache.computeIfAbsent(Set.copyOf(statuses), key -> {
            String placeholders = key.stream()
                    .map(s -> "?")
                    .collect(Collectors.joining(", "));
            String template = dialect == DatabaseDialect.MYSQL
                    ? DELETE_BY_STATUSES_MYSQL
                    : DELETE_BY_STATUSES;
            return template.formatted(tableName, tableName, placeholders, dialect.limitClause());
        });
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            setStatusCodes(ps, statuses);
            setBeforeTimestamp(ps, statuses.size() + 1, before);
            ps.setInt(statuses.size() + 2, batchSize);
            int deleted = ps.executeUpdate();
            if (deleted > 0) {
                log.debug("Deleted {} events by statuses: {} (before {})", deleted, statuses, before);
            }
            return deleted;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete events by statuses: " + statuses, e);
        }
    }

    @Override
    public Optional<StoredEvent> findById(UUID eventId) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectByIdSql)) {
            setUuid(ps, 1, eventId);
            return mapSingleResult(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find event: " + eventId, e);
        }
    }

    private List<StoredEvent> mapResultList(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<StoredEvent> results = new ArrayList<>();
            while (rs.next()) {
                results.add(mapRow(rs));
            }
            return results;
        }
    }

    private Optional<StoredEvent> mapSingleResult(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return Optional.of(mapRow(rs));
            }
            return Optional.empty();
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
        if (sqlState == null) {
            return false;
        }
        // SQL standard class "23" covers all integrity constraint violations,
        // including unique constraint violations across all major databases:
        //   - PostgreSQL: SQLSTATE 23505
        //   - MySQL:      SQLSTATE 23000
        //   - Oracle:     SQLSTATE 23000 (ORA-00001)
        //   - SQL Server: SQLSTATE 23000 (error 2627)
        //   - H2:         SQLSTATE 23505
        return sqlState.startsWith("23");
    }
}
