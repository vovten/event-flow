package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.DatabaseDialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
 * SQL syntax adapts to the database dialect via {@link SqlDialect}.
 * Each database family has its own {@link SqlDialect} implementation
 * that provides dialect-specific SQL statements and type mappings.
 * <p>
 * The {@code status} column uses {@code CHAR(1)} with single-character codes (U, N, P, H, F)
 * defined in {@link EventStatus}.
 * The {@code payload} column uses {@code TEXT} (or {@code CLOB} / {@code NVARCHAR(MAX)} depending on dialect)
 * for JSON-serialized event data.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class JdbcEventStore implements EventStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventStore.class);

    public static final String DEFAULT_TABLE_NAME = "event_store";

    private final DataSource dataSource;
    private final String tableName;
    private final SqlDialect sqlDialect;
    private final StoredEventMapper mapper;
    private final String insertSql;
    private final String selectByStatusSql;
    private final String selectByIdSql;
    private final String updateStatusOnlySql;
    private final String updateStatusWithRetrySql;
    private final String markForRetrySql;
    private final Map<Set<EventStatus>, String> selectByStatusesCache = new ConcurrentHashMap<>();
    private final Map<Set<EventStatus>, String> selectRetryableEventsCache = new ConcurrentHashMap<>();
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
     * and schema initialization. UUID strategy is derived from the detected dialect.
     */
    public JdbcEventStore(DataSource dataSource, String tableName, boolean autoInitSchema) {
        this(dataSource, tableName, SqlDialect.forDialect(detectDialect(dataSource)),
                SqlDialect.forDialect(detectDialect(dataSource)).uuidType(), autoInitSchema);
    }

    /**
     * Creates a JdbcEventStore with an explicit UUID storage strategy, bypassing auto-detection.
     * Dialect is auto-detected from the DataSource.
     */
    public JdbcEventStore(DataSource dataSource, String tableName, UuidType uuidType) {
        this(dataSource, tableName, SqlDialect.forDialect(detectDialect(dataSource)), uuidType, true);
    }

    /**
     * Creates a JdbcEventStore with explicit dialect and UUID strategy.
     */
    public JdbcEventStore(DataSource dataSource, String tableName, DatabaseDialect dialect, UuidType uuidType) {
        this(dataSource, tableName, SqlDialect.forDialect(dialect), uuidType, true);
    }

    /**
     * Creates a JdbcEventStore with a fully custom SQL dialect and UUID strategy,
     * bypassing dialect auto-detection entirely.
     * <p>
     * Use this constructor to provide a custom {@link SqlDialect} implementation
     * for databases not covered by the built-in dialects, or to override the
     * default strategy (e.g., native UUID for Oracle 23c+).
     *
     * @param dataSource the JDBC DataSource
     * @param tableName  the name of the event store table
     * @param sqlDialect the SQL dialect implementation (must not be null)
     * @param uuidType   the UUID storage strategy (must not be null)
     */
    public JdbcEventStore(DataSource dataSource, String tableName, SqlDialect sqlDialect, UuidType uuidType) {
        this(dataSource, tableName, sqlDialect, uuidType, true);
    }

    private JdbcEventStore(DataSource dataSource, String tableName, SqlDialect sqlDialect,
                           UuidType uuidType, boolean autoInitSchema) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.sqlDialect = Objects.requireNonNull(sqlDialect, "sqlDialect must not be null");
        this.mapper = new StoredEventMapper(Objects.requireNonNull(uuidType, "uuidType must not be null"));
        this.insertSql = sqlDialect.insertStatement().formatted(tableName);
        this.selectByStatusSql = sqlDialect.selectByStatusStatement().formatted(tableName);
        this.selectByIdSql = sqlDialect.selectByIdStatement().formatted(tableName);
        this.updateStatusOnlySql = sqlDialect.updateStatusOnlyStatement().formatted(tableName);
        this.updateStatusWithRetrySql = sqlDialect.updateStatusWithRetryStatement().formatted(tableName);
        this.markForRetrySql = sqlDialect.markForRetryStatement().formatted(tableName);
        var schemaInitializer = new SchemaInitializer(dataSource, tableName, sqlDialect, uuidType);
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

    @Override
    public void save(StoredEvent event) {
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(insertSql)) {
            mapper.setInsertParameters(ps, event);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw mapper.mapSaveError(e, event);
        }
    }

    @Override
    public void markForRetry(UUID eventId) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(markForRetrySql)) {
            mapper.setMarkForRetryParameters(ps, eventId);
            int affected = ps.executeUpdate();
            if (affected == 0) {
                throw new NoSuchElementException("Event not found: " + eventId);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark event for retry: " + eventId, e);
        }
    }

    @Override
    public void updateStatus(UUID eventId, EventStatus status, String errorDetails) {
        String sql = selectUpdateSql(status);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            mapper.setUpdateParameters(ps, eventId, status, errorDetails);
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

    @Override
    public List<StoredEvent> findByStatus(EventStatus status, Instant before) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectByStatusSql)) {
            ps.setString(1, String.valueOf(status.getCode()));
            mapper.setBeforeTimestamp(ps, 2, before);
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
        String sql = selectByStatusesCache.computeIfAbsent(Set.copyOf(statuses), key ->
                sqlDialect.selectByStatusesStatement(key.size()).formatted(tableName));
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            mapper.setStatusCodes(ps, statuses);
            mapper.setBeforeTimestamp(ps, statuses.size() + 1, before);
            ps.setInt(statuses.size() + 2, batchSize);
            return mapResultList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events by statuses: " + statuses, e);
        }
    }

    @Override
    public List<StoredEvent> findRetryableEvents(List<EventStatus> statuses, Instant before, int batchSize, String service) {
        if (statuses.isEmpty() || batchSize <= 0) {
            return List.of();
        }
        Objects.requireNonNull(service, "service must not be null");
        String sql = selectRetryableEventsCache.computeIfAbsent(Set.copyOf(statuses), key ->
                sqlDialect.selectRetryableEventsStatement(key.size()).formatted(tableName));
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            mapper.setStatusCodes(ps, statuses);
            mapper.setBeforeTimestamp(ps, statuses.size() + 1, before);
            ps.setString(statuses.size() + 2, service);
            ps.setInt(statuses.size() + 3, batchSize);
            return mapResultList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events eligible for retry for service: " + service, e);
        }
    }

    @Override
    public int deleteByStatuses(List<EventStatus> statuses, Instant before, int batchSize) {
        if (statuses.isEmpty() || batchSize <= 0) {
            return 0;
        }
        String sql = deleteByStatusesCache.computeIfAbsent(Set.copyOf(statuses), key ->
                sqlDialect.deleteByStatusesStatement(key.size()).formatted(tableName, tableName));
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            mapper.setStatusCodes(ps, statuses);
            mapper.setBeforeTimestamp(ps, statuses.size() + 1, before);
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
        try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(selectByIdSql)) {
            mapper.setUuid(ps, 1, eventId);
            return mapSingleResult(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find event: " + eventId, e);
        }
    }

    private List<StoredEvent> mapResultList(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<StoredEvent> results = new ArrayList<>();
            while (rs.next()) {
                results.add(mapper.mapRow(rs));
            }
            return results;
        }
    }

    private Optional<StoredEvent> mapSingleResult(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return Optional.of(mapper.mapRow(rs));
            }
            return Optional.empty();
        }
    }

}
