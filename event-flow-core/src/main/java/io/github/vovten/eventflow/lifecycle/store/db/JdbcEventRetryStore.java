package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.DatabaseDialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import io.github.vovten.eventflow.retry.RetryKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JDBC implementation of {@link EventRetryStore}.
 * <p>
 * The table name defaults to {@value #DEFAULT_TABLE_NAME}. Schema initialization
 * follows the same conventions as {@link JdbcEventStore}.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class JdbcEventRetryStore implements EventRetryStore {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventRetryStore.class);

    /**
     * Default retry table name.
     */
    public static final String DEFAULT_TABLE_NAME = "event_retry";

    private final DataSource dataSource;
    private final String tableName;
    private final RetryTaskMapper mapper;
    private final String insertSql;
    private final String updateSql;
    private final String selectDueSql;
    private final String claimSql;
    private final String rescheduleSql;
    private final String completeSql;
    private final String deadSql;
    private final String selectByIdSql;
    private final SqlDialect sqlDialect;
    private final Map<Integer, String> deleteByEventIdsCache = new ConcurrentHashMap<>();

    /**
     * Creates a store with the default table name and automatic schema initialization.
     *
     * @param dataSource the JDBC DataSource
     */
    public JdbcEventRetryStore(DataSource dataSource) {
        this(dataSource, DEFAULT_TABLE_NAME, true);
    }

    /**
     * Creates a store with a custom table name and automatic schema initialization.
     *
     * @param dataSource the JDBC DataSource
     * @param tableName  the retry table name
     */
    public JdbcEventRetryStore(DataSource dataSource, String tableName) {
        this(dataSource, tableName, true);
    }

    /**
     * Creates a store with full control over table name and schema initialization.
     *
     * @param dataSource     the JDBC DataSource
     * @param tableName      the retry table name
     * @param autoInitSchema whether to create the schema automatically
     */
    public JdbcEventRetryStore(DataSource dataSource, String tableName, boolean autoInitSchema) {
        this(dataSource, tableName, SqlDialect.forDialect(detectDialect(dataSource)),
                SqlDialect.forDialect(detectDialect(dataSource)).uuidType(), autoInitSchema);
    }

    /**
     * Creates a store with explicit dialect and schema initialization.
     *
     * @param dataSource     the JDBC DataSource
     * @param tableName      the retry table name
     * @param dialect        the database dialect
     * @param autoInitSchema whether to create the schema automatically
     */
    public JdbcEventRetryStore(DataSource dataSource, String tableName,
                               DatabaseDialect dialect, boolean autoInitSchema) {
        this(dataSource, tableName, SqlDialect.forDialect(dialect),
                SqlDialect.forDialect(dialect).uuidType(), autoInitSchema);
    }

    /**
     * Creates a store with explicit SQL dialect and UUID type.
     *
     * @param dataSource     the JDBC DataSource
     * @param tableName      the retry table name
     * @param sqlDialect     the SQL dialect
     * @param uuidType       the UUID storage strategy
     * @param autoInitSchema whether to create the schema automatically
     */
    public JdbcEventRetryStore(DataSource dataSource, String tableName,
                               SqlDialect sqlDialect, UuidType uuidType, boolean autoInitSchema) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.tableName = Objects.requireNonNull(tableName, "tableName must not be null");
        this.sqlDialect = Objects.requireNonNull(sqlDialect, "sqlDialect must not be null");
        this.mapper = new RetryTaskMapper(Objects.requireNonNull(uuidType, "uuidType must not be null"));
        this.insertSql = sqlDialect.insertRetryStatement().formatted(tableName);
        this.updateSql = sqlDialect.updateRetryStatement().formatted(tableName);
        this.selectDueSql = sqlDialect.selectDueRetriesStatement().formatted(tableName);
        this.claimSql = sqlDialect.claimRetryStatement().formatted(tableName);
        this.rescheduleSql = sqlDialect.rescheduleRetryStatement().formatted(tableName);
        this.completeSql = sqlDialect.completeRetryStatement().formatted(tableName);
        this.deadSql = sqlDialect.deadRetryStatement().formatted(tableName);
        this.selectByIdSql = sqlDialect.selectRetryByIdStatement().formatted(tableName);
        RetrySchemaInitializer initializer =
                new RetrySchemaInitializer(dataSource, tableName, sqlDialect, uuidType);
        if (autoInitSchema) {
            initializer.ensureSchema();
        } else {
            initializer.verifyTableExists();
        }
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
    public void schedule(RetryTask task) {
        Objects.requireNonNull(task, "task must not be null");
        try (Connection conn = dataSource.getConnection()) {
            int updated;
            try (PreparedStatement ps = conn.prepareStatement(updateSql)) {
                mapper.setUpdateParameters(ps, task);
                updated = ps.executeUpdate();
            }
            if (updated == 0) {
                try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                    mapper.setInsertParameters(ps, task);
                    ps.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to schedule retry for event: " + task.eventId(), e);
        }
    }

    @Override
    public List<RetryTask> findDue(RetryKind retryKind, String service, Instant now, int batchSize) {
        if (batchSize <= 0) {
            return List.of();
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectDueSql)) {
            ps.setString(1, retryKind.name());
            ps.setString(2, service);
            mapper.setTimestamp(ps, 3, now);
            mapper.setTimestamp(ps, 4, now);
            ps.setInt(5, batchSize);
            return mapList(ps);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find due retries for service: " + service, e);
        }
    }

    @Override
    public boolean claim(UUID eventId, RetryKind retryKind, String owner, Instant claimedUntil) {
        Instant now = Instant.now();
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(claimSql)) {
            ps.setString(1, owner);
            mapper.setTimestamp(ps, 2, claimedUntil);
            mapper.setTimestamp(ps, 3, now);
            mapper.setUuid(ps, 4, eventId);
            ps.setString(5, retryKind.name());
            mapper.setTimestamp(ps, 6, now);
            mapper.setTimestamp(ps, 7, now);
            return ps.executeUpdate() > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim retry for event: " + eventId, e);
        }
    }

    @Override
    public void reschedule(UUID eventId, RetryKind retryKind, int attempt, Instant nextAttemptAt,
                           String errorType, String error) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(rescheduleSql)) {
            ps.setInt(1, attempt);
            mapper.setTimestamp(ps, 2, nextAttemptAt);
            mapper.setOptionalString(ps, 3, errorType);
            mapper.setOptionalString(ps, 4, error);
            mapper.setTimestamp(ps, 5, Instant.now());
            mapper.setUuid(ps, 6, eventId);
            ps.setString(7, retryKind.name());
            if (ps.executeUpdate() == 0) {
                throw new NoSuchElementException("Retry task not found: " + eventId + "/" + retryKind);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to reschedule retry for event: " + eventId, e);
        }
    }

    @Override
    public void complete(UUID eventId, RetryKind retryKind) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(completeSql)) {
            mapper.setUuid(ps, 1, eventId);
            ps.setString(2, retryKind.name());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to complete retry for event: " + eventId, e);
        }
    }

    @Override
    public void dead(UUID eventId, RetryKind retryKind, RetryReason reason, String errorType, String error) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(deadSql)) {
            ps.setString(1, reason.name());
            mapper.setOptionalString(ps, 2, errorType);
            mapper.setOptionalString(ps, 3, error);
            mapper.setTimestamp(ps, 4, Instant.now());
            mapper.setUuid(ps, 5, eventId);
            ps.setString(6, retryKind.name());
            if (ps.executeUpdate() == 0) {
                throw new NoSuchElementException("Retry task not found: " + eventId + "/" + retryKind);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to dead-letter retry for event: " + eventId, e);
        }
    }

    @Override
    public Optional<RetryTask> find(UUID eventId, RetryKind retryKind) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(selectByIdSql)) {
            mapper.setUuid(ps, 1, eventId);
            ps.setString(2, retryKind.name());
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(mapper.mapRow(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to find retry for event: " + eventId, e);
        }
    }

    @Override
    public int deleteByEventIds(Collection<UUID> eventIds) {
        if (eventIds == null || eventIds.isEmpty()) {
            return 0;
        }
        List<UUID> ids = List.copyOf(eventIds);
        String sql = deleteByEventIdsCache.computeIfAbsent(
                ids.size(), count -> sqlDialect.deleteRetriesByEventIdsStatement(count).formatted(tableName));
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(sql)) {
            int index = 1;
            for (UUID eventId : ids) {
                mapper.setUuid(ps, index++, eventId);
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to delete retries by event ids", e);
        }
    }

    private List<RetryTask> mapList(PreparedStatement ps) throws SQLException {
        List<RetryTask> result = new ArrayList<>();
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.add(mapper.mapRow(rs));
            }
        }
        return result;
    }
}
