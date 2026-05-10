package io.github.vovten.eventflow.publisher.persistence.jdbc;

import io.github.vovten.eventflow.publisher.persistence.EventRecord;
import io.github.vovten.eventflow.publisher.persistence.EventStatus;
import io.github.vovten.eventflow.publisher.persistence.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * JDBC implementation of {@link EventRepository}
 * using a simplified schema with a single JSON event column.
 * <p>
 * Table schema:
 * <pre>{@code
 * CREATE TABLE schema_name.event_outbox (
 *     id          UUID PRIMARY KEY,
 *     process_id  UUID,
 *     event       JSON NOT NULL,  -- full serialized event (Envelope or Event)
 *     status      SMALLINT NOT NULL DEFAULT 0,  -- 0=PENDING, 1=PUBLISHED, 2=FAILED
 *     retry       BOOLEAN NOT NULL DEFAULT FALSE,
 *     retry_count INTEGER NOT NULL DEFAULT 0,
 *     created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
 *     modified_at TIMESTAMP NOT NULL DEFAULT NOW(),
 *     error_message TEXT
 * );
 * }</pre>
 * <p>
 * Status codes (SMALLINT):
 * <ul>
 *   <li>0 = PENDING - Event is saved but not yet published</li>
 *   <li>1 = PUBLISHED - Event was successfully published</li>
 *   <li>2 = FAILED - Event publishing failed after all retries</li>
 * </ul>
 */
public class JdbcEventRepository implements EventRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventRepository.class);

    private final DataSource dataSource;
    private final String schema;
    private final String tableName;
    private final String qualifiedTableName;
    private final boolean createTableIfNotExists;
    private final boolean isPostgres;

    public JdbcEventRepository(DataSource dataSource, String tableName) {
        this(dataSource, "public", tableName, true);
    }

    public JdbcEventRepository(DataSource dataSource, String schema, String tableName) {
        this(dataSource, schema, tableName, true);
    }

    public JdbcEventRepository(DataSource dataSource, String schema, String tableName, boolean createTableIfNotExists) {
        this.dataSource = dataSource;
        this.schema = schema;
        this.tableName = tableName;
        this.qualifiedTableName = schema + "." + tableName;
        this.createTableIfNotExists = createTableIfNotExists;

        this.isPostgres = detectPostgres();
        createTableIfNotExists();
    }

    /**
     * Detect if we're connected to PostgreSQL.
     */
    private boolean detectPostgres() {
        try (Connection conn = dataSource.getConnection()) {
            String productName = conn.getMetaData().getDatabaseProductName();
            return "PostgreSQL".equalsIgnoreCase(productName);
        } catch (SQLException e) {
            log.warn("Failed to detect database type, assuming non-PostgreSQL", e);
            return false;
        }
    }

    /**
     * Get SQL type for JSON column.
     * PostgreSQL uses JSONB, others use TEXT or JSON.
     */
    private String jsonType() {
        return isPostgres ? "JSONB" : "TEXT";
    }

    @Override
    public void save(EventRecord record) {
        log.debug("Saving event record: id={}, processId={}, retry={}", record.id(), record.processId(), record.retry());

        String sql = String.format(
                "INSERT INTO %s (id, process_id, event, status, retry, retry_count, created_at, modified_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setObject(1, record.id());
            stmt.setObject(2, record.processId());
            stmt.setString(3, record.event());
            stmt.setInt(4, record.status().code());
            stmt.setBoolean(5, record.retry());
            stmt.setInt(6, record.retryCount());
            stmt.setTimestamp(7, Timestamp.from(record.createdAt()));
            stmt.setTimestamp(8, Timestamp.from(record.modifiedAt()));

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to save event record: " + record.id(), e);
        }
    }

    @Override
    public void updateStatus(UUID id, EventStatus status) {
        updateStatus(id, status, null);
    }

    @Override
    public void updateStatus(UUID id, EventStatus status, String errorMessage) {
        log.debug("Updating event status: id={}, status={}", id, status);

        Instant now = Instant.now();
        String sql = String.format(
                "UPDATE %s SET status = ?, error_message = ?, modified_at = ? WHERE id = ?",
                qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setInt(1, status.code());
            stmt.setString(2, errorMessage);
            stmt.setTimestamp(3, Timestamp.from(now));
            stmt.setObject(4, id);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                log.warn("No event record found to update: id={}", id);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to update event status: " + id, e);
        }
    }

    @Override
    public void updateFields(UUID id, FieldUpdate update) {
        if (update.retry() == null && update.modifiedAt() == null && update.status() == null) {
            return;
        }

        StringBuilder sql = new StringBuilder("UPDATE ").append(qualifiedTableName).append(" SET ");
        List<Object> params = new ArrayList<>();
        boolean first = true;

        if (update.status() != null) {
            sql.append("status = ?");
            params.add(update.status().code());
            first = false;
        }
        if (update.retry() != null) {
            if (!first) sql.append(", ");
            sql.append("retry = ?");
            params.add(update.retry());
            first = false;
        }
        if (update.modifiedAt() != null) {
            if (!first) sql.append(", ");
            sql.append("modified_at = ?");
            params.add(Timestamp.from(update.modifiedAt()));
        }
        sql.append(" WHERE id = ?");
        params.add(id);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {

            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to update event fields: " + id, e);
        }
    }

    @Override
    public int markFailed(UUID id, String errorMessage) {
        log.debug("Marking event as failed: id={}", id);

        // First get current retry count
        String selectSql = String.format(
                "SELECT retry_count FROM %s WHERE id = ?", qualifiedTableName);

        // Then update with incremented retry count and modified_at
        String updateSql = String.format(
                "UPDATE %s SET status = ?, error_message = ?, retry_count = retry_count + 1, modified_at = ? WHERE id = ?",
                qualifiedTableName);

        int newRetryCount = 0;

        try (Connection conn = dataSource.getConnection()) {
            // Get current retry count
            try (PreparedStatement selectStmt = conn.prepareStatement(selectSql)) {
                selectStmt.setObject(1, id);
                try (ResultSet rs = selectStmt.executeQuery()) {
                    if (rs.next()) {
                        newRetryCount = rs.getInt("retry_count") + 1;
                    }
                }
            }

            // Update with incremented count
            try (PreparedStatement updateStmt = conn.prepareStatement(updateSql)) {
                updateStmt.setInt(1, EventStatus.FAILED.code());
                updateStmt.setString(2, errorMessage);
                updateStmt.setTimestamp(3, Timestamp.from(Instant.now()));
                updateStmt.setObject(4, id);
                updateStmt.executeUpdate();
            }

            log.debug("Event marked as failed: id={}, retryCount={}", id, newRetryCount);
            return newRetryCount;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark event as failed: " + id, e);
        }
    }

    @Override
    public List<EventRecord> findPending(int limit) {
        log.debug("Finding pending events: limit={}", limit);

        String sql = String.format(
                "SELECT id, process_id, event, status, retry, retry_count, created_at, modified_at, error_message FROM %s WHERE status = ? ORDER BY created_at LIMIT ?",
                qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setMaxRows(limit);
            stmt.setInt(1, EventStatus.PENDING.code());
            stmt.setInt(2, limit);

            List<EventRecord> records = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    records.add(mapRow(rs));
                }
            }
            return records;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to find pending events", e);
        }
    }

    @Override
    public List<EventRecord> findFailed(int limit) {
        log.debug("Finding events with retry=true: limit={}", limit);

        String sql = String.format(
                "SELECT id, process_id, event, status, retry, retry_count, created_at, modified_at, error_message FROM %s " +
                "WHERE retry = TRUE AND status IN (?, ?) ORDER BY modified_at LIMIT ?",
                qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setMaxRows(limit);
            stmt.setInt(1, EventStatus.PENDING.code());
            stmt.setInt(2, EventStatus.FAILED.code());
            stmt.setInt(3, limit);

            List<EventRecord> records = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    records.add(mapRow(rs));
                }
            }
            return records;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events for manual retry", e);
        }
    }

    @Override
    public List<EventRecord> findFailedForRetry(int limit, int maxRetryCount, Instant minModifiedAt) {
        log.debug("Finding failed events for retry: limit={}, maxRetryCount={}, minModifiedAt={}", limit, maxRetryCount, minModifiedAt);

        String sql = String.format(
                "SELECT id, process_id, event, status, retry, retry_count, created_at, modified_at, error_message FROM %s " +
                "WHERE status = ? AND retry_count < ? AND modified_at <= ? " +
                "ORDER BY modified_at LIMIT ?",
                qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setMaxRows(limit);
            stmt.setInt(1, EventStatus.FAILED.code());
            stmt.setInt(2, maxRetryCount);
            stmt.setTimestamp(3, Timestamp.from(minModifiedAt));
            stmt.setInt(4, limit);

            List<EventRecord> records = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    records.add(mapRow(rs));
                }
            }
            return records;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to find failed events for retry", e);
        }
    }

    @Override
    public Optional<EventRecord> findById(UUID id) {
        String sql = String.format(
                "SELECT id, process_id, event, status, retry, retry_count, created_at, modified_at, error_message FROM %s WHERE id = ?",
                qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setObject(1, id);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRow(rs));
                }
            }
            return Optional.empty();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to find event by id: " + id, e);
        }
    }

    private void createTableIfNotExists() {
        if (!createTableIfNotExists) {
            return;
        }

        String createTableSql = String.format("""
                CREATE TABLE IF NOT EXISTS %s (
                    id          UUID PRIMARY KEY,
                    process_id  UUID,
                    event       %s NOT NULL,
                    status      SMALLINT NOT NULL DEFAULT 0,
                    retry       BOOLEAN NOT NULL DEFAULT FALSE,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                    modified_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    error_message TEXT
                )
                """, qualifiedTableName, jsonType());

        String createIndexSql = String.format(
                "CREATE INDEX IF NOT EXISTS idx_%s_status ON %s (status)", tableName, qualifiedTableName);

        String createIndexRetrySql = String.format(
                "CREATE INDEX IF NOT EXISTS idx_%s_retry ON %s (retry, status)", tableName, qualifiedTableName);

        String createIndexFailedSql = String.format(
                "CREATE INDEX IF NOT EXISTS idx_%s_failed ON %s (status, retry_count, modified_at)", tableName, qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createTableSql);
            stmt.execute(createIndexSql);
            stmt.execute(createIndexRetrySql);
            stmt.execute(createIndexFailedSql);
            log.info("Ensured outbox table exists: {}", qualifiedTableName);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to create outbox table: " + qualifiedTableName, e);
        }
    }

    private EventRecord mapRow(ResultSet rs) throws SQLException {
        UUID id = rs.getObject("id", UUID.class);
        UUID processId = rs.getObject("process_id", UUID.class);
        String event = rs.getString("event");
        EventStatus status = EventStatus.fromCode(rs.getInt("status"));
        boolean retry = rs.getBoolean("retry");
        int retryCount = rs.getInt("retry_count");
        Instant createdAt = rs.getTimestamp("created_at").toInstant();
        Instant modifiedAt = rs.getTimestamp("modified_at").toInstant();
        String errorMessage = rs.getString("error_message");

        return new EventRecord(id, processId, event, createdAt)
                .status(status)
                .retry(retry)
                .retryCount(retryCount)
                .modifiedAt(modifiedAt)
                .errorMessage(errorMessage);
    }
}