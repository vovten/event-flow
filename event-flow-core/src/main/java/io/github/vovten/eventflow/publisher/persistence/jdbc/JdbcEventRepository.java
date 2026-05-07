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
 * using a simplified schema with a single JSON payload column.
 * <p>
 * Table schema (PostgreSQL):
 * <pre>{@code
 * CREATE TABLE schema_name.event_outbox (
 *     id          UUID PRIMARY KEY,
 *     process_id  UUID,
 *     payload     JSONB NOT NULL,  -- full serialized event (Envelope or Event)
 *     status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
 *     retry       BOOLEAN NOT NULL DEFAULT FALSE,
 *     created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
 *     error_message TEXT
 * );
 * }</pre>
 */
public class JdbcEventRepository implements EventRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventRepository.class);

    private final DataSource dataSource;
    private final String schema;
    private final String tableName;
    private final String qualifiedTableName;
    private final String insertSql;
    private final String updateStatusSql;
    private final String selectPendingSql;
    private final String selectForRetrySql;
    private final String selectByIdSql;
    private final boolean createTableIfNotExists;

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

        this.insertSql = String.format(
                "INSERT INTO %s (id, process_id, payload, status, retry, created_at) VALUES (?, ?, ?::jsonb, ?, ?, ?)", qualifiedTableName);
        this.updateStatusSql = String.format(
                "UPDATE %s SET status = ?, error_message = ? WHERE id = ?", qualifiedTableName);
        this.selectPendingSql = String.format(
                "SELECT id, process_id, payload, status, retry, created_at, error_message FROM %s WHERE status = 'PENDING' ORDER BY created_at FOR UPDATE SKIP LOCKED", qualifiedTableName);
        this.selectForRetrySql = String.format(
                "SELECT id, process_id, payload, status, retry, created_at, error_message FROM %s WHERE retry = TRUE AND status IN ('PENDING', 'FAILED') ORDER BY created_at FOR UPDATE SKIP LOCKED", qualifiedTableName);
        this.selectByIdSql = String.format(
                "SELECT id, process_id, payload, status, retry, created_at, error_message FROM %s WHERE id = ?", qualifiedTableName);

        createTableIfNotExists();
    }

    @Override
    public void save(EventRecord record) {
        log.debug("Saving event record: id={}, processId={}, retry={}", record.id(), record.processId(), record.retry());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(insertSql)) {

            stmt.setObject(1, record.id());
            stmt.setObject(2, record.processId());
            stmt.setString(3, record.event());
            stmt.setString(4, record.status().name());
            stmt.setBoolean(5, record.retry());
            stmt.setTimestamp(6, Timestamp.from(record.createdAt()));

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

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateStatusSql)) {

            stmt.setString(1, status.name());
            stmt.setString(2, errorMessage);
            stmt.setObject(3, id);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                log.warn("No event record found to update: id={}", id);
            }

        } catch (SQLException e) {
            throw new RuntimeException("Failed to update event status: " + id, e);
        }
    }

    @Override
    public List<EventRecord> findPending(int limit) {
        log.debug("Finding pending events: limit={}", limit);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(selectPendingSql)) {

            stmt.setMaxRows(limit);

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
        log.debug("Finding events for retry: limit={}", limit);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(selectForRetrySql)) {

            stmt.setMaxRows(limit);

            List<EventRecord> records = new ArrayList<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    records.add(mapRow(rs));
                }
            }
            return records;

        } catch (SQLException e) {
            throw new RuntimeException("Failed to find events for retry", e);
        }
    }

    @Override
    public Optional<EventRecord> findById(UUID id) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(selectByIdSql)) {

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
                    payload     JSONB NOT NULL,
                    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    retry       BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                    error_message TEXT,
                    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
                )
                """, qualifiedTableName);

        String createIndexSql = String.format(
                "CREATE INDEX IF NOT EXISTS idx_%s_status_retry ON %s (status, retry)", tableName, qualifiedTableName);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createTableSql);
            stmt.execute(createIndexSql);
            log.info("Ensured outbox table exists: {}", qualifiedTableName);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to create outbox table: " + qualifiedTableName, e);
        }
    }

    private EventRecord mapRow(ResultSet rs) throws SQLException {
        UUID id = rs.getObject("id", UUID.class);
        UUID processId = rs.getObject("process_id", UUID.class);
        String payload = rs.getString("payload");
        EventStatus status = EventStatus.valueOf(rs.getString("status"));
        boolean retry = rs.getBoolean("retry");
        Instant createdAt = rs.getTimestamp("created_at").toInstant();
        String errorMessage = rs.getString("error_message");

        return new EventRecord(id, processId, payload, createdAt)
                .status(status)
                .retry(retry)
                .errorMessage(errorMessage);
    }
}