package io.github.vovten.eventflow.publisher.persistence.jdbc;

import io.github.vovten.eventflow.publisher.persistence.EventRecord;
import io.github.vovten.eventflow.publisher.persistence.EventStatus;
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
 * JDBC implementation of {@link io.github.vovten.eventflow.publisher.persistence.EventRepository}
 * using a simplified schema with a single JSON payload column.
 * <p>
 * Table schema:
 * <pre>{@code
 * CREATE TABLE event_outbox (
 *     id          UUID PRIMARY KEY,
 *     payload     JSONB NOT NULL,
 *     status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
 *     created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
 *     published_at TIMESTAMP,
 *     error_message TEXT,
 *     CONSTRAINT chk_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
 * );
 * }</pre>
 */
public class JdbcEventRepository implements io.github.vovten.eventflow.publisher.persistence.EventRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcEventRepository.class);

    private final DataSource dataSource;
    private final String tableName;
    private final String insertSql;
    private final String updateStatusSql;
    private final String selectPendingSql;
    private final String selectByIdSql;
    private final boolean createTableIfNotExists;

    /**
     * Create a new JdbcEventRepository.
     *
     * @param dataSource the JDBC DataSource
     * @param tableName  the outbox table name
     */
    public JdbcEventRepository(DataSource dataSource, String tableName) {
        this(dataSource, tableName, true);
    }

    /**
     * Create a new JdbcEventRepository.
     *
     * @param dataSource          the JDBC DataSource
     * @param tableName           the outbox table name
     * @param createTableIfNotExists create table if it doesn't exist
     */
    public JdbcEventRepository(DataSource dataSource, String tableName, boolean createTableIfNotExists) {
        this.dataSource = dataSource;
        this.tableName = tableName;
        this.createTableIfNotExists = createTableIfNotExists;

        this.insertSql = String.format(
                "INSERT INTO %s (id, payload, status, created_at) VALUES (?, ?::jsonb, ?, ?)", tableName);
        this.updateStatusSql = String.format(
                "UPDATE %s SET status = ?, published_at = ?, error_message = ? WHERE id = ?", tableName);
        this.selectPendingSql = String.format(
                "SELECT id, payload, status, created_at, published_at, error_message FROM %s WHERE status = 'PENDING' ORDER BY created_at FOR UPDATE SKIP LOCKED", tableName);
        this.selectByIdSql = String.format(
                "SELECT id, payload, status, created_at, published_at, error_message FROM %s WHERE id = ?", tableName);

        createTableIfNotExists();
    }

    @Override
    public void save(EventRecord record) {
        log.debug("Saving event record: id={}", record.id());

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(insertSql)) {

            stmt.setObject(1, record.id());
            stmt.setString(2, record.payload());
            stmt.setString(3, record.status().name());
            stmt.setTimestamp(4, Timestamp.from(record.createdAt()));

            stmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException("Failed to save event record: " + record.id(), e);
        }
    }

    @Override
    public void updateStatus(UUID id, EventStatus status, Instant publishedAt) {
        log.debug("Updating event status: id={}, status={}", id, status);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateStatusSql)) {

            stmt.setString(1, status.name());
            stmt.setTimestamp(2, publishedAt != null ? Timestamp.from(publishedAt) : null);
            stmt.setString(3, null);
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
    public void updateStatus(UUID id, EventStatus status, String errorMessage) {
        log.debug("Updating event status with error: id={}, status={}", id, status);

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(updateStatusSql)) {

            stmt.setString(1, status.name());
            stmt.setTimestamp(2, null);
            stmt.setString(3, errorMessage);
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
                    payload     JSONB NOT NULL,
                    status      VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                    published_at TIMESTAMP,
                    error_message TEXT,
                    CONSTRAINT chk_status CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED'))
                )
                """, tableName);

        String createIndexSql = String.format(
                "CREATE INDEX IF NOT EXISTS idx_%s_status ON %s (status)", tableName, tableName);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {

            stmt.execute(createTableSql);
            stmt.execute(createIndexSql);
            log.info("Ensured outbox table exists: {}", tableName);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to create outbox table: " + tableName, e);
        }
    }

    private EventRecord mapRow(ResultSet rs) throws SQLException {
        UUID id = rs.getObject("id", UUID.class);
        String payload = rs.getString("payload");
        EventStatus status = EventStatus.valueOf(rs.getString("status"));
        Instant createdAt = rs.getTimestamp("created_at").toInstant();
        Timestamp publishedAtTs = rs.getTimestamp("published_at");
        Instant publishedAt = publishedAtTs != null ? publishedAtTs.toInstant() : null;
        String errorMessage = rs.getString("error_message");

        return new EventRecord(id, payload, createdAt)
                .status(status)
                .publishedAt(publishedAt)
                .errorMessage(errorMessage);
    }
}