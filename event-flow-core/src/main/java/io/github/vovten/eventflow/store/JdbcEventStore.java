package io.github.vovten.eventflow.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
 * Executes DDL on construction to ensure the {@code event_store} table exists.
 * Designed to work with any {@link DataSource} (PostgreSQL, H2, MySQL, etc.).
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
            CREATE TABLE IF NOT EXISTS event_store (
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

    private static final String CREATE_INDEX = """
            CREATE INDEX IF NOT EXISTS idx_event_store_status
            ON event_store(status, updated_at)
            """;

    private static final String INSERT = """
            INSERT INTO event_store
                (event_id, event_type, payload, process_id, status,
                 retry_count, created_at, updated_at, error_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String SELECT_BY_STATUS = """
            SELECT event_id, event_type, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM event_store
            WHERE status = ? AND updated_at < ?
            ORDER BY updated_at ASC
            """;

    private static final String SELECT_BY_ID = """
            SELECT event_id, event_type, payload, process_id, status,
                   retry_count, created_at, updated_at, error_details
            FROM event_store
            WHERE event_id = ?
            """;

    private static final String UPDATE_STATUS_ONLY =
            "UPDATE event_store SET status = ?, error_details = ?, updated_at = ? WHERE event_id = ?";

    private static final String UPDATE_STATUS_WITH_RETRY =
            "UPDATE event_store SET status = ?, error_details = ?, updated_at = ?, retry_count = retry_count + 1 WHERE event_id = ?";

    private final DataSource dataSource;

    /**
     * Creates a new JdbcEventStore and initializes the database schema.
     *
     * @param dataSource the JDBC DataSource (must not be null)
     */
    public JdbcEventStore(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        initSchema();
    }

    private void initSchema() {
        try (Connection conn = dataSource.getConnection(); var stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE);
            stmt.execute(CREATE_INDEX);
            log.info("Event store schema initialized");
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to initialize event store schema", e);
        }
    }

    @Override
    public void save(StoredEvent event) {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(INSERT)) {
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
        String sql = (status == EventStatus.NEW) ? UPDATE_STATUS_WITH_RETRY : UPDATE_STATUS_ONLY;
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
                PreparedStatement ps = conn.prepareStatement(SELECT_BY_STATUS)) {
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
                PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID)) {
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
