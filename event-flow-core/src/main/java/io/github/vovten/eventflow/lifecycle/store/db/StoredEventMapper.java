package io.github.vovten.eventflow.lifecycle.store.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;

import java.nio.ByteBuffer;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * Maps {@link StoredEvent} objects to and from JDBC {@link PreparedStatement}
 * and {@link ResultSet}, handling UUID conversion and type variations.
 * <p>
 * This class is package-private and used exclusively by {@link JdbcEventStore}.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class StoredEventMapper {

    private static final Logger log = LoggerFactory.getLogger(StoredEventMapper.class);
    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private final UuidType uuidType;

    /**
     * Creates a new StoredEventMapper.
     *
     * @param uuidType the UUID storage strategy (native or binary)
     */
    public StoredEventMapper(UuidType uuidType) {
        this.uuidType = uuidType;
    }

    private byte[] uuidToBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.allocate(16);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }

    private UUID bytesToUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        return new UUID(bb.getLong(), bb.getLong());
    }

    /**
     * Sets a UUID parameter on a PreparedStatement, using the configured UUID strategy.
     *
     * @param ps    the PreparedStatement
     * @param index the parameter index (1-based)
     * @param uuid  the UUID value (must not be null)
     * @throws SQLException if a database access error occurs
     */
    public void setUuid(PreparedStatement ps, int index, UUID uuid) throws SQLException {
        if (uuidType == UuidType.NATIVE) {
            ps.setObject(index, uuid);
        } else {
            ps.setBytes(index, uuidToBytes(uuid));
        }
    }

    /**
     * Sets a nullable UUID parameter on a PreparedStatement.
     *
     * @param ps    the PreparedStatement
     * @param index the parameter index (1-based)
     * @param uuid  the UUID value (may be null)
     * @throws SQLException if a database access error occurs
     */
    public void setUuidNullable(PreparedStatement ps, int index, UUID uuid) throws SQLException {
        if (uuid != null) {
            setUuid(ps, index, uuid);
        } else {
            ps.setNull(index, uuidType == UuidType.NATIVE ? Types.OTHER : Types.BINARY);
        }
    }

    /**
     * Reads a UUID column from a ResultSet, using the configured UUID strategy.
     *
     * @param rs     the ResultSet
     * @param column the column name
     * @return the UUID value, or null if the column value was NULL
     * @throws SQLException if a database access error occurs
     */
    public UUID getUuid(ResultSet rs, String column) throws SQLException {
        if (uuidType == UuidType.NATIVE) {
            return rs.getObject(column, UUID.class);
        }
        byte[] bytes = rs.getBytes(column);
        return bytes != null ? bytesToUuid(bytes) : null;
    }

    /**
     * Sets all INSERT parameters on a PreparedStatement from a StoredEvent.
     *
     * @param ps    the PreparedStatement (must be prepared with the INSERT statement)
     * @param event the event to insert
     * @throws SQLException if a database access error occurs
     */
    public void setInsertParameters(PreparedStatement ps, StoredEvent event) throws SQLException {
        setUuid(ps, 1, event.eventId());
        ps.setString(2, event.eventType());
        setOptionalString(ps, 3, event.service());
        ps.setString(4, event.payload());
        setUuidNullable(ps, 5, event.processId());
        ps.setString(6, String.valueOf(event.status().getCode()));
        ps.setInt(7, event.retryCount());
        ps.setBoolean(8, event.retry());
        ps.setTimestamp(9, Timestamp.from(event.createdAt()), UTC);
        ps.setTimestamp(10, Timestamp.from(event.updatedAt()), UTC);
        setOptionalString(ps, 11, event.errorDetails());
    }

    /**
     * Sets UPDATE parameters for marking an event for manual retry.
     *
     * @param ps      the PreparedStatement (must be prepared with markForRetryStatement)
     * @param eventId the event ID to mark for retry
     * @throws SQLException if a database access error occurs
     */
    public void setMarkForRetryParameters(PreparedStatement ps, UUID eventId) throws SQLException {
        ps.setTimestamp(1, Timestamp.from(Instant.now()), UTC);
        setUuid(ps, 2, eventId);
    }

    /**
     * Sets UPDATE parameters for an event status change.
     *
     * @param ps            the PreparedStatement (must be prepared with the UPDATE statement)
     * @param eventId       the event ID
     * @param status        the new status
     * @param errorDetails  the error details (may be null)
     * @throws SQLException if a database access error occurs
     */
    public void setUpdateParameters(PreparedStatement ps, UUID eventId, EventStatus status,
                              String errorDetails) throws SQLException {
        ps.setString(1, String.valueOf(status.getCode()));
        setOptionalString(ps, 2, errorDetails);
        ps.setTimestamp(3, Timestamp.from(Instant.now()), UTC);
        setUuid(ps, 4, eventId);
    }

    /**
     * Sets the status code characters on a PreparedStatement for an IN clause.
     *
     * @param ps       the PreparedStatement
     * @param statuses the list of statuses to set (must not be empty)
     * @throws SQLException if a database access error occurs
     */
    public void setStatusCodes(PreparedStatement ps, List<EventStatus> statuses) throws SQLException {
        int i = 1;
        for (EventStatus status : statuses) {
            ps.setString(i++, String.valueOf(status.getCode()));
        }
    }

    /**
     * Sets a timestamp parameter for the {@code updated_at < ?} cutoff.
     * Logs a warning if the timestamp is in the future.
     *
     * @param ps    the PreparedStatement
     * @param index the parameter index (1-based)
     * @param before the cutoff instant
     * @throws SQLException if a database access error occurs
     */
    public void setBeforeTimestamp(PreparedStatement ps, int index, Instant before) throws SQLException {
        warnIfFutureTimestamp(before);
        ps.setTimestamp(index, Timestamp.from(before), UTC);
    }

    /**
     * Maps a single row of a ResultSet to a StoredEvent.
     *
     * @param rs the ResultSet positioned at the current row
     * @return the mapped event
     * @throws SQLException if a database access error occurs
     */
    public StoredEvent mapRow(ResultSet rs) throws SQLException {
        return new StoredEvent(
                getUuid(rs, "event_id"),
                rs.getString("event_type"),
                rs.getString("service"),
                rs.getString("payload"),
                getUuid(rs, "process_id"),
                EventStatus.fromCode(rs.getString("status").charAt(0)),
                rs.getInt("retry_count"),
                rs.getBoolean("retry"),
                rs.getTimestamp("created_at").toInstant(),
                rs.getTimestamp("updated_at").toInstant(),
                rs.getString("error_details")
        );
    }

    /**
     * Maps a {@link SQLException} thrown during event save to the appropriate runtime exception.
     *
     * @param e     the SQL exception
     * @param event the event being saved
     * @return {@link IllegalArgumentException} for duplicate key, {@link RuntimeException} otherwise
     */
    public RuntimeException mapSaveError(SQLException e, StoredEvent event) {
        if (isDuplicateKey(e)) {
            return new IllegalArgumentException(
                    "Event with ID " + event.eventId() + " already exists", e);
        }
        return new RuntimeException("Failed to save event: " + event.eventId(), e);
    }

    /**
     * Sets a nullable String parameter on a PreparedStatement.
     *
     * @param ps    the PreparedStatement
     * @param index the parameter index (1-based)
     * @param value the String value (may be null)
     * @throws SQLException if a database access error occurs
     */
    public void setOptionalString(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            ps.setString(index, value);
        } else {
            ps.setNull(index, Types.VARCHAR);
        }
    }

    private void warnIfFutureTimestamp(Instant before) {
        if (before.isAfter(Instant.now())) {
            log.warn("Looking for events with future timestamp: {}", before);
        }
    }

    private boolean isDuplicateKey(SQLException e) {
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
