package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.retry.RetryKind;

import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

/**
 * Maps {@link RetryTask} records to and from JDBC statements, handling UUID
 * conversion and type variations.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
public final class RetryTaskMapper {

    private static final Calendar UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private final UuidType uuidType;

    /**
     * Creates a new mapper.
     *
     * @param uuidType UUID storage strategy
     */
    public RetryTaskMapper(UuidType uuidType) {
        this.uuidType = uuidType;
    }

    /**
     * Sets INSERT parameters for a retry task.
     *
     * @param ps   prepared statement
     * @param task retry task
     * @throws SQLException on JDBC error
     */
    public void setInsertParameters(PreparedStatement ps, RetryTask task) throws SQLException {
        setUuid(ps, 1, task.eventId());
        ps.setString(2, task.service());
        ps.setString(3, task.retryKind().name());
        ps.setInt(4, task.attempt());
        ps.setString(5, task.state().name());
        ps.setString(6, task.reason().name());
        setTimestamp(ps, 7, task.nextAttemptAt());
        setOptionalString(ps, 8, task.lastErrorType());
        setOptionalString(ps, 9, task.lastError());
        setOptionalString(ps, 10, task.policyName());
        setOptionalString(ps, 11, task.claimedBy());
        setOptionalTimestamp(ps, 12, task.claimedUntil());
        setTimestamp(ps, 13, task.createdAt());
        setTimestamp(ps, 14, task.updatedAt());
    }

    /**
     * Sets UPDATE parameters for an existing retry task.
     *
     * @param ps   prepared statement
     * @param task retry task
     * @throws SQLException on JDBC error
     */
    public void setUpdateParameters(PreparedStatement ps, RetryTask task) throws SQLException {
        ps.setString(1, task.service());
        ps.setInt(2, task.attempt());
        ps.setString(3, task.state().name());
        ps.setString(4, task.reason().name());
        setTimestamp(ps, 5, task.nextAttemptAt());
        setOptionalString(ps, 6, task.lastErrorType());
        setOptionalString(ps, 7, task.lastError());
        setOptionalString(ps, 8, task.policyName());
        setOptionalString(ps, 9, task.claimedBy());
        setOptionalTimestamp(ps, 10, task.claimedUntil());
        setTimestamp(ps, 11, task.updatedAt());
        setUuid(ps, 12, task.eventId());
        ps.setString(13, task.retryKind().name());
    }

    /**
     * Maps a result-set row to a retry task.
     *
     * @param rs result set positioned at the current row
     * @return the retry task
     * @throws SQLException on JDBC error
     */
    public RetryTask mapRow(ResultSet rs) throws SQLException {
        return new RetryTask(
                getUuid(rs, "event_id"),
                rs.getString("service"),
                RetryKind.valueOf(rs.getString("retry_kind")),
                rs.getInt("attempt"),
                RetryState.valueOf(rs.getString("state")),
                RetryReason.valueOf(rs.getString("reason")),
                getTimestamp(rs, "next_attempt_at"),
                rs.getString("last_error_type"),
                rs.getString("last_error"),
                rs.getString("policy_name"),
                rs.getString("claimed_by"),
                getTimestampNullable(rs, "claimed_until"),
                getTimestamp(rs, "created_at"),
                getTimestamp(rs, "updated_at"));
    }

    /**
     * Sets a timestamp parameter.
     *
     * @param ps      prepared statement
     * @param index   parameter index
     * @param instant timestamp value
     * @throws SQLException on JDBC error
     */
    public void setTimestamp(PreparedStatement ps, int index, Instant instant) throws SQLException {
        ps.setTimestamp(index, Timestamp.from(instant), UTC);
    }

    /**
     * Sets an optional timestamp parameter.
     *
     * @param ps      prepared statement
     * @param index   parameter index
     * @param instant timestamp value, may be null
     * @throws SQLException on JDBC error
     */
    public void setOptionalTimestamp(PreparedStatement ps, int index, Instant instant) throws SQLException {
        if (instant != null) {
            setTimestamp(ps, index, instant);
        } else {
            ps.setNull(index, Types.TIMESTAMP);
        }
    }

    /**
     * Sets a UUID parameter.
     *
     * @param ps    prepared statement
     * @param index parameter index
     * @param uuid  UUID value
     * @throws SQLException on JDBC error
     */
    public void setUuid(PreparedStatement ps, int index, UUID uuid) throws SQLException {
        if (uuidType == UuidType.NATIVE) {
            ps.setObject(index, uuid);
        } else {
            ps.setBytes(index, uuidToBytes(uuid));
        }
    }

    /**
     * Reads a UUID column.
     *
     * @param rs     result set
     * @param column column name
     * @return UUID value
     * @throws SQLException on JDBC error
     */
    public UUID getUuid(ResultSet rs, String column) throws SQLException {
        if (uuidType == UuidType.NATIVE) {
            return rs.getObject(column, UUID.class);
        }
        byte[] bytes = rs.getBytes(column);
        return bytes != null ? bytesToUuid(bytes) : null;
    }

    private Instant getTimestamp(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column, UTC);
        return value != null ? value.toInstant() : null;
    }

    private Instant getTimestampNullable(ResultSet rs, String column) throws SQLException {
        return getTimestamp(rs, column);
    }

    /**
     * Sets an optional string parameter.
     *
     * @param ps    prepared statement
     * @param index parameter index
     * @param value string value, may be null
     * @throws SQLException on JDBC error
     */
    public void setOptionalString(PreparedStatement ps, int index, String value) throws SQLException {
        if (value != null) {
            ps.setString(index, value);
        } else {
            ps.setNull(index, Types.VARCHAR);
        }
    }

    private byte[] uuidToBytes(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits());
        buffer.putLong(uuid.getLeastSignificantBits());
        return buffer.array();
    }

    private UUID bytesToUuid(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return new UUID(buffer.getLong(), buffer.getLong());
    }
}
