package io.github.vovten.eventflow.lifecycle.store.db.dialect;

import io.github.vovten.eventflow.lifecycle.store.db.UuidType;

/**
 * SQL dialect for database-specific SQL syntax and type mappings.
 * <p>
 * Each implementation encapsulates the SQL fragments and type names
 * for a specific database family. Used by {@code JdbcEventStore}
 * and {@code SchemaInitializer} to generate dialect-appropriate SQL.
 * </p>
 * <p>
 * To add support for a new database, implement this interface and
 * add the mapping to {@link #forDialect(DatabaseDialect)}.
 * </p>
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public interface SqlDialect {

    /**
     * SQL clause for limiting the number of rows returned.
     * Appended after ORDER BY, includes the parameter placeholder {@code ?}.
     *
     * @return e.g. {@code "LIMIT ?"} or {@code "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"}
     */
    String limitClause();

    /**
     * SQL data type for large text columns ({@code payload}, {@code error_details}).
     *
     * @return e.g. {@code "TEXT"}, {@code "CLOB"}, or {@code "NVARCHAR(MAX)"}
     */
    String textType();

    /**
     * SQL data type for timestamp columns ({@code created_at}, {@code updated_at}).
     *
     * @return e.g. {@code "TIMESTAMP"} or {@code "DATETIME2"}
     */
    String timestampType();

    /**
     * UUID column storage strategy for this database.
     * <p>
     * Override in dialect implementations that support native UUID columns.
     *
     * @return {@link UuidType#BINARY} by default
     */
    default UuidType uuidType() {
        return UuidType.BINARY;
    }

    /**
     * INSERT statement template. The placeholder {@code %s} in table names
     * is already resolved by the caller.
     *
     * @return INSERT SQL with {@code ?} placeholders for all columns
     */
    String insertStatement();

    /**
     * SELECT by single status with updated_at cutoff.
     *
     * @return SELECT SQL with placeholders for status and cutoff timestamp
     */
    String selectByStatusStatement();

    /**
     * SELECT by ID.
     *
     * @return SELECT SQL with placeholder for event_id
     */
    String selectByIdStatement();

    /**
     * UPDATE status only (without retry increment).
     *
     * @return UPDATE SQL with placeholders for status, error_details, updated_at, event_id
     */
    String updateStatusOnlyStatement();

    /**
     * UPDATE status with retry count increment.
     *
     * @return UPDATE SQL with placeholders for status, error_details, updated_at, event_id
     */
    String updateStatusWithRetryStatement();

    /**
     * SELECT by multiple statuses with cutoff and limit.
     * The caller fills status placeholders before the cutoff and limit.
     *
     * @param statusCount number of status values for the IN clause
     * @return SELECT SQL with {@code ?} placeholders for statuses, cutoff, and limit
     */
    String selectByStatusesStatement(int statusCount);

    /**
     * SELECT for events eligible for retry belonging to a specific service:
     * matching any of the given statuses <b>or</b> having the manual retry flag
     * set ({@code retry = TRUE}), with an updated-at cutoff, a service filter,
     * and a limit.
     * <p>
     * The service filter is mandatory: when the event store is shared between
     * multiple services, each scheduler must only re-publish events it
     * originally published itself. Retrying events from all services is not
     * supported.
     * <p>
     * The caller can distinguish retry-flagged events by checking
     * {@code retry} — those bypass maxRetries/backoff checks.
     *
     * @param statusCount number of status values for the IN clause
     * @return SELECT SQL with placeholders for statuses, cutoff, service, and limit
     */
    default String selectRetryableEventsStatement(int statusCount) {
        StringBuilder placeholders = new StringBuilder();
        for (int i = 0; i < statusCount; i++) {
            if (i > 0) placeholders.append(", ");
            placeholders.append("?");
        }
        String template = """
                SELECT event_id, event_type, service, payload, channels, process_id,
                       status, retry_count, retry, created_at, updated_at, error_details
                FROM %%s
                WHERE (status IN (%s) OR retry = TRUE)
                  AND updated_at < ?
                  AND service = ?
                ORDER BY updated_at ASC
                %s
                """;
        return String.format(template, placeholders.toString(), limitClause());
    }

    /**
     * UPDATE to mark an event for manual retry.
     * <p>
     * Sets {@code retry = TRUE}, clears error details, and updates the timestamp.
     * Does NOT change the event status or retry count — the existing status
     * is preserved. The retry scheduler picks up the event by the {@code retry}
     * flag regardless of its current status.
     *
     * @return UPDATE SQL with placeholders for updated_at and event_id
     */
    default String markForRetryStatement() {
        return """
                UPDATE %s
                SET retry = TRUE, error_details = NULL, updated_at = ?
                WHERE event_id = ?
                """;
    }

    /**
     * DELETE by multiple statuses with cutoff and limit.
     * Uses subquery to select rows before deleting.
     *
     * @param statusCount number of status values for the IN clause
     * @return DELETE SQL with {@code ?} placeholders for statuses, cutoff, and limit
     */
    String deleteByStatusesStatement(int statusCount);

    /**
     * Returns the {@link SqlDialect} implementation for the given {@link DatabaseDialect}.
     *
     * @param dialect the database dialect enum value
     * @return the corresponding SqlDialect implementation
     */
    static SqlDialect forDialect(DatabaseDialect dialect) {
        return switch (dialect) {
            case POSTGRESQL -> PostgresqlDialect.INSTANCE;
            case H2 -> H2Dialect.INSTANCE;
            case MYSQL -> MySqlDialect.INSTANCE;
            case ORACLE -> OracleDialect.INSTANCE;
            case SQLSERVER -> SqlServerDialect.INSTANCE;
        };
    }
}
