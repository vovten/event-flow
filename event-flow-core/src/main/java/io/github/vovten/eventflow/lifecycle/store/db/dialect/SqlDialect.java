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
