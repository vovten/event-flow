package io.github.vovten.eventflow.lifecycle.store.db.dialect;

import io.github.vovten.eventflow.lifecycle.store.db.UuidType;

/**
 * {@link SqlDialect} for H2 in-memory database (used for testing).
 * <p>
 * SQL syntax is identical to {@link PostgresqlDialect}:
 * uses {@code LIMIT ?}, standard {@code TEXT} and {@code TIMESTAMP} types,
 * and single-level subquery for DELETE.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class H2Dialect implements SqlDialect {

    public static final SqlDialect INSTANCE = new H2Dialect();

    protected H2Dialect() {
    }

    @Override
    public String limitClause() {
        return "LIMIT ?";
    }

    @Override
    public String textType() {
        return "TEXT";
    }

    @Override
    public String timestampType() {
        return "TIMESTAMP";
    }

    @Override
    public UuidType uuidType() {
        return UuidType.NATIVE;
    }

    @Override
    public String insertStatement() {
        return """
                INSERT INTO %s (event_id, event_type, service, payload, channels, process_id,
                                status, retry_count, retry, created_at, updated_at, error_details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;
    }

    @Override
    public String selectByStatusStatement() {
        return """
                SELECT event_id, event_type, service, payload, channels, process_id,
                       status, retry_count, retry, created_at, updated_at, error_details
                FROM %s
                WHERE status = ? AND updated_at < ?
                ORDER BY updated_at ASC
                """;
    }

    @Override
    public String selectByIdStatement() {
        return """
                SELECT event_id, event_type, service, payload, channels, process_id,
                       status, retry_count, retry, created_at, updated_at, error_details
                FROM %s
                WHERE event_id = ?
                """;
    }

    @Override
    public String updateStatusOnlyStatement() {
        return """
                UPDATE %s
                SET status = ?, error_details = ?, updated_at = ?, retry = FALSE
                WHERE event_id = ?
                """;
    }

    @Override
    public String updateStatusWithRetryStatement() {
        return """
                UPDATE %s
                SET status = ?, error_details = ?, updated_at = ?,
                    retry_count = CASE WHEN retry THEN retry_count ELSE retry_count + 1 END
                WHERE event_id = ?
                """;
    }

    @Override
    public String selectByStatusesStatement(int statusCount) {
        String placeholders = placeholders(statusCount);
        return "SELECT event_id, event_type, service, payload, channels, process_id,"
                + " status, retry_count, retry, created_at, updated_at, error_details"
                + " FROM %s"
                + " WHERE status IN (" + placeholders + ") AND updated_at < ?"
                + " ORDER BY updated_at ASC"
                + " " + limitClause();
    }

    @Override
    public String deleteByStatusesStatement(int statusCount) {
        String placeholders = placeholders(statusCount);
        return "DELETE FROM %s"
                + " WHERE event_id IN ("
                + " SELECT event_id FROM %s"
                + " WHERE status IN (" + placeholders + ") AND updated_at < ?"
                + " ORDER BY updated_at ASC"
                + " " + limitClause()
                + ")";
    }

    private String placeholders(int count) {
        if (count <= 0) {
            return "";
        }
        return "?" + ", ?".repeat(count - 1);
    }
}
