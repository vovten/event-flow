package io.github.vovten.eventflow.lifecycle.store.db.dialect;

/**
 * {@link SqlDialect} for MySQL (and compatible databases such as MariaDB).
 * <p>
 * Uses {@code LIMIT ?} for row limiting, standard {@code TEXT} and {@code TIMESTAMP} types,
 * and a double-nested subquery for DELETE (MySQL forbids referencing the target table
 * directly in a subquery).
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class MySqlDialect implements SqlDialect {

    public static final SqlDialect INSTANCE = new MySqlDialect();

    protected MySqlDialect() {
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
                + " SELECT event_id FROM ("
                + " SELECT event_id FROM %s"
                + " WHERE status IN (" + placeholders + ") AND updated_at < ?"
                + " ORDER BY updated_at ASC"
                + " " + limitClause()
                + " ) AS cleanup_ids"
                + ")";
    }

    private String placeholders(int count) {
        if (count <= 0) {
            return "";
        }
        return "?" + ", ?".repeat(count - 1);
    }
}
