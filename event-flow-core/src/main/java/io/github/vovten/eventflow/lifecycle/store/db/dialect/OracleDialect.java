package io.github.vovten.eventflow.lifecycle.store.db.dialect;

/**
 * {@link SqlDialect} for Oracle Database 12c+.
 * <p>
 * Uses {@code OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY} for row limiting,
 * {@code CLOB} for large text columns, and standard {@code TIMESTAMP} type.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public class OracleDialect implements SqlDialect {

    public static final SqlDialect INSTANCE = new OracleDialect();

    protected OracleDialect() {
    }

    @Override
    public String limitClause() {
        return "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY";
    }

    @Override
    public String textType() {
        return "CLOB";
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
    public String booleanLiteral(boolean value) {
        return value ? "1" : "0";
    }

    @Override
    public String booleanPredicate(String column) {
        return column + " = 1";
    }

    @Override
    public String booleanType() {
        return "NUMBER(1)";
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
