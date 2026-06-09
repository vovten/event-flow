package io.github.vovten.eventflow.lifecycle.store.db;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database dialect for SQL syntax differences across supported databases.
 * <p>
 * Encapsulates all database-specific SQL fragments and type mappings.
 * Detection is automatic from {@link java.sql.DatabaseMetaData#getDatabaseProductName()}.
 * <p>
 * Supported dialects:
 * <ul>
 *   <li>{@link #POSTGRESQL} — PostgreSQL, CockroachDB, YugabyteDB</li>
 *   <li>{@link #MYSQL} — MySQL, MariaDB</li>
 *   <li>{@link #H2} — H2 in-memory (used for testing)</li>
 *   <li>{@link #ORACLE} — Oracle Database 12c+</li>
 *   <li>{@link #SQLSERVER} — Microsoft SQL Server 2012+</li>
 * </ul>
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
enum DatabaseDialect {

    POSTGRESQL,
    MYSQL,
    H2,
    ORACLE,
    SQLSERVER;

    /**
     * SQL clause for limiting the number of rows returned.
     * This clause is appended after ORDER BY and includes the parameter placeholder {@code ?}.
     *
     * @return the dialect-specific limit clause, e.g. {@code "LIMIT ?"} or
     *         {@code "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY"}
     */
    String limitClause() {
        return switch (this) {
            case POSTGRESQL, MYSQL, H2 -> "LIMIT ?";
            case ORACLE, SQLSERVER -> "OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY";
        };
    }

    /**
     * SQL data type for large text columns ({@code payload}, {@code error_details}).
     *
     * @return the dialect-specific text type, e.g. {@code "TEXT"}, {@code "CLOB"}, or {@code "NVARCHAR(MAX)"}
     */
    String textType() {
        return switch (this) {
            case ORACLE -> "CLOB";
            case SQLSERVER -> "NVARCHAR(MAX)";
            default -> "TEXT";
        };
    }

    /**
     * SQL data type for timestamp columns ({@code created_at}, {@code updated_at}).
     * <p>
     * Microsoft SQL Server uses {@code DATETIME2} because its native
     * {@code TIMESTAMP} type is a binary row version, not a date/time type.
     *
     * @return the dialect-specific timestamp type, e.g. {@code "TIMESTAMP"} or {@code "DATETIME2"}
     */
    String timestampType() {
        return switch (this) {
            case SQLSERVER -> "DATETIME2";
            default -> "TIMESTAMP";
        };
    }

    /**
     * Detects the dialect from a live JDBC connection's product name.
     *
     * @param conn an active JDBC connection
     * @return the detected dialect (never {@code null})
     * @throws SQLException if metadata cannot be obtained
     */
    static DatabaseDialect detect(Connection conn) throws SQLException {
        String name = conn.getMetaData().getDatabaseProductName().toLowerCase();
        if (name.contains("oracle")) {
            return ORACLE;
        }
        if (name.contains("mysql") || name.contains("mariadb")) {
            return MYSQL;
        }
        if (name.contains("microsoft") || name.contains("sql server")) {
            return SQLSERVER;
        }
        if (name.contains("h2")) {
            return H2;
        }
        return POSTGRESQL;
    }
}
