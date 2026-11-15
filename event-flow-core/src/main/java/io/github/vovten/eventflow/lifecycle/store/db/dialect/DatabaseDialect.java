package io.github.vovten.eventflow.lifecycle.store.db.dialect;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Database dialect detection for supported database systems.
 * <p>
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
 * <p>
 * SQL syntax differences (LIMIT clause, type mappings, DELETE subquery) are
 * handled by {@link SqlDialect}, obtained via {@link SqlDialect#forDialect(DatabaseDialect)}.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public enum DatabaseDialect {

    POSTGRESQL,
    MYSQL,
    H2,
    ORACLE,
    SQLSERVER;

    /**
     * Detects the dialect from a live JDBC connection's product name.
     *
     * @param conn an active JDBC connection
     * @return the detected dialect (never {@code null})
     * @throws SQLException         if metadata cannot be obtained
     * @throws IllegalStateException if the database product is not supported
     */
    public static DatabaseDialect detect(Connection conn) throws SQLException {
        String name = conn.getMetaData().getDatabaseProductName();
        String normalized = name == null ? "" : name.toLowerCase(java.util.Locale.ROOT);
        if (normalized.contains("oracle")) {
            return ORACLE;
        }
        if (normalized.contains("mysql") || normalized.contains("mariadb")) {
            return MYSQL;
        }
        if (normalized.contains("microsoft") || normalized.contains("sql server")) {
            return SQLSERVER;
        }
        if (normalized.contains("h2")) {
            return H2;
        }
        if (normalized.contains("postgres") || normalized.contains("cockroach")
                || normalized.contains("yugabyte")) {
            return POSTGRESQL;
        }
        throw new IllegalStateException(
                "Unsupported database '" + name + "'. Supported databases: "
                        + "PostgreSQL/CockroachDB/YugabyteDB, MySQL/MariaDB, H2, Oracle, SQL Server");
    }
}
