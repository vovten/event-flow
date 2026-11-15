package io.github.vovten.eventflow.lifecycle.store.db;

import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Shared JDBC helpers for the event store and retry schema initializers.
 * <p>
 * Keeps the table-existence lookup and the concurrent DDL execution in one place
 * so both schemas behave identically when several application instances start
 * against the same database.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
final class SchemaSupport {

    private SchemaSupport() {
        // Utility class
    }

    /**
     * Checks whether a table exists, trying the name as written and in upper and
     * lower case to accommodate databases that normalize identifiers.
     *
     * @param conn      the connection
     * @param tableName the table name (may be schema-qualified)
     * @return true when the table exists
     * @throws SQLException if metadata cannot be read
     */
    static boolean tableExists(Connection conn, String tableName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        String baseName = tableBase(tableName);
        try (ResultSet rs = meta.getTables(null, null, baseName, null)) {
            if (rs.next()) {
                return true;
            }
        }
        try (ResultSet rs = meta.getTables(null, null, baseName.toUpperCase(), null)) {
            if (rs.next()) {
                return true;
            }
        }
        try (ResultSet rs = meta.getTables(null, null, baseName.toLowerCase(), null)) {
            return rs.next();
        }
    }

    /**
     * Returns the unqualified part of a possibly schema-qualified table name.
     *
     * @param fullName the table name
     * @return the base name
     */
    static String tableBase(String fullName) {
        int dot = fullName.indexOf('.');
        return dot > 0 ? fullName.substring(dot + 1) : fullName;
    }

    /**
     * Executes the table and index DDL, tolerating the check-then-create race
     * between concurrently starting instances.
     * <p>
     * If the table creation fails because another instance created the table in
     * the meantime, the table is re-checked and the failure is treated as a
     * successful concurrent initialization (the winning instance owns the
     * indexes). Index failures caused by an already-existing object are ignored;
     * all other failures are propagated.
     *
     * @param conn            the connection (auto-commit disabled by the caller)
     * @param tableName       the table name
     * @param createTableSql  the CREATE TABLE statement
     * @param indexStatements the CREATE INDEX statements with their names
     * @param log             the caller's logger
     * @throws SQLException if the DDL fails for a reason other than a concurrent
     *                      initialization
     */
    static void executeDdl(Connection conn, String tableName, String createTableSql,
                           List<IndexStatement> indexStatements, Logger log) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try {
                stmt.execute(createTableSql);
            } catch (SQLException e) {
                if (tableExists(conn, tableName)) {
                    log.info("Concurrent schema initialization detected for table '{}': the table "
                            + "was created by another instance, skipping DDL", tableName);
                    return;
                }
                throw e;
            }
            for (IndexStatement index : indexStatements) {
                try {
                    stmt.execute(index.sql());
                } catch (SQLException e) {
                    // Some databases (for example H2) may still report a
                    // duplicate object for CREATE INDEX IF NOT EXISTS during a
                    // concurrent start, so confirm the index existence instead
                    // of relying on the error code alone.
                    if (!isDuplicateObject(e) && !indexExists(conn, tableName, index.name())) {
                        throw e;
                    }
                    log.debug("Ignoring already-existing index for table '{}': {}",
                            tableName, e.getMessage());
                }
            }
        }
    }

    /**
     * Checks whether an index exists, trying the table name as written and in
     * upper and lower case.
     *
     * @param conn      the connection
     * @param tableName the table name
     * @param indexName the index name
     * @return true when the index exists
     * @throws SQLException if metadata cannot be read
     */
    static boolean indexExists(Connection conn, String tableName, String indexName) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        String baseName = tableBase(tableName);
        return indexExists(meta, baseName, indexName)
                || indexExists(meta, baseName.toUpperCase(), indexName)
                || indexExists(meta, baseName.toLowerCase(), indexName);
    }

    private static boolean indexExists(DatabaseMetaData meta, String tableName,
                                       String indexName) throws SQLException {
        try (ResultSet rs = meta.getIndexInfo(null, null, tableName, false, false)) {
            while (rs.next()) {
                String found = rs.getString("INDEX_NAME");
                if (found != null && found.equalsIgnoreCase(indexName)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks whether an SQL failure means that the object already exists.
     *
     * @param e the SQL exception
     * @return true for duplicate-object errors of the supported databases
     */
    static boolean isDuplicateObject(SQLException e) {
        String state = e.getSQLState();
        int code = e.getErrorCode();
        return "42P07".equals(state)      // PostgreSQL: relation already exists
                || "42710".equals(state)  // PostgreSQL: duplicate object
                || "42S01".equals(state)  // MySQL / SQL Server: table already exists
                || "42101".equals(state)  // H2: table already exists
                || code == 1050           // MySQL: table already exists
                || code == 1061           // MySQL: duplicate key name
                || code == 2714           // SQL Server: object already exists
                || code == 1913           // SQL Server: duplicate index
                || code == 955;           // Oracle: ORA-00955 name already used
    }

    /**
     * An index DDL statement together with its index name.
     *
     * @param sql  the CREATE INDEX statement
     * @param name the index name used for the existence check
     */
    record IndexStatement(String sql, String name) {
    }
}
