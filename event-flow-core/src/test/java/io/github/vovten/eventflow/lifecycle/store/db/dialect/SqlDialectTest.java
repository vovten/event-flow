package io.github.vovten.eventflow.lifecycle.store.db.dialect;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("SqlDialect Tests")
class SqlDialectTest {

    @Test
    @DisplayName("Should provide LIMIT ? for PostgreSQL")
    void postgresqlLimitClause() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        assertThat(dialect.limitClause()).isEqualTo("LIMIT ?");
    }

    @Test
    @DisplayName("Should provide LIMIT ? for MySQL")
    void mysqlLimitClause() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.MYSQL);
        assertThat(dialect.limitClause()).isEqualTo("LIMIT ?");
    }

    @Test
    @DisplayName("Should provide LIMIT ? for H2")
    void h2LimitClause() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.H2);
        assertThat(dialect.limitClause()).isEqualTo("LIMIT ?");
    }

    @Test
    @DisplayName("Should provide OFFSET/FETCH for Oracle")
    void oracleLimitClause() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.ORACLE);
        assertThat(dialect.limitClause()).isEqualTo("OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
    }

    @Test
    @DisplayName("Should provide OFFSET/FETCH for SQL Server")
    void sqlServerLimitClause() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.SQLSERVER);
        assertThat(dialect.limitClause()).isEqualTo("OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
    }

    @Test
    @DisplayName("Should provide TEXT for PostgreSQL")
    void postgresqlTextType() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        assertThat(dialect.textType()).isEqualTo("TEXT");
    }

    @Test
    @DisplayName("Should provide TEXT for MySQL")
    void mysqlTextType() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.MYSQL);
        assertThat(dialect.textType()).isEqualTo("TEXT");
    }

    @Test
    @DisplayName("Should provide CLOB for Oracle")
    void oracleTextType() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.ORACLE);
        assertThat(dialect.textType()).isEqualTo("CLOB");
    }

    @Test
    @DisplayName("Should provide NVARCHAR(MAX) for SQL Server")
    void sqlServerTextType() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.SQLSERVER);
        assertThat(dialect.textType()).isEqualTo("NVARCHAR(MAX)");
    }

    @Test
    @DisplayName("Should provide TIMESTAMP for PostgreSQL")
    void postgresqlTimestampType() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        assertThat(dialect.timestampType()).isEqualTo("TIMESTAMP");
    }

    @Test
    @DisplayName("Should provide DATETIME2 for SQL Server")
    void sqlServerTimestampType() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.SQLSERVER);
        assertThat(dialect.timestampType()).isEqualTo("DATETIME2");
    }

    @Test
    @DisplayName("Should include channels column in INSERT and SELECT statements")
    void statementsContainChannelsColumn() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        assertThat(dialect.insertStatement()).contains("channels");
        assertThat(dialect.selectByIdStatement()).contains("channels");
        assertThat(dialect.selectByStatusStatement()).contains("channels");
        assertThat(dialect.selectByStatusesStatement(2)).contains("channels");
        assertThat(dialect.selectRetryableEventsStatement(2)).contains("channels");
    }

    @Test
    @DisplayName("Should generate INSERT statement with table placeholder")
    void insertStatementContainsPlaceholder() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.insertStatement();
        assertThat(sql).contains("%s");
        assertThat(sql).contains("INSERT INTO");
        assertThat(sql).contains("event_id");
        assertThat(sql).contains("payload");
    }

    @Test
    @DisplayName("Should generate SELECT BY ID statement with table placeholder")
    void selectByIdStatementContainsPlaceholder() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.selectByIdStatement();
        assertThat(sql).contains("%s");
        assertThat(sql).contains("WHERE event_id = ?");
    }

    @Test
    @DisplayName("Should generate SELECT BY STATUS statement with table placeholder")
    void selectByStatusStatementContainsPlaceholder() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.selectByStatusStatement();
        assertThat(sql).contains("%s");
        assertThat(sql).contains("WHERE status = ?");
    }

    @Test
    @DisplayName("Should generate SELECT BY STATUSES with correct number of placeholders")
    void selectByStatusesStatementHasCorrectPlaceholders() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.selectByStatusesStatement(3);
        assertThat(sql).contains("%s");
        assertThat(sql).contains("IN (?, ?, ?)");
        assertThat(sql).contains("LIMIT ?");
    }

    @Test
    @DisplayName("Should generate SELECT BY STATUSES with single placeholder")
    void selectByStatusesStatementWithOneStatus() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.selectByStatusesStatement(1);
        assertThat(sql).contains("IN (?)");
    }

    @Test
    @DisplayName("Should generate DELETE with single-level subquery for PostgreSQL")
    void deleteByStatusesStatementPostgresql() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.deleteByStatusesStatement(2);
        assertThat(sql).contains("%s"); // two table name placeholders
        assertThat(sql).contains("IN (?, ?)");
        assertThat(sql).contains("LIMIT ?");
        assertThat(sql).doesNotContain("cleanup_ids");
    }

    @Test
    @DisplayName("Should generate DELETE with double-nested subquery for MySQL")
    void deleteByStatusesStatementMysql() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.MYSQL);
        String sql = dialect.deleteByStatusesStatement(2);
        assertThat(sql).contains("cleanup_ids");
        assertThat(sql).contains("SELECT event_id FROM");
    }

    @Test
    @DisplayName("Should generate DELETE with single-level subquery for Oracle")
    void deleteByStatusesStatementOracle() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.ORACLE);
        String sql = dialect.deleteByStatusesStatement(2);
        assertThat(sql).contains("OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
        assertThat(sql).doesNotContain("cleanup_ids");
    }

    @Test
    @DisplayName("Should generate DELETE with single-level subquery for SQL Server")
    void deleteByStatusesStatementSqlServer() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.SQLSERVER);
        String sql = dialect.deleteByStatusesStatement(2);
        assertThat(sql).contains("OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY");
        assertThat(sql).doesNotContain("cleanup_ids");
    }

    @Test
    @DisplayName("Should provide formatted SQL via forDialect factory")
    void forDialectReturnsCorrectImpl() {
        assertThat(SqlDialect.forDialect(DatabaseDialect.POSTGRESQL)).isInstanceOf(PostgresqlDialect.class);
        assertThat(SqlDialect.forDialect(DatabaseDialect.H2)).isInstanceOf(H2Dialect.class);
        assertThat(SqlDialect.forDialect(DatabaseDialect.MYSQL)).isInstanceOf(MySqlDialect.class);
        assertThat(SqlDialect.forDialect(DatabaseDialect.ORACLE)).isInstanceOf(OracleDialect.class);
        assertThat(SqlDialect.forDialect(DatabaseDialect.SQLSERVER)).isInstanceOf(SqlServerDialect.class);
    }

    @Test
    @DisplayName("Should produce valid INSERT SQL when formatted with table name")
    void insertWithTableName() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.insertStatement().formatted("event_store");
        assertThat(sql).contains("INTO event_store");
        assertThat(sql).doesNotContain("%s");
    }

    @Test
    @DisplayName("Should produce valid SELECT BY STATUSES when formatted with table name")
    void selectByStatusesWithTableName() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.selectByStatusesStatement(3).formatted("event_store");
        assertThat(sql).contains("FROM event_store");
        assertThat(sql).doesNotContain("%s");
    }

    @Test
    @DisplayName("Should produce valid DELETE when formatted with table name")
    void deleteByStatusesWithTableName() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.deleteByStatusesStatement(2).formatted("event_store", "event_store");
        assertThat(sql).contains("FROM event_store");
        assertThat(sql).contains("SELECT event_id FROM event_store");
        assertThat(sql).doesNotContain("%s");
    }

    @Test
    @DisplayName("Should return UPDATE STATUS ONLY with table placeholder")
    void updateStatusOnlyStatement() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.updateStatusOnlyStatement().formatted("events");
        assertThat(sql).contains("UPDATE events");
        assertThat(sql).contains("SET status = ?, error_details = ?, updated_at = ?");
    }

    @Test
    @DisplayName("Should return UPDATE WITH RETRY with conditional retry count increment")
    void updateStatusWithRetryStatement() {
        SqlDialect dialect = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);
        String sql = dialect.updateStatusWithRetryStatement().formatted("events");
        assertThat(sql).contains("UPDATE events");
        assertThat(sql).contains("CASE WHEN retry THEN retry_count ELSE retry_count + 1 END");
    }
}
