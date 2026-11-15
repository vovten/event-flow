package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.DatabaseDialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.H2Dialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for concurrent schema initialization (DEFECT-2.1).
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("SchemaInitializer Concurrency Tests")
class SchemaInitializerConcurrencyTest {

    private static final String TABLE = "event_store";

    @Test
    @DisplayName("DEFECT-2.1: PostgreSQL DDL uses IF NOT EXISTS for the table and indexes")
    void shouldUseIdempotentDdlForPostgres() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, TABLE,
                SqlDialect.forDialect(DatabaseDialect.POSTGRESQL), UuidType.BINARY);
        initializer.ensureSchema();

        List<String> executed = capturedSql(jdbc.statement);
        assertThat(executed).anyMatch(sql -> sql.contains("CREATE TABLE IF NOT EXISTS " + TABLE));
        assertThat(executed).anyMatch(sql -> sql.contains("CREATE INDEX IF NOT EXISTS idx_event_store_status"));
        assertThat(executed).anyMatch(sql -> sql.contains("CREATE INDEX IF NOT EXISTS idx_event_store_service"));
    }

    @Test
    @DisplayName("DEFECT-2.1: Recovers when another instance creates the table between check and DDL")
    void shouldRecoverWhenTableIsCreatedConcurrently() throws Exception {
        MockJdbc jdbc = new MockJdbc();
        ResultSet emptyBase = resultSet(false);
        ResultSet emptyUpper = resultSet(false);
        ResultSet emptyLower = resultSet(false);
        ResultSet exists = resultSet(true);
        // Pre-check (base/upper/lower) sees no table; the re-check after the
        // failed CREATE TABLE sees the table created by the rival instance.
        when(jdbc.metadata.getTables(nullable(String.class), nullable(String.class),
                anyString(), nullable(String[].class)))
                .thenReturn(emptyBase, emptyUpper, emptyLower, exists);
        doThrow(new SQLException("There is already an object named 'event_store'", "42S01", 2714))
                .when(jdbc.statement).execute(contains("CREATE TABLE"));

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, TABLE,
                SqlDialect.forDialect(DatabaseDialect.SQLSERVER), UuidType.BINARY);

        assertThatCode(initializer::ensureSchema).doesNotThrowAnyException();

        verify(jdbc.connection).commit();
        assertThat(capturedSql(jdbc.statement))
                .as("The winning instance owns the indexes; the recovered instance must not create them")
                .noneMatch(sql -> sql.contains("CREATE INDEX"));
    }

    @Test
    @DisplayName("DEFECT-2.1: Propagates a non-recoverable DDL failure instead of masking it")
    void shouldPropagateNonRecoverableDdlFailure() throws Exception {
        MockJdbc jdbc = new MockJdbc();
        ResultSet empty = resultSet(false);
        when(jdbc.metadata.getTables(nullable(String.class), nullable(String.class),
                anyString(), nullable(String[].class)))
                .thenReturn(empty);
        doThrow(new SQLException("permission denied", "42501", 0))
                .when(jdbc.statement).execute(contains("CREATE TABLE"));

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, TABLE,
                SqlDialect.forDialect(DatabaseDialect.POSTGRESQL), UuidType.BINARY);

        assertThatThrownBy(initializer::ensureSchema)
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(SQLException.class);
        verify(jdbc.connection).rollback();
    }

    @Test
    @DisplayName("DEFECT-2.1: The retry table DDL uses IF NOT EXISTS for the table and indexes")
    void shouldUseIdempotentDdlForRetryTable() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        RetrySchemaInitializer initializer = new RetrySchemaInitializer(jdbc.dataSource, "event_retry",
                SqlDialect.forDialect(DatabaseDialect.POSTGRESQL), UuidType.NATIVE);
        initializer.ensureSchema();

        List<String> executed = capturedSql(jdbc.statement);
        assertThat(executed).anyMatch(sql -> sql.contains("CREATE TABLE IF NOT EXISTS event_retry"));
        assertThat(executed).anyMatch(sql -> sql.contains("CREATE INDEX IF NOT EXISTS idx_event_retry_due"));
        assertThat(executed).anyMatch(sql -> sql.contains(
                "CREATE INDEX IF NOT EXISTS idx_event_retry_service_due"));
    }

    @Test
    @DisplayName("DEFECT-2.7: Does not emit COMMENT ON statements for MySQL")
    void shouldSkipCommentOnForMySql() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        new SchemaInitializer(jdbc.dataSource, TABLE,
                SqlDialect.forDialect(DatabaseDialect.MYSQL), UuidType.BINARY).ensureSchema();

        assertThat(capturedSql(jdbc.statement))
                .as("MySQL does not support COMMENT ON; no noisy statements must be executed")
                .noneMatch(sql -> sql.contains("COMMENT ON"));
    }

    @Test
    @DisplayName("DEFECT-2.7: Does not emit COMMENT ON statements for the MySQL retry table")
    void shouldSkipRetryCommentOnForMySql() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        new RetrySchemaInitializer(jdbc.dataSource, "event_retry",
                SqlDialect.forDialect(DatabaseDialect.MYSQL), UuidType.BINARY).ensureSchema();

        assertThat(capturedSql(jdbc.statement)).noneMatch(sql -> sql.contains("COMMENT ON"));
    }

    @Test
    @DisplayName("DEFECT-2.7: Still emits COMMENT ON for databases that support it")
    void shouldEmitCommentOnForPostgres() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        new SchemaInitializer(jdbc.dataSource, TABLE,
                SqlDialect.forDialect(DatabaseDialect.POSTGRESQL), UuidType.BINARY).ensureSchema();

        assertThat(capturedSql(jdbc.statement)).anyMatch(sql -> sql.contains("COMMENT ON"));
    }

    @Test
    @DisplayName("DEFECT-2.1: Concurrent initialization against a real database does not fail")
    void shouldInitializeSchemaConcurrently() throws Exception {
        JdbcDataSource dataSource = inMemoryDataSource();
        int instances = 8;
        CyclicBarrier barrier = new CyclicBarrier(instances);
        ExecutorService pool = Executors.newFixedThreadPool(instances);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < instances; i++) {
                futures.add(pool.submit(() -> {
                    barrier.await(10, TimeUnit.SECONDS);
                    new SchemaInitializer(dataSource, TABLE, H2Dialect.INSTANCE, UuidType.NATIVE)
                            .ensureSchema();
                    return null;
                }));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        try (Connection conn = dataSource.getConnection()) {
            assertThat(SchemaSupport.tableExists(conn, TABLE)).isTrue();
        }
    }

    private static List<String> capturedSql(Statement statement) throws Exception {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(statement, atLeastOnce()).execute(captor.capture());
        return captor.getAllValues();
    }

    private static ResultSet resultSet(boolean exists) throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(exists);
        return rs;
    }

    private static JdbcDataSource inMemoryDataSource() {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:schema-race-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    /**
     * Mock JDBC graph: a DataSource over a connection whose table does not exist
     * until the test says otherwise.
     */
    private static final class MockJdbc {

        private final DataSource dataSource = mock(DataSource.class);
        private final Connection connection = mock(Connection.class);
        private final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        private final Statement statement = mock(Statement.class);

        private MockJdbc() throws SQLException {
            ResultSet empty = resultSet(false);
            when(dataSource.getConnection()).thenReturn(connection);
            when(connection.getMetaData()).thenReturn(metadata);
            when(connection.createStatement()).thenReturn(statement);
            when(metadata.getTables(nullable(String.class), nullable(String.class),
                    anyString(), nullable(String[].class)))
                    .thenReturn(empty);
        }
    }
}
