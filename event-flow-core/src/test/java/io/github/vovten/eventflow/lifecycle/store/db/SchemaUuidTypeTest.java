package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.DatabaseDialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.OracleDialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the UUID column type in the auto-generated DDL
 * (DEFECT-2.2: Oracle must use RAW(16), not the non-existent BINARY(16)).
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("Schema UUID Type Tests")
class SchemaUuidTypeTest {

    @Test
    @DisplayName("DEFECT-2.2: The Oracle event-store DDL uses RAW(16) for UUID columns")
    void shouldUseRawForOracleEventStore() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, "event_store",
                SqlDialect.forDialect(DatabaseDialect.ORACLE), UuidType.BINARY);
        initializer.ensureSchema();

        List<String> executed = capturedSql(jdbc.statement);
        assertThat(executed).anyMatch(sql -> sql.contains("event_id        RAW(16) PRIMARY KEY"));
        assertThat(executed).anyMatch(sql -> sql.contains("process_id      RAW(16)"));
        assertThat(executed).noneMatch(sql -> sql.contains("BINARY(16)"));
    }

    @Test
    @DisplayName("DEFECT-2.2: The Oracle retry-table DDL uses RAW(16) for the UUID column")
    void shouldUseRawForOracleRetryTable() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        RetrySchemaInitializer initializer = new RetrySchemaInitializer(jdbc.dataSource, "event_retry",
                SqlDialect.forDialect(DatabaseDialect.ORACLE), UuidType.BINARY);
        initializer.ensureSchema();

        List<String> executed = capturedSql(jdbc.statement);
        assertThat(executed).anyMatch(sql -> sql.contains("event_id        RAW(16) NOT NULL"));
        assertThat(executed).noneMatch(sql -> sql.contains("BINARY(16)"));
    }

    @Test
    @DisplayName("DEFECT-2.2: Other binary UUID dialects keep BINARY(16)")
    void shouldKeepBinaryForOtherDialects() throws Exception {
        MockJdbc jdbc = new MockJdbc();

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, "event_store",
                SqlDialect.forDialect(DatabaseDialect.MYSQL), UuidType.BINARY);
        initializer.ensureSchema();

        List<String> executed = capturedSql(jdbc.statement);
        assertThat(executed).anyMatch(sql -> sql.contains("BINARY(16)"));
        assertThat(executed).noneMatch(sql -> sql.contains("RAW(16)"));
    }

    @Test
    @DisplayName("DEFECT-2.2: Dialect default maps NATIVE to UUID and BINARY to BINARY(16)")
    void shouldExposeUuidDdlTypePerDialect() {
        assertThat(OracleDialect.INSTANCE.uuidDdlType(UuidType.BINARY)).isEqualTo("RAW(16)");
        assertThat(SqlDialect.forDialect(DatabaseDialect.POSTGRESQL).uuidDdlType(UuidType.NATIVE))
                .isEqualTo("UUID");
        assertThat(SqlDialect.forDialect(DatabaseDialect.MYSQL).uuidDdlType(UuidType.BINARY))
                .isEqualTo("BINARY(16)");
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

    /**
     * Minimal mock JDBC graph: the table does not exist, so the initializer
     * executes its DDL.
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
