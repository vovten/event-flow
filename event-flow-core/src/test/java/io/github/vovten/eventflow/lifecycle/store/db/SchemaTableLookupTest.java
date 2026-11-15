package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.DatabaseDialect;
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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for schema-qualified table lookup (DEFECT-2.3): a table with
 * the same base name in another schema must not be treated as the target table.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("Schema Table Lookup Tests")
class SchemaTableLookupTest {

    private static final SqlDialect POSTGRES = SqlDialect.forDialect(DatabaseDialect.POSTGRESQL);

    @Test
    @DisplayName("DEFECT-2.3: Does not skip creation when a same-named table exists in another schema")
    void shouldCreateTableInTargetSchema() throws Exception {
        // The table exists only in "probe_a"; the target is "probe_b".
        MockJdbc jdbc = new MockJdbc("probe_a", null);

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, "probe_b.event_store",
                POSTGRES, UuidType.BINARY);
        initializer.ensureSchema();

        List<String> executed = capturedSql(jdbc.statement);
        assertThat(executed)
                .as("A table in another schema must not suppress creation in the target schema")
                .anyMatch(sql -> sql.contains("CREATE TABLE") && sql.contains("probe_b.event_store"));
    }

    @Test
    @DisplayName("DEFECT-2.3: Verifies a qualified table only in its own schema")
    void shouldVerifyQualifiedTableInItsSchema() throws Exception {
        MockJdbc jdbc = new MockJdbc("probe_a", null);

        assertThatThrownBy(() -> new SchemaInitializer(jdbc.dataSource, "probe_b.event_store",
                POSTGRES, UuidType.BINARY).verifyTableExists())
                .as("probe_a.event_store must not satisfy verification of probe_b.event_store")
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");

        assertThatCode(() -> new SchemaInitializer(jdbc.dataSource, "probe_a.event_store",
                POSTGRES, UuidType.BINARY).verifyTableExists())
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("DEFECT-2.3: Unqualified names resolve against the current connection schema")
    void shouldResolveUnqualifiedNameInCurrentSchema() throws Exception {
        MockJdbc jdbc = new MockJdbc("app", "app");

        assertThatCode(() -> new SchemaInitializer(jdbc.dataSource, "event_store",
                POSTGRES, UuidType.BINARY).verifyTableExists())
                .doesNotThrowAnyException();

        MockJdbc otherSchema = new MockJdbc("app", "other");
        assertThatThrownBy(() -> new SchemaInitializer(otherSchema.dataSource, "event_store",
                POSTGRES, UuidType.BINARY).verifyTableExists())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
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
     * Mock JDBC graph where a table with the base name exists only in the given
     * schema (matched as a schema pattern or as a catalog, depending on the
     * driver conventions).
     */
    private static final class MockJdbc {

        private final DataSource dataSource = mock(DataSource.class);
        private final Connection connection = mock(Connection.class);
        private final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        private final Statement statement = mock(Statement.class);

        private MockJdbc(String schemaWithTable, String currentSchema) throws SQLException {
            when(dataSource.getConnection()).thenReturn(connection);
            when(connection.getMetaData()).thenReturn(metadata);
            when(connection.createStatement()).thenReturn(statement);
            when(connection.getSchema()).thenReturn(currentSchema);
            when(metadata.getTables(nullable(String.class), nullable(String.class),
                    anyString(), nullable(String[].class)))
                    .thenAnswer(invocation -> {
                        String catalog = invocation.getArgument(0);
                        String schemaPattern = invocation.getArgument(1);
                        // An unrestricted lookup (the old buggy behaviour) finds
                        // the table in any schema, including the foreign one.
                        boolean unrestricted = catalog == null && schemaPattern == null;
                        boolean exists = unrestricted
                                || schemaWithTable.equalsIgnoreCase(catalog)
                                || schemaWithTable.equalsIgnoreCase(schemaPattern);
                        return resultSet(exists);
                    });
        }
    }
}
