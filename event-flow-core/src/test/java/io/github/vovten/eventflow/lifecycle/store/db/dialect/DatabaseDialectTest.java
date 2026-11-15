package io.github.vovten.eventflow.lifecycle.store.db.dialect;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DatabaseDialect} detection.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("DatabaseDialect Detection Tests")
class DatabaseDialectTest {

    @Test
    @DisplayName("Should detect every supported database family")
    void shouldDetectSupportedDatabases() throws SQLException {
        assertThat(detect("PostgreSQL")).isEqualTo(DatabaseDialect.POSTGRESQL);
        assertThat(detect("CockroachDB")).isEqualTo(DatabaseDialect.POSTGRESQL);
        assertThat(detect("YugabyteDB")).isEqualTo(DatabaseDialect.POSTGRESQL);
        assertThat(detect("MySQL")).isEqualTo(DatabaseDialect.MYSQL);
        assertThat(detect("MariaDB")).isEqualTo(DatabaseDialect.MYSQL);
        assertThat(detect("Microsoft SQL Server")).isEqualTo(DatabaseDialect.SQLSERVER);
        assertThat(detect("H2")).isEqualTo(DatabaseDialect.H2);
        assertThat(detect("Oracle")).isEqualTo(DatabaseDialect.ORACLE);
    }

    @Test
    @DisplayName("DEFECT-2.6: Should fail fast for an unsupported database instead of assuming PostgreSQL")
    void shouldFailFastForUnsupportedDatabase() throws SQLException {
        assertThatThrownBy(() -> detect("SQLite"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported database")
                .hasMessageContaining("SQLite");
    }

    @Test
    @DisplayName("DEFECT-2.6: Should fail fast when the product name is missing")
    void shouldFailFastWhenProductNameIsMissing() throws SQLException {
        assertThatThrownBy(() -> detect(null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unsupported database");
    }

    private static DatabaseDialect detect(String productName) throws SQLException {
        Connection connection = mock(Connection.class);
        DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        when(connection.getMetaData()).thenReturn(metadata);
        when(metadata.getDatabaseProductName()).thenReturn(productName);
        return DatabaseDialect.detect(connection);
    }
}
