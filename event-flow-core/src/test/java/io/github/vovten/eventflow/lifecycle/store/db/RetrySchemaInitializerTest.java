package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.H2Dialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetrySchemaInitializer Tests")
class RetrySchemaInitializerTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        dataSource = createH2DataSource("jdbc:h2:mem:retryschema-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    @DisplayName("Should create the retry table and indexes")
    void shouldCreateTableAndIndexes() throws SQLException {
        initializer("event_retry").ensureSchema();

        assertThat(tableExists("EVENT_RETRY")).isTrue();
        assertThat(indexExists("EVENT_RETRY", "IDX_EVENT_RETRY_DUE")).isTrue();
        assertThat(indexExists("EVENT_RETRY", "IDX_EVENT_RETRY_SERVICE_DUE")).isTrue();
    }

    @Test
    @DisplayName("Should create a table with a custom name")
    void shouldCreateCustomTable() throws SQLException {
        initializer("my_retry").ensureSchema();

        assertThat(tableExists("MY_RETRY")).isTrue();
        assertThat(indexExists("MY_RETRY", "IDX_MY_RETRY_DUE")).isTrue();
    }

    @Test
    @DisplayName("Should verify an existing table")
    void shouldVerifyExistingTable() {
        RetrySchemaInitializer initializer = initializer("event_retry");
        initializer.ensureSchema();

        initializer.verifyTableExists();
    }

    @Test
    @DisplayName("Should fail verification when the table is missing")
    void shouldFailWhenTableMissing() {
        assertThatThrownBy(() -> initializer("event_retry").verifyTableExists())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
    }

    private RetrySchemaInitializer initializer(String tableName) {
        return new RetrySchemaInitializer(dataSource, tableName, H2Dialect.INSTANCE, UuidType.NATIVE);
    }

    private boolean tableExists(String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
                return rs.next();
            }
        }
    }

    private boolean indexExists(String tableName, String indexName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getIndexInfo(null, null, tableName, false, false)) {
                while (rs.next()) {
                    String found = rs.getString("INDEX_NAME");
                    if (found != null && found.equalsIgnoreCase(indexName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static DataSource createH2DataSource(String url) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }
}
