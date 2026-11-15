package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.db.dialect.DatabaseDialect;
import io.github.vovten.eventflow.lifecycle.store.db.dialect.SqlDialect;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the schema-init recovery on a poisoned transaction:
 * PostgreSQL aborts the whole transaction after any statement error (for
 * example the pg_type race between concurrent CREATE TABLE statements), and
 * the recovery must isolate the failed statement by rolling back to a
 * savepoint before touching the connection again.
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 */
@DisplayName("Schema DDL Recovery Tests")
class SchemaDdlRecoveryTest {

    @Test
    @DisplayName("DEFECT: Recovers from the pg_type race by rolling back to a savepoint before the re-check")
    void shouldRollbackToSavepointBeforeRecoveryRecheck() throws Exception {
        PoisonedJdbc jdbc = new PoisonedJdbc();
        // The CREATE TABLE hits the classic PostgreSQL pg_type race; the
        // rival instance wins and the recovery re-check sees its table.
        jdbc.failCreateTableWith(
                new SQLException("duplicate key value violates unique constraint \"pg_type_typname_nsp_index\"",
                        "23505", 0));

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, "event_store",
                SqlDialect.forDialect(DatabaseDialect.POSTGRESQL), UuidType.BINARY);

        assertThatCode(initializer::ensureSchema).doesNotThrowAnyException();

        verify(jdbc.connection).rollback(any(Savepoint.class));
    }

    @Test
    @DisplayName("DEFECT: A duplicate index with an unrecognized error code is re-checked without aborting the transaction")
    void shouldIsolateFailedIndexStatement() throws Exception {
        PoisonedJdbc jdbc = new PoisonedJdbc();
        // The first index fails as an already-existing object with a SQLState
        // the duplicate-object matcher does not know: the existence re-check
        // runs after the savepoint rollback and must not see an aborted
        // transaction; the second index statement must still execute.
        jdbc.failIndexesWith(
                new SQLException("relation already exists", null, 0));
        jdbc.indexExists(true);

        SchemaInitializer initializer = new SchemaInitializer(jdbc.dataSource, "event_store",
                SqlDialect.forDialect(DatabaseDialect.POSTGRESQL), UuidType.BINARY);

        assertThatCode(initializer::ensureSchema).doesNotThrowAnyException();

        verify(jdbc.connection, atLeastOnce()).rollback(any(Savepoint.class));
        verify(jdbc.statement).execute(contains("idx_event_store_service"));
    }

    /**
     * Mock JDBC graph modeling PostgreSQL transaction semantics: once a DDL
     * statement has failed, every metadata call fails with "current transaction
     * is aborted" until the connection is rolled back (to a savepoint or
     * entirely). A rollback performed during recovery also simulates the rival
     * instance having created the table, mirroring the concurrent-start race.
     */
    private static final class PoisonedJdbc {

        private final DataSource dataSource = mock(DataSource.class);
        private final Connection connection = mock(Connection.class);
        private final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        private final Statement statement = mock(Statement.class);
        private final AtomicBoolean transactionAborted = new AtomicBoolean(false);
        private final AtomicBoolean tableCreatedByRival = new AtomicBoolean(false);
        private final AtomicBoolean createTableFailed = new AtomicBoolean(false);

        private PoisonedJdbc() throws SQLException {
            when(dataSource.getConnection()).thenReturn(connection);
            when(connection.getMetaData()).thenReturn(metadata);
            when(connection.createStatement()).thenReturn(statement);
            when(connection.setSavepoint()).thenReturn(mock(Savepoint.class));
            when(connection.getSchema()).thenReturn("public");
            doAnswer(invocation -> {
                transactionAborted.set(false);
                tableCreatedByRival.set(createTableFailed.get());
                return null;
            }).when(connection).rollback(any(Savepoint.class));
            doAnswer(invocation -> {
                transactionAborted.set(false);
                return null;
            }).when(connection).rollback();
            when(metadata.getTables(nullable(String.class), nullable(String.class),
                    anyString(), nullable(String[].class)))
                    .thenAnswer(invocation -> {
                        if (transactionAborted.get()) {
                            throw new SQLException(
                                    "current transaction is aborted, commands ignored until end of transaction block",
                                    "25P02", 0);
                        }
                        ResultSet rs = mock(ResultSet.class);
                        when(rs.next()).thenReturn(tableCreatedByRival.get());
                        return rs;
                    });
        }

        private void failCreateTableWith(SQLException error) throws SQLException {
            createTableFailed.set(true);
            doAnswer(invocation -> {
                transactionAborted.set(true);
                throw error;
            }).when(statement).execute(contains("CREATE TABLE"));
        }

        private void failIndexesWith(SQLException error) throws SQLException {
            doAnswer(invocation -> {
                transactionAborted.set(true);
                throw error;
            }).when(statement).execute(contains("CREATE INDEX"));
        }

        /**
         * Stubs the index-existence re-check (used when the error code is not
         * recognized as a duplicate-object error): the metadata lists every
         * index of the table, so any already-created index is found.
         */
        private void indexExists(boolean exists) throws SQLException {
            ResultSet rs = mock(ResultSet.class);
            if (exists) {
                when(rs.next()).thenReturn(true, true, false);
                when(rs.getString("INDEX_NAME"))
                        .thenReturn("idx_event_store_status", "idx_event_store_service");
            } else {
                when(rs.next()).thenReturn(false);
            }
            when(metadata.getIndexInfo(nullable(String.class), nullable(String.class),
                    anyString(), anyBoolean(), anyBoolean()))
                    .thenAnswer(invocation -> {
                        if (transactionAborted.get()) {
                            throw new SQLException(
                                    "current transaction is aborted, commands ignored until end of transaction block",
                                    "25P02", 0);
                        }
                        return rs;
                    });
        }
    }
}
