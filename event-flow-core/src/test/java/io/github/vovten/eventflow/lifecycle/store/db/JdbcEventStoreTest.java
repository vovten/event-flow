package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JdbcEventStore Tests")
class JdbcEventStoreTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        dataSource = createH2DataSource("jdbc:h2:mem:eventflow-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    private static DataSource createH2DataSource(String url) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    @Test
    @DisplayName("Should create table with default name")
    void shouldCreateTableWithDefaultName() throws SQLException {
        new JdbcEventStore(dataSource);
        assertThat(tableExists("EVENT_STORE")).isTrue();
    }

    @Test
    @DisplayName("Should create table with custom name")
    void shouldCreateTableWithCustomName() throws SQLException {
        new JdbcEventStore(dataSource, "my_events");
        assertThat(tableExists("my_events")).isTrue();
        assertThat(tableExists("EVENT_STORE")).isFalse();
    }

    @Test
    @DisplayName("Should fail at construction when autoInitSchema is false and table does not exist")
    void shouldFailWhenTableDoesNotExistOnConstruction() {
        assertThatThrownBy(() -> new JdbcEventStore(dataSource, "event_store", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    @DisplayName("Should succeed when autoInitSchema is false and table already exists")
    void shouldSucceedWhenTableExistsAutoInitFalse() throws SQLException {
        new JdbcEventStore(dataSource, "event_store", true);
        assertThat(tableExists("EVENT_STORE")).isTrue();

        // Now create a second store with autoInitSchema=false — should not throw
        new JdbcEventStore(dataSource, "event_store", false);
    }

    @Test
    @DisplayName("Should save and find event by ID")
    void shouldSaveAndFindById() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        StoredEvent event = StoredEvent.newEvent(eventId, "test.TestEvent", null, "{\"data\":\"test\"}", null);

        store.save(event);

        Optional<StoredEvent> found = store.findById(eventId);
        assertThat(found).isPresent();
        assertThat(found.get().eventId()).isEqualTo(eventId);
        assertThat(found.get().eventType()).isEqualTo("test.TestEvent");
        assertThat(found.get().payload()).isEqualTo("{\"data\":\"test\"}");
        assertThat(found.get().status()).isEqualTo(EventStatus.NEW);
        assertThat(found.get().retryCount()).isZero();
        assertThat(found.get().createdAt()).isNotNull();
        assertThat(found.get().updatedAt()).isNotNull();
    }

    @Test
    @DisplayName("Should return empty when event not found")
    void shouldReturnEmptyWhenNotFound() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        assertThat(store.findById(UUID.randomUUID())).isEmpty();
    }

    @Test
    @DisplayName("Should throw on duplicate event ID")
    void shouldThrowOnDuplicate() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        StoredEvent event = StoredEvent.newEvent(eventId, "test.T", null, "{}", null);

        store.save(event);
        assertThatThrownBy(() -> store.save(event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(eventId.toString());
    }

    @Test
    @DisplayName("Should update status to PUBLISHED")
    void shouldUpdateStatus() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));

        store.updateStatus(eventId, EventStatus.PUBLISHED, null);

        StoredEvent updated = store.findById(eventId).orElseThrow();
        assertThat(updated.status()).isEqualTo(EventStatus.PUBLISHED);
        assertThat(updated.errorDetails()).isNull();
        assertThat(updated.updatedAt()).isAfter(updated.createdAt());
    }

    @Test
    @DisplayName("Should update status with error details")
    void shouldUpdateStatusWithError() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));

        store.updateStatus(eventId, EventStatus.FAILED, "timeout");

        StoredEvent updated = store.findById(eventId).orElseThrow();
        assertThat(updated.status()).isEqualTo(EventStatus.FAILED);
        assertThat(updated.errorDetails()).isEqualTo("timeout");
    }

    @Test
    @DisplayName("Should increment retry count on NEW status update")
    void shouldIncrementRetryOnNewStatus() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));

        store.updateStatus(eventId, EventStatus.FAILED, "err");
        store.updateStatus(eventId, EventStatus.NEW, null);

        StoredEvent updated = store.findById(eventId).orElseThrow();
        assertThat(updated.status()).isEqualTo(EventStatus.NEW);
        assertThat(updated.retryCount()).isEqualTo(1);
        assertThat(updated.errorDetails()).isNull();
    }

    @Test
    @DisplayName("Should throw when updating non-existent event")
    void shouldThrowWhenUpdatingNonExistent() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        assertThatThrownBy(() ->
                store.updateStatus(UUID.randomUUID(), EventStatus.PUBLISHED, null)
        ).isInstanceOf(NoSuchElementException.class);
    }

    @Test
    @DisplayName("Should limit results in findByStatuses with limit parameter")
    void shouldLimitFindByStatuses() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID[] ids = new UUID[5];
        for (int i = 0; i < 5; i++) {
            ids[i] = UUID.randomUUID();
            store.save(StoredEvent.newEvent(ids[i], "test.T", null, "{}", null));
            store.updateStatus(ids[i], EventStatus.FAILED, "err");
        }

        Instant deadline = Instant.now().plusSeconds(1);
        List<StoredEvent> limited = store.findByStatuses(
                List.of(EventStatus.FAILED), deadline, 2);

        assertThat(limited).hasSize(2);
    }

    @Test
    @DisplayName("Should filter retryable events by service when service is provided")
    void shouldFilterRetryableEventsByService() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID ownId = UUID.randomUUID();
        UUID foreignId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(ownId, "test.A", "svc-a", "{}", null));
        store.save(StoredEvent.newEvent(foreignId, "test.B", "svc-b", "{}", null));
        store.updateStatus(ownId, EventStatus.FAILED, "err");
        store.updateStatus(foreignId, EventStatus.FAILED, "err");

        Instant deadline = Instant.now().plus(1, ChronoUnit.DAYS);
        List<StoredEvent> results = store.findRetryableEvents(
                List.of(EventStatus.FAILED), deadline, 10, "svc-a");

        assertThat(results).hasSize(1);
        assertThat(results.getFirst().eventId()).isEqualTo(ownId);
    }

    @Test
    @DisplayName("Should throw NullPointerException when service is null")
    void shouldRejectNullService() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", "svc-a", "{}", null));
        store.updateStatus(eventId, EventStatus.FAILED, "err");

        Instant deadline = Instant.now().plus(1, ChronoUnit.DAYS);
        assertThatThrownBy(() -> store.findRetryableEvents(
                List.of(EventStatus.FAILED), deadline, 10, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("service must not be null");
    }

    @Test
    @DisplayName("Should return no events when service does not match")
    void shouldReturnEmptyWhenServiceDoesNotMatch() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", "svc-a", "{}", null));
        store.updateStatus(eventId, EventStatus.FAILED, "err");

        Instant deadline = Instant.now().plus(1, ChronoUnit.DAYS);
        assertThat(store.findRetryableEvents(
                List.of(EventStatus.FAILED), deadline, 10, "other-service")).isEmpty();
    }

    @Test
    @DisplayName("Should find events by status and age")
    void shouldFindByStatusAndAge() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        store.updateStatus(eventId, EventStatus.FAILED, "err");

        Instant now = Instant.now();

        // Should not find events newer than 'before' (event was just updated)
        Instant justBeforeNow = now.minus(1, ChronoUnit.MILLIS);
        assertThat(store.findByStatus(EventStatus.FAILED, justBeforeNow)).isEmpty();

        // Should find events older than 'before' when queried with a future deadline
        Instant farFuture = now.plus(1, ChronoUnit.DAYS);
        List<StoredEvent> results = store.findByStatus(EventStatus.FAILED, farFuture);
        assertThat(results).hasSize(1);
        assertThat(results.getFirst().eventId()).isEqualTo(eventId);
    }

    @Test
    @DisplayName("Should return empty when no events match status")
    void shouldReturnEmptyWhenNoMatch() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        store.save(StoredEvent.newEvent(UUID.randomUUID(), "test.T", null, "{}", null));

        List<StoredEvent> results = store.findByStatus(EventStatus.HANDLED, Instant.now().plus(1, ChronoUnit.DAYS));
        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("Should work with custom table name")
    void shouldWorkWithCustomTableName() {
        JdbcEventStore store = new JdbcEventStore(dataSource, "my_events");
        UUID eventId = UUID.randomUUID();
        StoredEvent event = StoredEvent.newEvent(eventId, "test.T", null, "{}", null);

        store.save(event);
        assertThat(store.findById(eventId)).isPresent();

        store.updateStatus(eventId, EventStatus.HANDLED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("Should handle full event lifecycle")
    void shouldHandleFullLifecycle() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();

        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.NEW);

        store.updateStatus(eventId, EventStatus.FAILED, "err");
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.FAILED);

        store.updateStatus(eventId, EventStatus.NEW, null);
        assertThat(store.findById(eventId).orElseThrow().retryCount()).isEqualTo(1);

        store.updateStatus(eventId, EventStatus.PUBLISHED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.PUBLISHED);

        store.updateStatus(eventId, EventStatus.HANDLED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("Should store and retrieve process ID")
    void shouldStoreProcessId() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        UUID processId = UUID.randomUUID();
        StoredEvent event = StoredEvent.newEvent(eventId, "test.T", null, "{}", processId);

        store.save(event);
        StoredEvent found = store.findById(eventId).orElseThrow();
        assertThat(found.processId()).isEqualTo(processId);
    }

    @Test
    @DisplayName("Should work with BINARY(16) UUID strategy")
    void shouldWorkWithBinaryUuidStrategy() {
        JdbcEventStore store = new JdbcEventStore(dataSource, "event_store", UuidType.BINARY);
        UUID eventId = UUID.randomUUID();
        UUID processId = UUID.randomUUID();
        StoredEvent event = StoredEvent.newEvent(eventId, "test.BinaryEvent", null,
                "{\"data\":\"binary-test\"}", processId);

        store.save(event);

        StoredEvent found = store.findById(eventId).orElseThrow();
        assertThat(found.eventId()).isEqualTo(eventId);
        assertThat(found.eventType()).isEqualTo("test.BinaryEvent");
        assertThat(found.payload()).isEqualTo("{\"data\":\"binary-test\"}");
        assertThat(found.processId()).isEqualTo(processId);
        assertThat(found.status()).isEqualTo(EventStatus.NEW);
        assertThat(found.retryCount()).isZero();
    }

    @Test
    @DisplayName("Should handle full lifecycle with BINARY(16) UUID strategy")
    void shouldHandleFullLifecycleWithBinaryUuid() {
        JdbcEventStore store = new JdbcEventStore(dataSource, "event_store_bin", UuidType.BINARY);
        UUID eventId = UUID.randomUUID();

        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.NEW);

        store.updateStatus(eventId, EventStatus.FAILED, "err");
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.FAILED);

        store.updateStatus(eventId, EventStatus.NEW, null);
        assertThat(store.findById(eventId).orElseThrow().retryCount()).isEqualTo(1);

        store.updateStatus(eventId, EventStatus.PUBLISHED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.PUBLISHED);

        store.updateStatus(eventId, EventStatus.HANDLED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.HANDLED);
    }

    @Test
    @DisplayName("Should work with schema-qualified table name and create column comments")
    void shouldWorkWithSchemaQualifiedTableName() throws SQLException {
        // Create the schema before store initialization
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS myschema");
        }

        String qualifiedName = "myschema.event_store";
        JdbcEventStore store = new JdbcEventStore(dataSource, qualifiedName);

        // Basic CRUD should work
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        assertThat(store.findById(eventId)).isPresent();

        store.updateStatus(eventId, EventStatus.PUBLISHED, null);
        assertThat(store.findById(eventId).orElseThrow().status()).isEqualTo(EventStatus.PUBLISHED);

        // Verify column comments were created
        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT REMARKS FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = 'MYSCHEMA' AND TABLE_NAME = 'EVENT_STORE'");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("REMARKS"))
                    .isEqualTo("Event store for persistent event lifecycle tracking");

            rs = st.executeQuery(
                    "SELECT COLUMN_NAME, REMARKS FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE TABLE_SCHEMA = 'MYSCHEMA' AND TABLE_NAME = 'EVENT_STORE' " +
                    "AND REMARKS IS NOT NULL");
            // Should have at least 10 column comments
            int commentCount = 0;
            while (rs.next()) {
                commentCount++;
                assertThat(rs.getString("REMARKS")).isNotBlank();
            }
            assertThat(commentCount).isGreaterThanOrEqualTo(10);
        }
    }

    @Test
    @DisplayName("Should delete events by statuses before deadline")
    void shouldDeleteByStatuses() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        store.updateStatus(eventId, EventStatus.HANDLED, null);

        UUID eventId2 = UUID.randomUUID();
        StoredEvent event2 = StoredEvent.newEvent(eventId2, "test.T", null, "{}", null);
        store.save(event2);
        store.updateStatus(eventId2, EventStatus.UNDEFINED, null);

        UUID eventId3 = UUID.randomUUID();
        StoredEvent event3 = StoredEvent.newEvent(eventId3, "test.T", null, "{}", null);
        store.save(event3);
        store.updateStatus(eventId3, EventStatus.FAILED, "error");

        Instant deadline = Instant.now().plusSeconds(1);
        int deleted = store.deleteByStatuses(
                List.of(EventStatus.HANDLED, EventStatus.UNDEFINED), deadline, 100);

        assertThat(deleted).isEqualTo(2);
        assertThat(store.findById(eventId)).isEmpty();
        assertThat(store.findById(eventId2)).isEmpty();
        assertThat(store.findById(eventId3)).isPresent();
        assertThat(store.findById(eventId3).orElseThrow().status()).isEqualTo(EventStatus.FAILED);
    }

    @Test
    @DisplayName("Should delete nothing when no events match statuses")
    void shouldDeleteNothingWhenNoMatch() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        store.updateStatus(eventId, EventStatus.PUBLISHED, null);

        Instant deadline = Instant.now().plusSeconds(1);
        int deleted = store.deleteByStatuses(
                List.of(EventStatus.HANDLED, EventStatus.UNDEFINED), deadline, 100);

        assertThat(deleted).isZero();
        assertThat(store.findById(eventId)).isPresent();
    }

    @Test
    @DisplayName("Should delete nothing when deadline is in the past")
    void shouldDeleteNothingWhenDeadlineInPast() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        store.updateStatus(eventId, EventStatus.HANDLED, null);

        Instant deadline = Instant.now().minusSeconds(1);
        int deleted = store.deleteByStatuses(
                List.of(EventStatus.HANDLED), deadline, 100);

        assertThat(deleted).isZero();
        assertThat(store.findById(eventId)).isPresent();
    }

    @Test
    @DisplayName("Should delete no more than batch size in a single call")
    void shouldDeleteAtMostBatchSize() {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID[] ids = new UUID[5];
        for (int i = 0; i < 5; i++) {
            ids[i] = UUID.randomUUID();
            store.save(StoredEvent.newEvent(ids[i], "test.T", null, "{}", null));
            store.updateStatus(ids[i], EventStatus.HANDLED, null);
        }

        Instant deadline = Instant.now().plusSeconds(1);
        // batchSize = 2 — deletes at most 2 events in one call
        int deleted = store.deleteByStatuses(List.of(EventStatus.HANDLED), deadline, 2);

        assertThat(deleted).isEqualTo(2);
        // Remaining 3 events should still be in the store
        int remaining = 0;
        for (UUID id : ids) {
            if (store.findById(id).isPresent()) {
                remaining++;
            }
        }
        assertThat(remaining).isEqualTo(3);
    }

    @Test
    @DisplayName("Should create table with default name and verify column comments")
    void shouldCreateTableWithDefaultNameAndComments() throws SQLException {
        new JdbcEventStore(dataSource);
        assertThat(tableExists("EVENT_STORE")).isTrue();

        try (Connection conn = dataSource.getConnection();
                Statement st = conn.createStatement()) {
            ResultSet rs = st.executeQuery(
                    "SELECT REMARKS FROM INFORMATION_SCHEMA.TABLES " +
                    "WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_NAME = 'EVENT_STORE'");
            assertThat(rs.next()).isTrue();
            assertThat(rs.getString("REMARKS"))
                    .isEqualTo("Event store for persistent event lifecycle tracking");
        }
    }

    private boolean tableExists(String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
                if (rs.next()) return true;
            }
            try (ResultSet rs = meta.getTables(null, null, tableName.toUpperCase(), null)) {
                if (rs.next()) return true;
            }
            try (ResultSet rs = meta.getTables(null, null, tableName.toLowerCase(), null)) {
                return rs.next();
            }
        }
    }
}
