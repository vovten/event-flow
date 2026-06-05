package io.github.vovten.eventflow.lifecycle.store;

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
    @DisplayName("Should find events by status and age")
    void shouldFindByStatusAndAge() throws InterruptedException {
        JdbcEventStore store = new JdbcEventStore(dataSource);
        UUID eventId = UUID.randomUUID();
        store.save(StoredEvent.newEvent(eventId, "test.T", null, "{}", null));
        store.updateStatus(eventId, EventStatus.FAILED, "err");

        // Should not find events newer than 'before'
        Instant recent = Instant.now().minus(1, ChronoUnit.MILLIS);
        assertThat(store.findByStatus(EventStatus.FAILED, recent)).isEmpty();

        // Should find events older than 'before'
        Thread.sleep(10); // ensure freshness
        Instant future = Instant.now().plus(1, ChronoUnit.DAYS);
        List<StoredEvent> results = store.findByStatus(EventStatus.FAILED, future);
        assertThat(results).hasSize(1);
        assertThat(results.get(0).eventId()).isEqualTo(eventId);
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
        JdbcEventStore store = new JdbcEventStore(dataSource, "event_store", JdbcEventStore.UuidType.BINARY);
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
        JdbcEventStore store = new JdbcEventStore(dataSource, "event_store_bin", JdbcEventStore.UuidType.BINARY);
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
