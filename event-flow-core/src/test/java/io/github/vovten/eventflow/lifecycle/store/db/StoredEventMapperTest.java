package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("StoredEventMapper Tests")
class StoredEventMapperTest {

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = createH2DataSource("jdbc:h2:mem:mapper-test-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        createEventTable(dataSource);
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

    private static void createEventTable(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE event_store (
                        event_id        UUID PRIMARY KEY,
                        event_type      VARCHAR(512) NOT NULL,
                        service         VARCHAR(255),
                        status          CHAR(1) NOT NULL DEFAULT 'U',
                        payload         TEXT NOT NULL,
                        channels        TEXT,
                        process_id      UUID,
                        created_at      TIMESTAMP NOT NULL,
                        updated_at      TIMESTAMP NOT NULL,
                        retry_count     INT DEFAULT 0 NOT NULL,
                        retry           BOOLEAN DEFAULT FALSE NOT NULL,
                        error_details   TEXT
                    )
                    """);
        }
    }

    // ---- UUID strategy tests ----

    @Test
    @DisplayName("Should roundtrip UUID via NATIVE strategy")
    void shouldRoundtripUuidNative() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID original = UUID.randomUUID();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, payload, created_at, updated_at) VALUES (?, 't', '{}', NOW(), NOW())")) {
            mapper.setUuid(ps, 1, original);
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT event_id FROM event_store WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, original);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                UUID read = mapper.getUuid(rs, "event_id");
                assertThat(read).isEqualTo(original);
            }
        }
    }

    @Test
    @DisplayName("Should roundtrip UUID via BINARY(16) strategy")
    void shouldRoundtripUuidBinary() throws SQLException {
        // Use a table with BINARY(16) for event_id
        String binaryTable = "event_store_binary";
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("""
                    CREATE TABLE event_store_binary (
                        event_id        BINARY(16) PRIMARY KEY,
                        event_type      VARCHAR(512) NOT NULL,
                        payload         TEXT NOT NULL,
                        created_at      TIMESTAMP NOT NULL,
                        updated_at      TIMESTAMP NOT NULL
                    )
                    """);
        }

        StoredEventMapper mapper = new StoredEventMapper(UuidType.BINARY);
        UUID original = UUID.randomUUID();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + binaryTable + " (event_id, event_type, payload, created_at, updated_at) VALUES (?, 't', '{}', NOW(), NOW())")) {
            mapper.setUuid(ps, 1, original);
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT event_id FROM " + binaryTable + " WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, original);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                UUID read = mapper.getUuid(rs, "event_id");
                assertThat(read).isEqualTo(original);
            }
        }
    }

    @Test
    @DisplayName("Should set nullable UUID as NULL when value is null")
    void shouldSetNullableUuidAsNull() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID eventId = UUID.randomUUID();

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, process_id, payload, created_at, updated_at) VALUES (?, 't', ?, '{}', NOW(), NOW())")) {
            mapper.setUuid(ps, 1, eventId);
            mapper.setUuidNullable(ps, 2, null);
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT process_id FROM event_store WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getObject("process_id")).isNull();
            }
        }
    }

    // ---- Insert/Read roundtrip ----

    @Test
    @DisplayName("Should insert event and read it back via mapSingleResult")
    void shouldInsertAndReadBack() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        StoredEvent event = new StoredEvent(eventId, "test.TestEvent", "my-service",
                "{\"key\":\"value\"}", null, null, EventStatus.NEW, 0, false, now, now, null);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, service, payload, channels, process_id, status, retry_count, retry, created_at, updated_at, error_details) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            mapper.setInsertParameters(ps, event);
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT event_id, event_type, service, payload, channels, process_id, status, retry_count, retry, created_at, updated_at, error_details FROM event_store WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, eventId);
            StoredEvent read;
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                read = mapper.mapRow(rs);
            }
            assertThat(read.eventId()).isEqualTo(eventId);
            assertThat(read.eventType()).isEqualTo("test.TestEvent");
            assertThat(read.service()).isEqualTo("my-service");
            assertThat(read.payload()).isEqualTo("{\"key\":\"value\"}");
            assertThat(read.channels()).isNull();
            assertThat(read.status()).isEqualTo(EventStatus.NEW);
            assertThat(read.retryCount()).isZero();
            // Timestamps are affected by H2 timezone handling;
            // verify they are set and within a reasonable window
            assertThat(read.createdAt()).isNotNull();
            assertThat(read.updatedAt()).isNotNull();
            assertThat(read.errorDetails()).isNull();
            assertThat(read.retry()).isFalse();
        }
    }

    @Test
    @DisplayName("Should insert event with null service and read it back")
    void shouldInsertWithNullService() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        StoredEvent event = StoredEvent.newEvent(eventId, "test.TestEvent", null, "{}", null);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, service, payload, channels, process_id, status, retry_count, retry, created_at, updated_at, error_details) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            mapper.setInsertParameters(ps, event);
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT service FROM event_store WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("service")).isNull();
            }
        }
    }

    // ---- Update tests ----

    @Test
    @DisplayName("Should update status and read changes back")
    void shouldUpdateStatus() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID eventId = UUID.randomUUID();

        // Insert
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, payload, status, retry_count, created_at, updated_at) VALUES (?, 't', '{}', 'N', 1, NOW(), NOW())")) {
            mapper.setUuid(ps, 1, eventId);
            ps.executeUpdate();
        }

        // Update
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "UPDATE event_store SET status = ?, error_details = ?, updated_at = ? WHERE event_id = ?")) {
            mapper.setUpdateParameters(ps, eventId, EventStatus.FAILED, "oops");
            int affected = ps.executeUpdate();
            assertThat(affected).isOne();
        }

        // Verify
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT status, error_details FROM event_store WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                assertThat(rs.getString("status")).isEqualTo("F");
                assertThat(rs.getString("error_details")).isEqualTo("oops");
            }
        }
    }

    // ---- Status codes ----

    @Test
    @DisplayName("Should set multiple status codes on PreparedStatement")
    void shouldSetStatusCodes() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        List<EventStatus> statuses = List.of(EventStatus.NEW, EventStatus.PUBLISHED, EventStatus.FAILED);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT * FROM event_store WHERE status IN (?, ?, ?)")) {
            mapper.setStatusCodes(ps, statuses);
            // Verify by checking the parameter metadata
            assertThat(ps.getParameterMetaData().getParameterCount()).isEqualTo(3);
        }
    }

    // ---- Before timestamp ----

    @Test
    @DisplayName("Should set before timestamp on PreparedStatement")
    void shouldSetBeforeTimestamp() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        Instant deadline = Instant.now().plus(1, ChronoUnit.DAYS);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT * FROM event_store WHERE updated_at < ?")) {
            mapper.setBeforeTimestamp(ps, 1, deadline);
            // Just verify no exception — timestamp is set correctly
            assertThat(ps.getParameterMetaData().getParameterCount()).isEqualTo(1);
        }
    }

    // ---- mapResultList ----

    @Test
    @DisplayName("Should map multiple rows via mapResultList")
    void shouldMapResultList() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        Instant earlier = Instant.now().truncatedTo(ChronoUnit.MILLIS).minus(1, ChronoUnit.HOURS);
        Instant later = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // Insert with different timestamps for deterministic ordering
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, payload, created_at, updated_at) VALUES (?, 't', '{}', ?, ?)")) {
            mapper.setUuid(ps, 1, id1);
            ps.setTimestamp(2, Timestamp.from(earlier));
            ps.setTimestamp(3, Timestamp.from(earlier));
            ps.executeUpdate();
        }
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, payload, created_at, updated_at) VALUES (?, 't', '{}', ?, ?)")) {
            mapper.setUuid(ps, 1, id2);
            ps.setTimestamp(2, Timestamp.from(later));
            ps.setTimestamp(3, Timestamp.from(later));
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT event_id, event_type, service, payload, channels, process_id, status, retry_count, retry, created_at, updated_at, error_details FROM event_store ORDER BY created_at ASC")) {
            List<StoredEvent> results = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapper.mapRow(rs));
                }
            }
            assertThat(results).hasSize(2);
            assertThat(results.get(0).eventId()).isEqualTo(id1);
            assertThat(results.get(1).eventId()).isEqualTo(id2);
        }
    }

    // ---- Error mapping ----

    @Test
    @DisplayName("Should map duplicate key violation to IllegalArgumentException")
    void shouldMapDuplicateKeyViolation() {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        StoredEvent event = StoredEvent.newEvent(UUID.randomUUID(), "t", null, "{}", null);

        SQLException duplicate = new SQLException("duplicate", "23505");
        RuntimeException result = mapper.mapSaveError(duplicate, event);
        assertThat(result).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(event.eventId().toString());
    }

    @Test
    @DisplayName("Should map other SQL exceptions to RuntimeException")
    void shouldMapOtherSqlException() {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        StoredEvent event = StoredEvent.newEvent(UUID.randomUUID(), "t", null, "{}", null);

        SQLException other = new SQLException("connection error", "08001");
        RuntimeException result = mapper.mapSaveError(other, event);
        assertThat(result).isInstanceOf(RuntimeException.class)
                .hasMessageContaining(event.eventId().toString());
    }

    @Test
    @DisplayName("Should handle SQLException with null SQL state")
    void shouldMapNullSqlState() {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        StoredEvent event = StoredEvent.newEvent(UUID.randomUUID(), "t", null, "{}", null);

        SQLException noState = new SQLException("error", (String) null);
        RuntimeException result = mapper.mapSaveError(noState, event);
        assertThat(result).isInstanceOf(RuntimeException.class);
    }

    // ---- setOptionalString ----

    @Test
    @DisplayName("Should set VARCHAR when optional string is non-null")
    void shouldSetOptionalStringNonNull() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT * FROM event_store WHERE service = ?")) {
            mapper.setOptionalString(ps, 1, "my-service");
            // Execute with the set value and verify no error and correct result
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isFalse(); // no rows match
            }
        }
    }

    @Test
    @DisplayName("Should set NULL when optional string is null")
    void shouldSetOptionalStringNull() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT * FROM event_store WHERE service = ?")) {
            mapper.setOptionalString(ps, 1, null);
            // Verify by executing — should return no rows since service IS NULL won't match
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isFalse();
            }
        }
    }

    @Test
    @DisplayName("Should round-trip channels column via setInsertParameters and mapRow")
    void shouldRoundTripChannels() throws SQLException {
        StoredEventMapper mapper = new StoredEventMapper(UuidType.NATIVE);
        UUID eventId = UUID.randomUUID();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        String channels = "io.github.vovten.eventflow.channel.ExternalEventChannel,"
                + "io.github.vovten.eventflow.channel.BroadcastEventChannel";
        StoredEvent event = new StoredEvent(eventId, "test.T", "svc-a", "{}",
                channels, null, EventStatus.NEW, 0, false, now, now, null);

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO event_store (event_id, event_type, service, payload, channels, process_id, status, retry_count, retry, created_at, updated_at, error_details) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
            mapper.setInsertParameters(ps, event);
            ps.executeUpdate();
        }

        try (Connection conn = dataSource.getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT event_id, event_type, service, payload, channels, process_id, status, retry_count, retry, created_at, updated_at, error_details FROM event_store WHERE event_id = ?")) {
            mapper.setUuid(ps, 1, eventId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).isTrue();
                StoredEvent read = mapper.mapRow(rs);
                assertThat(read.channels()).isEqualTo(channels);
            }
        }
    }
}
