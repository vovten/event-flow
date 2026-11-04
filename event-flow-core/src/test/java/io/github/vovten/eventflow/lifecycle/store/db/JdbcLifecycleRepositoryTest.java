package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.retry.RetryKind;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JdbcLifecycleRepository Tests")
class JdbcLifecycleRepositoryTest {

    private static final String SERVICE = "test-service";

    private DataSource dataSource;
    private JdbcEventStore eventStore;
    private JdbcEventRetryStore retryStore;
    private JdbcLifecycleRepository repository;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        dataSource = createH2DataSource("jdbc:h2:mem:lifecycle-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        eventStore = new JdbcEventStore(dataSource);
        retryStore = new JdbcEventRetryStore(dataSource);
        repository = new JdbcLifecycleRepository(dataSource, eventStore, retryStore);
        eventId = UUID.randomUUID();
        eventStore.save(StoredEvent.newEvent(
                eventId, "test.TestEvent", SERVICE, "{}", null));
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    @DisplayName("Should record publish failure and delivery retry atomically")
    void shouldRecordPublishFailure() {
        repository.onPublishFailed(eventId, SERVICE, RetryReason.PUBLISH_FAILED,
                Instant.now().plusSeconds(5), "Type", "error");

        assertThat(status()).isEqualTo(EventStatus.FAILED);
        RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.reason()).isEqualTo(RetryReason.PUBLISH_FAILED);
    }

    @Test
    @DisplayName("Should record acceptance and delete delivery retry")
    void shouldRecordAcceptance() {
        repository.onPublishFailed(eventId, SERVICE, RetryReason.PUBLISH_FAILED,
                Instant.now().plusSeconds(5), null, null);
        eventStore.transitionStatus(eventId, Set.of(EventStatus.FAILED), EventStatus.PUBLISHED, null);

        repository.onAccepted(eventId, 1);

        assertThat(status()).isEqualTo(EventStatus.ACCEPTED);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
    }

    @Test
    @DisplayName("Should record retryable processing failure")
    void shouldRecordRetryableProcessingFailure() {
        repository.onProcessingFailed(eventId, SERVICE, 2, true,
                Instant.now().plusSeconds(10), "payment", "Type", "error");

        assertThat(status()).isEqualTo(EventStatus.FAILED);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.attempt()).isEqualTo(2);
        assertThat(task.policyName()).isEqualTo("payment");
    }

    @Test
    @DisplayName("Should record permanent processing failure as dead")
    void shouldRecordPermanentProcessingFailure() {
        repository.onProcessingFailed(eventId, SERVICE, 1, false,
                Instant.now(), "payment", "Type", "error");

        assertThat(status()).isEqualTo(EventStatus.DEAD);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.DEAD);
        assertThat(task.policyName()).isEqualTo("payment");
    }

    @Test
    @DisplayName("Should not create retry rows for a healthy lifecycle")
    void shouldNotCreateRetryRowsForHealthyEvent() {
        repository.onAccepted(eventId, 1);
        repository.onHandled(eventId, 1);

        assertThat(status()).isEqualTo(EventStatus.HANDLED);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
    }

    @Test
    @DisplayName("Should record successful handling and clear retries")
    void shouldRecordHandled() {
        repository.onProcessingFailed(eventId, SERVICE, 1, true,
                Instant.now().plusSeconds(10), null, null, "error");

        repository.onHandled(eventId, 2);

        assertThat(status()).isEqualTo(EventStatus.HANDLED);
        assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
    }

    @Test
    @DisplayName("Should record a disabled-processing failure without creating a processing row")
    void shouldRecordProcessingDisabled() {
        retryStore.schedule(RetryTask.pending(eventId, SERVICE, RetryKind.DELIVERY, 1,
                RetryReason.PUBLISH_FAILED, Instant.now(), null));

        repository.onProcessingDisabled(eventId, "Type", "error");

        assertThat(status()).isEqualTo(EventStatus.FAILED);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
    }

    @Test
    @DisplayName("Should record no-handler as dead")
    void shouldRecordNoHandler() {
        repository.onNoHandler(eventId, SERVICE, 1, "Type", "no handler");

        assertThat(status()).isEqualTo(EventStatus.DEAD);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.DEAD);
        assertThat(task.reason()).isEqualTo(RetryReason.NO_HANDLER);
    }

    @Test
    @DisplayName("Should reset an event for manual retry")
    void shouldMarkForRetry() {
        repository.onProcessingFailed(eventId, SERVICE, 1, false,
                Instant.now(), "payment", "Type", "error");

        repository.markForRetry(eventId, SERVICE);

        assertThat(status()).isEqualTo(EventStatus.NEW);
        RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.attempt()).isEqualTo(1);
        assertThat(task.policyName()).isEqualTo("payment");
    }

    @Test
    @DisplayName("Should dead-letter an explicit retry kind")
    void shouldDeadLetterExplicitKind() {
        repository.onDeadLetter(eventId, SERVICE, RetryKind.DELIVERY,
                RetryReason.ACK_TIMEOUT, "Type", "timeout");

        assertThat(status()).isEqualTo(EventStatus.DEAD);
        RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
        assertThat(task.state()).isEqualTo(RetryState.DEAD);
        assertThat(task.reason()).isEqualTo(RetryReason.ACK_TIMEOUT);
    }

    @Test
    @DisplayName("Should roll back both writes when commit fails")
    void shouldRollbackOnCommitFailure() {
        DataSource failingDataSource = failingCommitDataSource(dataSource);
        JdbcLifecycleRepository failingRepository = new JdbcLifecycleRepository(
                failingDataSource, eventStore, retryStore);

        assertThatThrownBy(() -> failingRepository.onPublishFailed(
                eventId, SERVICE, RetryReason.PUBLISH_FAILED,
                Instant.now().plusSeconds(5), "Type", "error"))
                .isInstanceOf(RuntimeException.class);

        assertThat(status()).isEqualTo(EventStatus.NEW);
        assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
    }

    private DataSource failingCommitDataSource(DataSource real) {
        return (DataSource) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName())) {
                        return failingConnection(real.getConnection());
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private Connection failingConnection(Connection real) {
        return (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> {
                    if ("commit".equals(method.getName())) {
                        throw new SQLException("commit failed");
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private EventStatus status() {
        return eventStore.findById(eventId).orElseThrow().status();
    }

    private static DataSource createH2DataSource(String url) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }
}
