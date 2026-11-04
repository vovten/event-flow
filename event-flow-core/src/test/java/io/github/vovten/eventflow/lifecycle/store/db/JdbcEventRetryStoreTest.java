package io.github.vovten.eventflow.lifecycle.store.db;

import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.retry.RetryKind;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

@DisplayName("JdbcEventRetryStore Tests")
class JdbcEventRetryStoreTest {

    private static final String SERVICE = "test-service";

    private DataSource dataSource;

    @BeforeEach
    void setUp() {
        dataSource = createH2DataSource("jdbc:h2:mem:retryflow-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    }

    @AfterEach
    void tearDown() throws SQLException {
        try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.execute("DROP ALL OBJECTS");
        }
    }

    @Test
    @DisplayName("Should create the retry table")
    void shouldCreateRetryTable() throws SQLException {
        new JdbcEventRetryStore(dataSource);

        assertThat(tableExists("EVENT_RETRY")).isTrue();
    }

    @Test
    @DisplayName("Should fail when auto-init is disabled and the table is missing")
    void shouldFailWhenTableMissing() {
        assertThatThrownBy(() -> new JdbcEventRetryStore(dataSource, "event_retry", false))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    @DisplayName("Should schedule and find a retry task")
    void shouldScheduleAndFindTask() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));

        store.schedule(task);

        RetryTask found = store.find(task.eventId(), RetryKind.PROCESSING).orElseThrow();
        assertThat(found.service()).isEqualTo(SERVICE);
        assertThat(found.reason()).isEqualTo(RetryReason.HANDLER_FAILED);
        assertThat(found.state()).isEqualTo(RetryState.PENDING);
    }

    @Test
    @DisplayName("Should update an existing retry task on schedule")
    void shouldUpdateExistingTask() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask first = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(first);

        RetryTask updated = first.withReschedule(2, Instant.now().plusSeconds(10), "Type", "error");
        store.schedule(updated);

        RetryTask found = store.find(first.eventId(), RetryKind.PROCESSING).orElseThrow();
        assertThat(found.attempt()).isEqualTo(2);
        assertThat(found.nextAttemptAt()).isCloseTo(updated.nextAttemptAt(), within(1, ChronoUnit.MILLIS));
    }

    @Test
    @DisplayName("Should find only due tasks for kind and service")
    void shouldFindOnlyDueTasks() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        Instant now = Instant.now();
        UUID dueId = UUID.randomUUID();
        UUID deliveryId = UUID.randomUUID();
        UUID futureId = UUID.randomUUID();
        UUID otherServiceId = UUID.randomUUID();
        store.schedule(task(dueId, RetryKind.PROCESSING, 1, now.minusSeconds(1), SERVICE));
        store.schedule(task(deliveryId, RetryKind.DELIVERY, 1, now.minusSeconds(1), SERVICE));
        store.schedule(task(futureId, RetryKind.PROCESSING, 1, now.plusSeconds(60), SERVICE));
        store.schedule(task(otherServiceId, RetryKind.PROCESSING, 1,
                now.minusSeconds(1), "other-service"));

        List<RetryTask> due = store.findDue(RetryKind.PROCESSING, SERVICE, now, 10);

        assertThat(due).hasSize(1);
        assertThat(due.get(0).eventId()).isEqualTo(dueId);
    }

    @Test
    @DisplayName("Should claim a due task")
    void shouldClaimDueTask() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(task);

        boolean claimed = store.claim(task.eventId(), RetryKind.PROCESSING,
                "scheduler-1", Instant.now().plusSeconds(30));

        assertThat(claimed).isTrue();
        assertThat(store.find(task.eventId(), RetryKind.PROCESSING).orElseThrow().state())
                .isEqualTo(RetryState.IN_PROGRESS);
    }

    @Test
    @DisplayName("Should not claim an already claimed task")
    void shouldNotClaimAlreadyClaimedTask() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(task);
        store.claim(task.eventId(), RetryKind.PROCESSING, "scheduler-1", Instant.now().plusSeconds(30));

        boolean claimed = store.claim(task.eventId(), RetryKind.PROCESSING,
                "scheduler-2", Instant.now().plusSeconds(30));

        assertThat(claimed).isFalse();
    }

    @Test
    @DisplayName("Should reclaim a task after lease expiration")
    void shouldReclaimAfterLeaseExpiration() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(task);
        store.claim(task.eventId(), RetryKind.PROCESSING, "scheduler-1", Instant.now().minusSeconds(1));

        boolean claimed = store.claim(task.eventId(), RetryKind.PROCESSING,
                "scheduler-2", Instant.now().plusSeconds(30));

        assertThat(claimed).isTrue();
        assertThat(store.find(task.eventId(), RetryKind.PROCESSING).orElseThrow().claimedBy())
                .isEqualTo("scheduler-2");
    }

    @Test
    @DisplayName("Should allow only one concurrent claim")
    void shouldAllowOnlyOneConcurrentClaim() throws Exception {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(task);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        try {
            Future<Boolean> first = executor.submit(() -> {
                ready.countDown();
                start.await();
                return store.claim(task.eventId(), RetryKind.PROCESSING,
                        "scheduler-1", Instant.now().plusSeconds(30));
            });
            Future<Boolean> second = executor.submit(() -> {
                ready.countDown();
                start.await();
                return store.claim(task.eventId(), RetryKind.PROCESSING,
                        "scheduler-2", Instant.now().plusSeconds(30));
            });
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            long claims = (first.get(5, TimeUnit.SECONDS) ? 1 : 0)
                    + (second.get(5, TimeUnit.SECONDS) ? 1 : 0);
            assertThat(claims).isEqualTo(1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    @DisplayName("Should reschedule and complete a task")
    void shouldRescheduleAndCompleteTask() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(task);

        store.reschedule(task.eventId(), RetryKind.PROCESSING, 2,
                Instant.now().plusSeconds(10), "Type", "error");
        RetryTask rescheduled = store.find(task.eventId(), RetryKind.PROCESSING).orElseThrow();
        assertThat(rescheduled.state()).isEqualTo(RetryState.PENDING);
        assertThat(rescheduled.attempt()).isEqualTo(2);
        assertThat(rescheduled.lastError()).isEqualTo("error");

        store.complete(task.eventId(), RetryKind.PROCESSING);
        assertThat(store.find(task.eventId(), RetryKind.PROCESSING)).isEmpty();
    }

    @Test
    @DisplayName("Should mark a task as dead")
    void shouldMarkTaskAsDead() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1));
        store.schedule(task);

        store.dead(task.eventId(), RetryKind.PROCESSING,
                RetryReason.CAP_EXCEEDED, "Type", "error");

        RetryTask dead = store.find(task.eventId(), RetryKind.PROCESSING).orElseThrow();
        assertThat(dead.state()).isEqualTo(RetryState.DEAD);
        assertThat(dead.reason()).isEqualTo(RetryReason.CAP_EXCEEDED);
        assertThat(dead.lastError()).isEqualTo("error");
    }

    @Test
    @DisplayName("Should delete rows by event ids")
    void shouldDeleteByEventIds() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);
        RetryTask first = task(RetryKind.PROCESSING, 1, Instant.now());
        UUID otherId = UUID.randomUUID();
        store.schedule(first);
        store.schedule(task(otherId, RetryKind.PROCESSING, 1, Instant.now(), SERVICE));

        int deleted = store.deleteByEventIds(List.of(first.eventId()));

        assertThat(deleted).isEqualTo(1);
        assertThat(store.find(first.eventId(), RetryKind.PROCESSING)).isEmpty();
        assertThat(store.find(otherId, RetryKind.PROCESSING)).isPresent();
    }

    @Test
    @DisplayName("Should reject rescheduling a missing task")
    void shouldRejectReschedulingMissingTask() {
        JdbcEventRetryStore store = new JdbcEventRetryStore(dataSource);

        assertThatThrownBy(() -> store.reschedule(
                UUID.randomUUID(), RetryKind.PROCESSING, 2, Instant.now(), null, null))
                .isInstanceOf(NoSuchElementException.class);
    }

    private RetryTask task(RetryKind kind, int attempt, Instant nextAttemptAt) {
        return task(UUID.randomUUID(), kind, attempt, nextAttemptAt, SERVICE);
    }

    private RetryTask task(UUID id, RetryKind kind, int attempt, Instant nextAttemptAt, String service) {
        return RetryTask.pending(id, service, kind, attempt,
                RetryReason.HANDLER_FAILED, nextAttemptAt, null);
    }

    private static DataSource createH2DataSource(String url) {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL(url);
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    private boolean tableExists(String tableName) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getTables(null, null, tableName, null)) {
                if (rs.next()) {
                    return true;
                }
            }
            try (ResultSet rs = meta.getTables(null, null, tableName.toUpperCase(), null)) {
                if (rs.next()) {
                    return true;
                }
            }
            try (ResultSet rs = meta.getTables(null, null, tableName.toLowerCase(), null)) {
                return rs.next();
            }
        }
    }
}
