package io.github.vovten.eventflow.lifecycle.retry;

import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryEventRetryStore Tests")
class InMemoryEventRetryStoreTest {

    private static final String SERVICE = "test-service";

    private InMemoryEventRetryStore store;
    private UUID eventId;

    @BeforeEach
    void setUp() {
        store = new InMemoryEventRetryStore();
        eventId = UUID.randomUUID();
    }

    @Test
    @DisplayName("Should schedule and find a task")
    void shouldScheduleAndFindTask() {
        RetryTask task = task(RetryKind.PROCESSING, 1, Instant.now(), SERVICE);

        store.schedule(task);

        assertThat(store.find(eventId, RetryKind.PROCESSING)).contains(task);
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should find only due tasks for kind and service")
    void shouldFindOnlyDueTasks() {
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
        store.schedule(task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1), SERVICE));

        boolean claimed = store.claim(eventId, RetryKind.PROCESSING, "scheduler-1", Instant.now().plusSeconds(30));

        assertThat(claimed).isTrue();
        assertThat(store.find(eventId, RetryKind.PROCESSING).orElseThrow().state())
                .isEqualTo(RetryState.IN_PROGRESS);
    }

    @Test
    @DisplayName("Should not claim an already claimed task")
    void shouldNotClaimAlreadyClaimedTask() {
        store.schedule(task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1), SERVICE));
        store.claim(eventId, RetryKind.PROCESSING, "scheduler-1", Instant.now().plusSeconds(30));

        boolean claimed = store.claim(eventId, RetryKind.PROCESSING, "scheduler-2", Instant.now().plusSeconds(30));

        assertThat(claimed).isFalse();
    }

    @Test
    @DisplayName("Should reclaim a task after lease expiration")
    void shouldReclaimAfterLeaseExpiration() {
        store.schedule(task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1), SERVICE));
        store.claim(eventId, RetryKind.PROCESSING, "scheduler-1", Instant.now().minusSeconds(1));

        boolean claimed = store.claim(eventId, RetryKind.PROCESSING, "scheduler-2", Instant.now().plusSeconds(30));

        assertThat(claimed).isTrue();
        assertThat(store.find(eventId, RetryKind.PROCESSING).orElseThrow().claimedBy())
                .isEqualTo("scheduler-2");
    }

    @Test
    @DisplayName("Should reschedule and complete a task")
    void shouldRescheduleAndCompleteTask() {
        store.schedule(task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1), SERVICE));

        store.reschedule(eventId, RetryKind.PROCESSING, 2, Instant.now().plusSeconds(10), "Type", "error");
        RetryTask rescheduled = store.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(rescheduled.state()).isEqualTo(RetryState.PENDING);
        assertThat(rescheduled.attempt()).isEqualTo(2);
        assertThat(rescheduled.lastError()).isEqualTo("error");

        store.complete(eventId, RetryKind.PROCESSING);
        assertThat(store.find(eventId, RetryKind.PROCESSING)).isEmpty();
    }

    @Test
    @DisplayName("Should mark a task as dead")
    void shouldMarkTaskAsDead() {
        store.schedule(task(RetryKind.PROCESSING, 1, Instant.now().minusSeconds(1), SERVICE));

        store.dead(eventId, RetryKind.PROCESSING, RetryReason.CAP_EXCEEDED, "Type", "error");

        RetryTask dead = store.find(eventId, RetryKind.PROCESSING).orElseThrow();
        assertThat(dead.state()).isEqualTo(RetryState.DEAD);
        assertThat(dead.reason()).isEqualTo(RetryReason.CAP_EXCEEDED);
        assertThat(dead.lastError()).isEqualTo("error");
    }

    @Test
    @DisplayName("Should delete rows by event ids")
    void shouldDeleteByEventIds() {
        UUID otherId = UUID.randomUUID();
        store.schedule(task(RetryKind.PROCESSING, 1, Instant.now(), SERVICE));
        store.schedule(RetryTask.pending(
                otherId, SERVICE, RetryKind.PROCESSING, 1,
                RetryReason.HANDLER_FAILED, Instant.now(), null));

        int deleted = store.deleteByEventIds(List.of(eventId));

        assertThat(deleted).isEqualTo(1);
        assertThat(store.find(eventId, RetryKind.PROCESSING)).isEmpty();
        assertThat(store.find(otherId, RetryKind.PROCESSING)).isPresent();
    }

    @Test
    @DisplayName("Should reject rescheduling a missing task")
    void shouldRejectReschedulingMissingTask() {
        assertThatThrownBy(() -> store.reschedule(
                eventId, RetryKind.PROCESSING, 2, Instant.now(), null, null))
                .isInstanceOf(NoSuchElementException.class);
    }

    private RetryTask task(RetryKind kind, int attempt, Instant nextAttemptAt, String service) {
        return task(eventId, kind, attempt, nextAttemptAt, service);
    }

    private RetryTask task(UUID id, RetryKind kind, int attempt,
                           Instant nextAttemptAt, String service) {
        return RetryTask.pending(id, service, kind, attempt,
                RetryReason.HANDLER_FAILED, nextAttemptAt, null);
    }
}
