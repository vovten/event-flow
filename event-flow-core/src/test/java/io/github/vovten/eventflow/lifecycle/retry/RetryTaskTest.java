package io.github.vovten.eventflow.lifecycle.retry;

import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryTask Tests")
class RetryTaskTest {

    private static final UUID EVENT_ID = UUID.randomUUID();
    private static final Instant NOW = Instant.now();

    @Test
    @DisplayName("Should create a pending retry task")
    void shouldCreatePendingTask() {
        RetryTask task = RetryTask.pending(
                EVENT_ID, "test-service", RetryKind.PROCESSING, 3,
                RetryReason.HANDLER_FAILED, NOW, "payment");

        assertThat(task.eventId()).isEqualTo(EVENT_ID);
        assertThat(task.service()).isEqualTo("test-service");
        assertThat(task.retryKind()).isEqualTo(RetryKind.PROCESSING);
        assertThat(task.attempt()).isEqualTo(3);
        assertThat(task.state()).isEqualTo(RetryState.PENDING);
        assertThat(task.reason()).isEqualTo(RetryReason.HANDLER_FAILED);
        assertThat(task.nextAttemptAt()).isEqualTo(NOW);
        assertThat(task.policyName()).isEqualTo("payment");
        assertThat(task.claimedBy()).isNull();
        assertThat(task.claimedUntil()).isNull();
    }

    @Test
    @DisplayName("Should claim and reschedule a task")
    void shouldClaimAndRescheduleTask() {
        RetryTask pending = RetryTask.pending(
                EVENT_ID, "test-service", RetryKind.PROCESSING, 1,
                RetryReason.HANDLER_FAILED, NOW.minusSeconds(1), null);
        Instant claimedUntil = NOW.plusSeconds(30);

        RetryTask claimed = pending.withClaim("scheduler-1", claimedUntil);
        RetryTask rescheduled = claimed.withReschedule(2, NOW.plusSeconds(5), "Type", "error");

        assertThat(claimed.state()).isEqualTo(RetryState.IN_PROGRESS);
        assertThat(claimed.claimedBy()).isEqualTo("scheduler-1");
        assertThat(claimed.claimedUntil()).isEqualTo(claimedUntil);
        assertThat(rescheduled.state()).isEqualTo(RetryState.PENDING);
        assertThat(rescheduled.attempt()).isEqualTo(2);
        assertThat(rescheduled.nextAttemptAt()).isEqualTo(NOW.plusSeconds(5));
        assertThat(rescheduled.claimedBy()).isNull();
        assertThat(rescheduled.claimedUntil()).isNull();
        assertThat(rescheduled.lastErrorType()).isEqualTo("Type");
        assertThat(rescheduled.lastError()).isEqualTo("error");
    }

    @Test
    @DisplayName("Should reschedule the next attempt with a new reason")
    void shouldScheduleNextAttempt() {
        RetryTask task = RetryTask.pending(
                EVENT_ID, "test-service", RetryKind.DELIVERY, 1,
                RetryReason.PUBLISH_FAILED, NOW.minusSeconds(1), "delivery");

        RetryTask next = task.withNextAttempt(2, RetryReason.ACK_TIMEOUT,
                NOW.plusSeconds(5), "Type", "timeout");

        assertThat(next.state()).isEqualTo(RetryState.PENDING);
        assertThat(next.attempt()).isEqualTo(2);
        assertThat(next.reason()).isEqualTo(RetryReason.ACK_TIMEOUT);
        assertThat(next.nextAttemptAt()).isEqualTo(NOW.plusSeconds(5));
        assertThat(next.lastErrorType()).isEqualTo("Type");
        assertThat(next.lastError()).isEqualTo("timeout");
        assertThat(next.policyName()).isEqualTo("delivery");
        assertThat(next.createdAt()).isEqualTo(task.createdAt());
        assertThat(next.claimedBy()).isNull();
        assertThat(next.claimedUntil()).isNull();
    }

    @Test
    @DisplayName("Should mark a task as dead")
    void shouldMarkTaskAsDead() {
        RetryTask task = RetryTask.pending(
                EVENT_ID, "test-service", RetryKind.PROCESSING, 1,
                RetryReason.HANDLER_FAILED, NOW, null);

        RetryTask dead = task.withDead(RetryReason.CAP_EXCEEDED, "Type", "error");

        assertThat(dead.state()).isEqualTo(RetryState.DEAD);
        assertThat(dead.reason()).isEqualTo(RetryReason.CAP_EXCEEDED);
        assertThat(dead.lastErrorType()).isEqualTo("Type");
        assertThat(dead.lastError()).isEqualTo("error");
        assertThat(dead.claimedBy()).isNull();
    }

    @Test
    @DisplayName("Should report due and expired-claim states")
    void shouldReportDueAndExpiredClaims() {
        RetryTask due = RetryTask.pending(
                EVENT_ID, "test-service", RetryKind.PROCESSING, 1,
                RetryReason.HANDLER_FAILED, NOW.minusSeconds(1), null);
        RetryTask expired = due.withClaim("owner", NOW.minusSeconds(1));

        assertThat(due.isDue(NOW)).isTrue();
        assertThat(expired.isClaimExpired(NOW)).isTrue();
        assertThat(expired.isDue(NOW)).isFalse();
    }

    @Test
    @DisplayName("Should reject invalid task parameters")
    void shouldRejectInvalidParameters() {
        assertThatThrownBy(() -> RetryTask.pending(
                EVENT_ID, " ", RetryKind.PROCESSING, 1,
                RetryReason.HANDLER_FAILED, NOW, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("service");

        assertThatThrownBy(() -> RetryTask.pending(
                EVENT_ID, "test-service", RetryKind.PROCESSING, 0,
                RetryReason.HANDLER_FAILED, NOW, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("attempt");

        assertThatThrownBy(() -> new RetryTask(
                EVENT_ID, "test-service", RetryKind.PROCESSING, 1,
                RetryState.IN_PROGRESS, RetryReason.HANDLER_FAILED, NOW,
                null, null, null, null, null, NOW, NOW))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("claimedBy");
    }
}
