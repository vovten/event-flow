package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.lifecycle.retry.InMemoryEventRetryStore;
import io.github.vovten.eventflow.lifecycle.retry.RetryReason;
import io.github.vovten.eventflow.lifecycle.retry.RetryState;
import io.github.vovten.eventflow.lifecycle.retry.RetryTask;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.InMemoryEventStore;
import io.github.vovten.eventflow.lifecycle.store.StoredEvent;
import io.github.vovten.eventflow.retry.RetryKind;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("RetryWatchdog Tests")
class RetryWatchdogTest {

    private static final String SERVICE = "test-service";
    private static final Duration ACK_TIMEOUT = Duration.ofMinutes(1);

    private InMemoryEventStore eventStore;
    private InMemoryEventRetryStore retryStore;

    @BeforeEach
    void setUp() {
        eventStore = new InMemoryEventStore();
        retryStore = new InMemoryEventRetryStore();
    }

    private RetryWatchdog watchdog(boolean processingEnabled) {
        return new RetryWatchdog(eventStore, retryStore, options(processingEnabled));
    }

    private RetryWatchdogOptions options(boolean processingEnabled) {
        return new RetryWatchdogOptions(
                SERVICE, Duration.ofMinutes(1), ACK_TIMEOUT, ACK_TIMEOUT, 100, processingEnabled);
    }

    private UUID storeEvent(EventStatus status, Duration age) {
        return storeEvent(status, age, SERVICE);
    }

    private UUID storeEvent(EventStatus status, Duration age, String service) {
        UUID eventId = UUID.randomUUID();
        Instant timestamp = Instant.now().minus(age);
        eventStore.save(new StoredEvent(
                eventId, "test.TestEvent", service, "{}", null, null,
                status, 0, false, timestamp, timestamp, null));
        return eventId;
    }

    @Nested
    @DisplayName("watchdogCycle")
    class WatchdogCycle {

        @Test
        @DisplayName("should create a delivery retry for a stale PUBLISHED event")
        void createsDeliveryRetryForStalePublishedEvent() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED, Duration.ofHours(1));

            watchdog(true).watchdogCycle();

            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
            assertThat(task.reason()).isEqualTo(RetryReason.ACK_TIMEOUT);
            assertThat(task.attempt()).isEqualTo(1);
        }

        @Test
        @DisplayName("should not create a delivery retry for a recent PUBLISHED event")
        void ignoresRecentPublishedEvent() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED, Duration.ofSeconds(5));

            watchdog(true).watchdogCycle();

            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }

        @Test
        @DisplayName("should create a processing retry for a stale ACCEPTED event")
        void createsProcessingRetryForStaleAcceptedEvent() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED, Duration.ofHours(1));

            watchdog(true).watchdogCycle();

            RetryTask task = retryStore.find(eventId, RetryKind.PROCESSING).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
            assertThat(task.reason()).isEqualTo(RetryReason.PROCESSING_TIMEOUT);
        }

        @Test
        @DisplayName("should not create a processing retry when processing is disabled")
        void ignoresAcceptedEventWhenProcessingDisabled() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED, Duration.ofHours(1));

            watchdog(false).watchdogCycle();

            assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
        }

        @Test
        @DisplayName("should not create a processing retry for a recent ACCEPTED event")
        void ignoresRecentAcceptedEvent() {
            UUID eventId = storeEvent(EventStatus.ACCEPTED, Duration.ofSeconds(5));

            watchdog(true).watchdogCycle();

            assertThat(retryStore.find(eventId, RetryKind.PROCESSING)).isEmpty();
        }

        @Test
        @DisplayName("should raise an existing pending retry so it becomes due")
        void raisesExistingPendingRetry() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED, Duration.ofHours(1));
            retryStore.schedule(RetryTask.pending(
                    eventId, SERVICE, RetryKind.DELIVERY, 2,
                    RetryReason.PUBLISH_FAILED, Instant.now().plusSeconds(3600), null));

            watchdog(true).watchdogCycle();

            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.PENDING);
            assertThat(task.attempt()).isEqualTo(2);
            assertThat(task.nextAttemptAt()).isBeforeOrEqualTo(Instant.now());
        }

        @Test
        @DisplayName("should not touch an in-progress retry")
        void leavesInProgressRetryUntouched() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED, Duration.ofHours(1));
            retryStore.schedule(RetryTask.pending(
                            eventId, SERVICE, RetryKind.DELIVERY, 2,
                            RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null)
                    .withClaim("instance-1", Instant.now().plusSeconds(30)));

            watchdog(true).watchdogCycle();

            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.IN_PROGRESS);
            assertThat(task.claimedBy()).isEqualTo("instance-1");
        }

        @Test
        @DisplayName("should not resurrect a dead retry")
        void leavesDeadRetryUntouched() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED, Duration.ofHours(1));
            retryStore.schedule(RetryTask.pending(
                            eventId, SERVICE, RetryKind.DELIVERY, 2,
                            RetryReason.PUBLISH_FAILED, Instant.now().minusSeconds(1), null)
                    .withDead(RetryReason.CAP_EXCEEDED, null, "exhausted"));

            watchdog(true).watchdogCycle();

            RetryTask task = retryStore.find(eventId, RetryKind.DELIVERY).orElseThrow();
            assertThat(task.state()).isEqualTo(RetryState.DEAD);
            assertThat(task.reason()).isEqualTo(RetryReason.CAP_EXCEEDED);
        }

        @Test
        @DisplayName("should ignore stale events owned by another service")
        void ignoresOtherServiceEvents() {
            UUID eventId = storeEvent(EventStatus.PUBLISHED, Duration.ofHours(1), "other-service");

            watchdog(true).watchdogCycle();

            assertThat(retryStore.find(eventId, RetryKind.DELIVERY)).isEmpty();
        }
    }

    @Nested
    @DisplayName("options")
    class Options {

        @Test
        @DisplayName("should reject a blank service")
        void rejectsBlankService() {
            assertThatThrownBy(() -> new RetryWatchdogOptions(
                    "  ", Duration.ofMinutes(1), ACK_TIMEOUT, ACK_TIMEOUT, 100, true))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("service must not be null or blank");
        }

        @Test
        @DisplayName("should reject a non-positive interval")
        void rejectsNonPositiveInterval() {
            assertThatThrownBy(() -> new RetryWatchdogOptions(
                    SERVICE, Duration.ZERO, ACK_TIMEOUT, ACK_TIMEOUT, 100, true))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("interval must be positive");
        }

        @Test
        @DisplayName("should reject a non-positive batch size")
        void rejectsNonPositiveBatchSize() {
            assertThatThrownBy(() -> new RetryWatchdogOptions(
                    SERVICE, Duration.ofMinutes(1), ACK_TIMEOUT, ACK_TIMEOUT, 0, true))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("batchSize must be positive");
        }
    }

    @Nested
    @DisplayName("lifecycle")
    class Lifecycle {

        @Test
        @DisplayName("should start and stop without errors")
        void startsAndStops() {
            RetryWatchdog watchdog = watchdog(true);
            watchdog.start();
            watchdog.stop();
        }

        @Test
        @DisplayName("should stop on close")
        void closes() {
            RetryWatchdog watchdog = watchdog(true);
            watchdog.start();
            watchdog.close();
        }
    }
}
