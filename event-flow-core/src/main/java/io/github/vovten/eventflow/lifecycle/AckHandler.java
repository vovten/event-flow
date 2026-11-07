package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.retry.EventRetryStore;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.retry.RetryKind;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static io.github.vovten.eventflow.lifecycle.store.EventStatus.ACCEPTED;
import static io.github.vovten.eventflow.lifecycle.store.EventStatus.DEAD;
import static io.github.vovten.eventflow.lifecycle.store.EventStatus.FAILED;
import static io.github.vovten.eventflow.lifecycle.store.EventStatus.HANDLED;
import static io.github.vovten.eventflow.lifecycle.store.EventStatus.NEW;
import static io.github.vovten.eventflow.lifecycle.store.EventStatus.PUBLISHED;

/**
 * Processes acknowledgment events ({@link AcceptedAck}, {@link NoHandlerAck},
 * {@link SuccessAck} and {@link FailureAck}) and updates event lifecycle state.
 * <p>
 * A retry-aware handler coordinates {@code event_store} and {@code event_retry}
 * through the {@link LifecycleRepository}. It resolves the retry row for the
 * ack's retry context and ignores an ack whose attempt is older than the row's
 * current attempt. Acks that prove delivery clear the {@code DELIVERY} row,
 * even when the event status is already terminal.
 * <p>
 * A legacy constructor without a retry store preserves the original
 * status-only behaviour.
 * <p>
 * This handler filters acks by comparing the {@code originalService} field of
 * the ack event with the configured {@code service} name. If the service names
 * don't match, the ack is silently ignored — this allows multiple publisher
 * instances to share the same ack channel without interfering with each
 * other's lifecycle state.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class AckHandler implements EventSubscriber {

    private static final Logger log = LoggerFactory.getLogger(AckHandler.class);

    /**
     * A delivery confirmation can only move an event out of the pre-consumer
     * states. From {@code FAILED} the status is deliberately unchanged: the
     * failure may already describe a processing outcome that must stay visible.
     */
    private static final Set<EventStatus> ACCEPTANCE_SOURCES = Set.of(NEW, PUBLISHED);

    /**
     * Processing outcomes and terminal failures may arrive from any
     * non-terminal state, including a late {@code FAILED} after a publish error.
     */
    private static final Set<EventStatus> PROCESSING_SOURCES =
            Set.of(NEW, PUBLISHED, ACCEPTED, FAILED);

    private final String serviceName;
    private final EventStore eventStore;
    private final LifecycleRepository lifecycleRepository;
    private final EventRetryStore retryStore;
    private final AckHandlerOptions options;

    /**
     * Creates a status-only handler without durable retry routing.
     *
     * @param eventStore  the event store to update
     * @param serviceName the local service name for ack filtering, or empty to accept all acks
     */
    public AckHandler(EventStore eventStore, String serviceName) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.lifecycleRepository = null;
        this.retryStore = null;
        this.options = null;
        this.serviceName = serviceName;
    }

    /**
     * Creates a retry-aware handler.
     *
     * @param lifecycleRepository transactional lifecycle facade
     * @param retryStore          durable retry store used for stale-ack guards
     * @param serviceName         the local service name for ack filtering (must not be blank)
     * @param options             retry-aware options
     */
    public AckHandler(LifecycleRepository lifecycleRepository,
                      EventRetryStore retryStore,
                      String serviceName,
                      AckHandlerOptions options) {
        this.lifecycleRepository = Objects.requireNonNull(lifecycleRepository,
                "lifecycleRepository must not be null");
        this.retryStore = Objects.requireNonNull(retryStore, "retryStore must not be null");
        this.options = Objects.requireNonNull(options, "options must not be null");
        if (serviceName == null || serviceName.isBlank()) {
            throw new IllegalArgumentException("serviceName must not be null or blank");
        }
        this.serviceName = serviceName;
        this.eventStore = null;
    }

    @Override
    public List<Class<?>> events() {
        return List.of(AcceptedAck.class, NoHandlerAck.class, SuccessAck.class, FailureAck.class);
    }

    @Override
    public void onEvent(Event event) {
        switch (event) {
            case AcceptedAck acceptedAck -> handle(acceptedAck);
            case NoHandlerAck noHandlerAck -> handle(noHandlerAck);
            case SuccessAck successAck -> handle(successAck);
            case FailureAck failureAck -> handle(failureAck);
            default -> log.trace("Ignoring non-ack event: {}", event);
        }
    }

    private void handle(AcceptedAck ack) {
        if (!isRetryAware()) {
            updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                    ACCEPTANCE_SOURCES, ACCEPTED, null);
            return;
        }
        if (shouldSkipForForeignService(ack.originalService())) {
            return;
        }
        if (isStale(ack.originalEventId(), RetryKind.DELIVERY, ack.attempt(), ack.eventType())) {
            return;
        }
        lifecycleRepository.onAccepted(ack.originalEventId(), ack.attempt());
        log.debug("Event {} accepted on delivery attempt {} ({})",
                ack.originalEventId(), ack.attempt(), ack.eventType());
    }

    private void handle(NoHandlerAck ack) {
        if (!isRetryAware()) {
            updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                    PROCESSING_SOURCES, DEAD, "No handler resolved for event type: " + ack.eventType());
            return;
        }
        if (shouldSkipForForeignService(ack.originalService())) {
            return;
        }
        if (isStale(ack.originalEventId(), RetryKind.DELIVERY, ack.attempt(), ack.eventType())) {
            return;
        }
        lifecycleRepository.onNoHandler(ack.originalEventId(), serviceName, ack.attempt(),
                null, "No handler resolved for event type: " + ack.eventType());
        log.warn("Event {} dead-lettered: no handler resolved ({})",
                ack.originalEventId(), ack.eventType());
    }

    private void handle(SuccessAck ack) {
        if (!isRetryAware()) {
            updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                    PROCESSING_SOURCES, HANDLED, null);
            return;
        }
        if (shouldSkipForForeignService(ack.originalService())) {
            return;
        }
        if (isStale(ack.originalEventId(), ack.retryKind(), ack.attempt(), ack.eventType())) {
            return;
        }
        lifecycleRepository.onHandled(ack.originalEventId(), ack.attempt());
        log.debug("Event {} handled on {} attempt {} ({})",
                ack.originalEventId(), ack.retryKind(), ack.attempt(), ack.eventType());
    }

    private void handle(FailureAck ack) {
        if (!isRetryAware()) {
            EventStatus target = ack.retryable() ? FAILED : DEAD;
            updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                    PROCESSING_SOURCES, target, ack.error());
            return;
        }
        if (shouldSkipForForeignService(ack.originalService())) {
            return;
        }
        if (isStale(ack.originalEventId(), ack.retryKind(), ack.attempt(), ack.eventType())) {
            return;
        }
        int processingAttempt = processingAttempt(ack);
        if (!options.processingEnabled()) {
            lifecycleRepository.onProcessingDisabled(
                    ack.originalEventId(), ack.errorType(), ack.error());
            if (ack.retryable()) {
                log.warn("Processing retries are disabled; retry request for event {} ignored "
                                + "({} attempt {}, policy={})",
                        ack.originalEventId(), ack.retryKind(), ack.attempt(), ack.policyName());
            } else {
                log.debug("Processing retries are disabled; event {} recorded as FAILED",
                        ack.originalEventId());
            }
            return;
        }
        boolean retryable = ack.retryable()
                && processingAttempt <= options.maxProcessingRetries();
        Instant nextAttemptAt = retryable ? nextAttemptAt(ack) : Instant.now();
        lifecycleRepository.onProcessingFailed(ack.originalEventId(), serviceName, processingAttempt,
                retryable, nextAttemptAt, ack.policyName(), ack.errorType(), ack.error());
        if (!retryable) {
            log.warn("Event {} dead-lettered after {} processing attempt(s) ({})",
                    ack.originalEventId(), processingAttempt, ack.eventType());
        }
    }

    private int processingAttempt(FailureAck ack) {
        return ack.retryKind() == RetryKind.DELIVERY ? 1 : ack.attempt();
    }

    private Instant nextAttemptAt(FailureAck ack) {
        long retryAfterMs = ack.retryAfterMs();
        if (retryAfterMs <= 0) {
            return Instant.now().plus(options.defaultProcessingDelay());
        }
        long capped = Math.min(retryAfterMs, options.maxProcessingDelay().toMillis());
        return Instant.now().plusMillis(capped);
    }

    private boolean isStale(UUID eventId, RetryKind retryKind, int attempt, String eventType) {
        return retryStore.find(eventId, retryKind)
                .filter(row -> attempt < row.attempt())
                .map(row -> {
                    log.debug("Ignoring stale {} ack for event {}: attempt {} < {} ({})",
                            retryKind, eventId, attempt, row.attempt(), eventType);
                    return true;
                })
                .orElse(false);
    }

    private boolean isRetryAware() {
        return lifecycleRepository != null;
    }

    private void updateEventStatus(UUID originalEventId, String eventType, String originalService,
                                   Set<EventStatus> allowedSources, EventStatus target, String error) {
        if (shouldSkipForForeignService(originalService)) {
            return;
        }
        try {
            boolean applied = eventStore.transitionStatus(originalEventId, allowedSources, target, error);
            if (applied) {
                log.debug("Event {} status updated to {} ({})", originalEventId, target, eventType);
            } else {
                log.debug("Rejected ack transition for event {} to {}: current status is not in {} ({})",
                        originalEventId, target, allowedSources, eventType);
            }
        } catch (Exception e) {
            log.error("Failed to update event status to {} for {}", target, originalEventId, e);
        }
    }

    private boolean shouldSkipForForeignService(String ackService) {
        if (StringUtils.isEmpty(serviceName)) {
            return false;
        }
        if (!serviceName.equals(ackService)) {
            log.trace("Skipping ack for foreign service (local: {})", serviceName);
            return true;
        }
        return false;
    }

    @Override
    public String name() {
        return "AckHandler";
    }
}
