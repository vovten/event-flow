package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import io.github.vovten.eventflow.publisher.FailureTracker;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static io.github.vovten.eventflow.lifecycle.store.EventStatus.FAILED;
import static io.github.vovten.eventflow.lifecycle.store.EventStatus.HANDLED;

/**
 * Processes acknowledgment events ({@link SuccessAck} and {@link FailureAck})
 * and updates the corresponding event status in the {@link EventStore}.
 * <p>
 * This handler filters acks by comparing the {@code originalService} field of the
 * ack event with the configured {@code service} name. If the service names don't
 * match, the ack is silently ignored — this allows multiple publisher instances
 * to share the same ack channel without interfering with each other's lifecycle state.
 * <p>
 * This approach avoids querying the database to check event ownership:
 * the filtering is done purely by service identity.
 *
 * @author Vladimir Aleshkov
 * @since 1.2.0
 */
public final class AckHandler implements EventSubscriber {

    private static final Logger log = LoggerFactory.getLogger(AckHandler.class);

    private final String serviceName;
    private final EventStore eventStore;
    private final FailureTracker failureTracker;

    /**
     * Creates a new AckHandler.
     *
     * @param eventStore     the event store to update
     * @param serviceName    the local service name for ack filtering, or empty to accept all acks
     * @param failureTracker optional failure tracker for circuit breaker integration
     */
    public AckHandler(EventStore eventStore, String serviceName, FailureTracker failureTracker) {
        this.serviceName = serviceName;
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.failureTracker = failureTracker;
    }

    /**
     * Creates a new AckHandler without circuit breaker integration.
     *
     * @param eventStore  the event store to update
     * @param serviceName the local service name for ack filtering, or empty to accept all acks
     */
    public AckHandler(EventStore eventStore, String serviceName) {
        this(eventStore, serviceName, null);
    }

    @Override
    public List<Class<?>> events() {
        return List.of(SuccessAck.class, FailureAck.class);
    }

    @Override
    public void onEvent(Event event) {
        switch (event) {
            case SuccessAck successAck -> handle(successAck);
            case FailureAck failureAck -> handle(failureAck);
            default -> log.trace("Ignoring non-ack event: {}", event);
        }
    }

    private void handle(SuccessAck ack) {
        updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(), HANDLED, null);
    }

    private void handle(FailureAck ack) {
        if (failureTracker != null) {
            failureTracker.recordFailure(ack.eventType());
        }
        updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(), FAILED, ack.error());
    }

    private void updateEventStatus(UUID originalEventId, String eventType, String originalService,
                                    EventStatus status, String error) {
        if (shouldSkipForForeignService(originalService)) {
            return;
        }
        try {
            eventStore.updateStatus(originalEventId, status, error);
            log.debug("Event {} status updated to {}: {} ({})",
                    status == HANDLED ? "handled" : "failed",
                    status, originalEventId, eventType);
        } catch (Exception e) {
            log.error("Failed to update event status to {} for {}", status, originalEventId, e);
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
