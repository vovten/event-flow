package io.github.vovten.eventflow.lifecycle;

import io.github.vovten.eventflow.EventSubscriber;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.lifecycle.store.EventStatus;
import io.github.vovten.eventflow.lifecycle.store.EventStore;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

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

    private final EventStore eventStore;
    private final String service;

    /**
     * Creates a new AckHandler.
     *
     * @param eventStore the event store to update
     * @param service    the local service name for ack filtering, or empty to accept all acks
     */
    public AckHandler(EventStore eventStore, String service) {
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
        this.service = service;
    }

    @Override
    public List<Class<?>> events() {
        return List.of(SuccessAck.class, FailureAck.class);
    }

    @Override
    public void onEvent(Event event) {
        if (!(event instanceof LifecycleAckEvent ack)) {
            return;
        }
        if (StringUtils.isNotEmpty(service) && !matchesService(ack)) {
            log.trace("Skipping ack for foreign service (local: {})", service);
            return;
        }
        switch (ack) {
            case SuccessAck successAck -> handle(successAck);
            case FailureAck failureAck -> handle(failureAck);
            default -> log.warn("Unknown lifecycle ack event type: {}", ack.getClass().getName());
        }
    }

    private void handle(SuccessAck successAck) {
        UUID originalEventId = successAck.originalEventId();
        try {
            eventStore.updateStatus(originalEventId, EventStatus.HANDLED, null);
            log.debug("Event handled successfully: {} ({})", originalEventId, successAck.eventType());
        } catch (Exception e) {
            log.error("Failed to update event status to HANDLED for {}", originalEventId, e);
        }
    }

    private void handle(FailureAck failureAck) {
        UUID originalEventId = failureAck.originalEventId();
        try {
            eventStore.updateStatus(originalEventId, EventStatus.FAILED, failureAck.error());
            log.debug("Event handling failed: {} ({})", originalEventId, failureAck.eventType());
        } catch (Exception e) {
            log.error("Failed to update event status to FAILED for {}", originalEventId, e);
        }
    }

    private boolean matchesService(LifecycleAckEvent ack) {
        String ackService = switch (ack) {
            case SuccessAck successAck -> successAck.originalService();
            case FailureAck failureAck -> failureAck.originalService();
            default -> null;
        };
        return service.equals(ackService);
    }

    @Override
    public String name() {
        return "AckHandler";
    }
}
