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
 * {@link SuccessAck} and {@link FailureAck}) and updates the corresponding
 * event status in the {@link EventStore}.
 * <p>
 * Transitions are conditional: a late or out-of-order ack cannot roll back a
 * terminal status or overwrite a processing outcome. For example, a
 * {@code SuccessAck} that arrives before {@code AcceptedAck} moves the event to
 * {@code HANDLED}, and the late {@code AcceptedAck} is ignored.
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

    /**
     * A delivery confirmation can only move an event out of the pre-consumer
     * states. From {@code FAILED} it is deliberately ignored: the failure may
     * already describe a processing outcome that must stay visible.
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

    /**
     * Creates a new AckHandler.
     *
     * @param eventStore  the event store to update
     * @param serviceName the local service name for ack filtering, or empty to accept all acks
     */
    public AckHandler(EventStore eventStore, String serviceName) {
        this.serviceName = serviceName;
        this.eventStore = Objects.requireNonNull(eventStore, "eventStore must not be null");
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
        updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                ACCEPTANCE_SOURCES, ACCEPTED, null);
    }

    private void handle(NoHandlerAck ack) {
        updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                PROCESSING_SOURCES, DEAD, "No handler resolved for event type: " + ack.eventType());
    }

    private void handle(SuccessAck ack) {
        updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                PROCESSING_SOURCES, HANDLED, null);
    }

    private void handle(FailureAck ack) {
        EventStatus target = ack.retryable() ? FAILED : DEAD;
        updateEventStatus(ack.originalEventId(), ack.eventType(), ack.originalService(),
                PROCESSING_SOURCES, target, ack.error());
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
