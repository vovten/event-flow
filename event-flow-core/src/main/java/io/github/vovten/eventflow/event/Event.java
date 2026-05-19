package io.github.vovten.eventflow.event;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.util.EventUtils;

import java.util.List;

/**
 * Represents an event — a message indicating that something happened in the system.
 * <p>
 * Unlike commands, events describe facts (e.g., {@code OrderPlaced}, {@code PaymentFailed})
 * that other parts of the system can react to. Events are immutable and typically
 * published after state changes have already occurred.
 * <p>
 * Events are routed to channels based on {@link #channels()}.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Event {

    /**
     * @return the event type
     */
    Class<?> type();

    /**
     * List of channel classes this event should be published to.
     * Uses class types for compile-time safety.
     * By default, the event is published to the internal channel.
     *
     * @return list of channel classes
     */
    default List<Class<? extends EventChannel>> channels() {
        return List.of(InternalEventChannel.class);
    }

    /**
     * Returns the event as JSON string.
     * Used for logging purposes.
     *
     * @return the event as JSON
     * @deprecated Use {@code toString()} instead. This method will be removed in a future version.
     */
    @Deprecated(forRemoval = true)
    default String asJson() {
        return EventUtils.toJson(this);
    }
}
