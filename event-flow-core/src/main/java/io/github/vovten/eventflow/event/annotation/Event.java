package io.github.vovten.eventflow.event.annotation;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;
import io.github.vovten.eventflow.lifecycle.EventLifecycle;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for marking POJO/record classes as event payloads and configuring their publishing metadata.
 * <p>
 * When a POJO/record is wrapped in an {@link io.github.vovten.eventflow.event.Envelope}, this annotation
 * provides default configuration for event publication, such as target channels and lifecycle tracking.
 * <p>
 * <b>Configuration parameters:</b>
 * <ul>
 *   <li>{@code channels} — target event channel classes for routing</li>
 *   <li>{@code lifecycle} — lifecycle tracking level (default: {@link EventLifecycle#PERSISTED})</li>
 * </ul>
 * <p>
 * <b>Priority:</b> Factory method parameters take precedence over this annotation.
 * If values are specified via {@link io.github.vovten.eventflow.event.Envelope#of(Object, Class[])},
 * the corresponding annotation attributes are ignored.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * package com.example.events;
 *
 * import io.github.vovten.eventflow.event.annotation.Event;
 * import io.github.vovten.eventflow.channel.ExternalEventChannel;
 *
 * @Event(channels = ExternalEventChannel.class, lifecycle = EventLifecycle.MANAGED)
 * public record OrderCreatedEvent(String orderId) {}
 *
 * // Envelope will use ExternalEventChannel
 * Envelope.of(new OrderCreatedEvent("123"))
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.1.0
 * @see EventChannel
 * @see EventLifecycle
 * @see io.github.vovten.eventflow.event.Envelope
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Event {

    /**
     * Target event channel classes for routing.
     *
     * @return channel classes for event routing
     */
    Class<? extends EventChannel>[] channels() default InternalEventChannel.class;

    /**
     * Lifecycle tracking level for this event.
     * <p>
     * Controls whether the event is persisted and whether acknowledgment events
     * ({@code SuccessAck} / {@code FailureAck}) are generated after handler execution.
     *
     * @return lifecycle tracking level
     * @see EventLifecycle
     */
    EventLifecycle lifecycle() default EventLifecycle.PERSISTED;
}