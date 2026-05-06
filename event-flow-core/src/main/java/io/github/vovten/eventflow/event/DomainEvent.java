package io.github.vovten.eventflow.event;

import io.github.vovten.eventflow.channel.EventChannel;
import io.github.vovten.eventflow.channel.InternalEventChannel;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for POJO domain events to configure publishing metadata.
 * <p>
 * When a POJO is wrapped in an {@link Envelope}, this annotation provides default
 * configuration for event publication, such as target channels.
 * <p>
 * <b>Configuration parameters:</b>
 * <ul>
 *   <li>{@code channels} — target event channel classes for routing</li>
 * </ul>
 * <p>
 * <b>Priority:</b> Factory method parameters take precedence over this annotation.
 * If values are specified via {@link Envelope#of(Object, Class[])},
 * the corresponding annotation attributes are ignored.
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * @DomainEvent(
 *     channels = {ExternalEventChannel.class, BroadcastEventChannel.class}
 * )
 * public record OrderCreatedEvent(String orderId) {}
 *
 * // Envelope will use ExternalEventChannel and BroadcastEventChannel
 * Envelope.of(new OrderCreatedEvent("123"))
 *
 * // Factory method overrides annotation channels
 * Envelope.of(new OrderCreatedEvent("123"), InternalEventChannel.class)
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 2026-05-05
 * @see EventChannel
 * @see Envelope
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface DomainEvent {

    /**
     * Target event channel classes for routing.
     *
     * @return channel classes for event routing
     */
    Class<? extends EventChannel>[] channels() default InternalEventChannel.class;
}
