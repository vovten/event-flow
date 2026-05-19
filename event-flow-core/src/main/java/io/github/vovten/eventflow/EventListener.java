package io.github.vovten.eventflow;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.event.Event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotation applied to public methods that handle events dispatched by the dispatcher
 * (see {@link EventDispatcher}).
 * <p>Methods annotated with this must accept exactly one parameter of type
 * {@link Event} or {@link io.github.vovten.eventflow.event.Envelope}.
 * <p>
 * The annotation can optionally specify the domain event type to handle:
 * <pre>{@code
 * @EventListener(DomainOrderEvent.class)
 * public void onEvent(Envelope<DomainOrderEvent> event) {...}
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EventListener {

    /**
     * The domain event type to handle.
     * If not specified, the handler is registered based on the method parameter type.
     *
     * @return the domain event class
     */
    Class<?> value() default Event.class;
}
