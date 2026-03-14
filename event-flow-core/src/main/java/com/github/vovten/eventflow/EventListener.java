package com.github.vovten.eventflow;

import com.github.vovten.eventflow.dispatcher.EventDispatcher;
import com.github.vovten.eventflow.event.Event;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotation applied to public methods that handle events dispatched by the dispatcher
 * (see {@link EventDispatcher}).
 * <p>Methods annotated with this must accept exactly one parameter of type
 * {@link Event}.
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-06
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EventListener {
}
