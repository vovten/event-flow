package com.github.vovten.eventflow.event.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <p>Annotation applied to public methods that handle events dispatched by the dispatcher
 * (see {@link com.github.vovten.eventflow.event.EventDispatcher}).
 * <p>Methods with this annotation must accept exactly one parameter of type
 * {@link com.github.vovten.eventflow.event.Event}.
 *
 * @author Vladimir Aleshkov, 06.12.2024.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface EventListenerAnnotation {
}
