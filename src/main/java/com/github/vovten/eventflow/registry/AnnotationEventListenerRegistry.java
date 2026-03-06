package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry that discovers and manages event listeners based on the {@code @EventListener} annotation.
 * <p>
 * This registry scans objects for methods annotated with {@link com.github.vovten.eventflow.annotation.EventListener}
 * and registers them as event listeners. Each annotated method must accept exactly one parameter
 * that extends {@link Event}.
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Annotation-based listener discovery — no interface implementation required</li>
 *   <li>Support for multiple listener methods per object</li>
 *   <li>Support for generic Event.class listeners</li>
 *   <li>Automatic method signature validation</li>
 * </ul>
 * <p>
 * <b>Listener method requirements:</b>
 * <ul>
 *   <li>Must be annotated with {@code @EventListener}</li>
 *   <li>Must have exactly one parameter</li>
 *   <li>Parameter must be of type {@code Event} or its subclass</li>
 *   <li>Return type is ignored (typically void)</li>
 * </ul>
 * <p>
 * <b>Usage example:</b>
 * <pre>{@code
 * // Define listener with annotated methods
 * public class MyListener {
 *
 *     @EventListener
 *     public void handleOrderCreated(OrderCreatedEvent event) {
 *         System.out.println("Order created: " + event.getOrderId());
 *     }
 *
 *     @EventListener
 *     public void handleEvent(Event event) {
 *         // Generic handler for all events
 *     }
 * }
 *
 * // Register listener
 * EventListenerRegistry registry = new AnnotationEventListenerRegistry();
 * registry.register(new MyListener());
 * }</pre>
 * <p>
 * <b>Event type matching:</b>
 * Listeners are matched to events by exact class type. For example, a listener for
 * {@code OrderCreatedEvent} will only receive {@code OrderCreatedEvent} instances,
 * not subclasses or parent types.
 * <p>
 * <b>Generic listeners:</b>
 * Methods that accept {@code Event.class} as parameter will receive all events,
 * regardless of their specific type.
 * <p>
 * <b>Error handling:</b>
 * If a method has an invalid signature, {@link InvalidEventListenerMethodSignatureException}
 * is thrown during registration. If a method invocation fails,
 * {@link EventListenerInvocationException} wraps the underlying exception.
 *
 * @author Vladimir Aleshkov
 * @since 07.12.2024
 * @see com.github.vovten.eventflow.annotation.EventListener
 * @see Event
 * @see InvalidEventListenerMethodSignatureException
 * @see EventListenerInvocationException
 */
public class AnnotationEventListenerRegistry implements EventListenerRegistry {

    /**
     * Map of event types to listener-method pairs.
     * Key: Event class
     * Value: List of (listener object, method) pairs
     */
    private final Map<Class<? extends Event>, List<Pair<Object, Method>>> eventListeners;

    /**
     * Creates a new annotation-based event listener registry.
     */
    public AnnotationEventListenerRegistry() {
        this.eventListeners = new HashMap<>();
    }

    /**
     * Get all listeners for the specified event type.
     * <p>
     * This method returns:
     * <ol>
     *   <li>Listeners registered for the exact event class</li>
     *   <li>Listeners registered for generic Event.class (if any)</li>
     * </ol>
     *
     * @param event the event to find listeners for
     * @return list of listeners for this event type
     */
    @Override
    public List<EventListener> getListeners(Event event) {
        List<EventListener> listeners = new ArrayList<>();
        List<Pair<Object, Method>> methods = eventListeners.getOrDefault(event.getClass(), List.of());

        // Also add listeners for generic Event.class
        if (eventListeners.containsKey(Event.class)) {
            listeners.addAll(createEventListeners(eventListeners.get(Event.class)));
        }

        listeners.addAll(createEventListeners(methods));
        return listeners;
    }

    /**
     * Get the number of registered event types.
     * <p>
     * Note: This counts unique event types, not individual listener methods.
     *
     * @return number of registered event types
     */
    @Override
    public int listenerCount() {
        return eventListeners.size();
    }

    /**
     * Register an object as an event listener.
     * <p>
     * Scans all public methods of the object for {@code @EventListener} annotation.
     * Annotated methods are registered as event handlers.
     *
     * @param eventListener object with @EventListener-annotated methods
     * @throws InvalidEventListenerMethodSignatureException if method signature is invalid
     */
    @Override
    public void register(Object eventListener) {
        registerIfAnnotationPresent(eventListener);
    }

    /**
     * Unregister a listener object from all event types.
     *
     * @param eventListener the listener to unregister
     * @return true if the listener was found and removed, false otherwise
     */
    @Override
    public boolean unregister(Object eventListener) {
        for (List<Pair<Object, Method>> pairs : eventListeners.values()) {
            if (pairs.removeIf(pair -> pair.getLeft().equals(eventListener))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Check if an object is registered as a listener.
     *
     * @param eventListener the listener to check
     * @return true if registered, false otherwise
     */
    @Override
    public boolean isRegistered(Object eventListener) {
        return eventListeners.values().stream()
                .flatMap(List::stream)
                .anyMatch(pair -> pair.getLeft().equals(eventListener));
    }

    /**
     * Merging is not supported for this registry implementation.
     *
     * @param registry the registry to merge
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public void merge(EventListenerRegistry registry) {
        throw new UnsupportedOperationException("Merging registries is not supported");
    }

    /**
     * Register a listener if its methods have @EventListener annotation.
     * <p>
     * Scans all public methods and registers those with the annotation.
     *
     * @param bean the listener object to scan
     * @throws InvalidEventListenerMethodSignatureException if method signature is invalid
     */
    protected void registerIfAnnotationPresent(Object bean) {
        Method[] methods = bean.getClass().getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(com.github.vovten.eventflow.annotation.EventListener.class)) {
                checkMethodSignature(method);
                registerListener(bean, method);
            }
        }
    }

    /**
     * Register a specific method as an event listener.
     * <p>
     * The method's first parameter determines the event type it will handle.
     *
     * @param bean the listener object
     * @param method the method to register
     */
    protected void registerListener(Object bean, Method method) {
        var eventType = (Class<? extends Event>) method.getParameterTypes()[0];
        var listeners = eventListeners.computeIfAbsent(eventType, k -> new ArrayList<>());
        listeners.add(new ImmutablePair<>(bean, method));
    }

    /**
     * Validate that a method has a valid listener signature.
     * <p>
     * Requirements:
     * <ul>
     *   <li>Exactly one parameter</li>
     *   <li>Parameter type is Event or subclass</li>
     * </ul>
     *
     * @param method the method to validate
     * @throws InvalidEventListenerMethodSignatureException if signature is invalid
     */
    protected void checkMethodSignature(Method method) {
        var types = method.getParameterTypes();
        if (types.length != 1 || !Event.class.isAssignableFrom(types[0])) {
            throw new InvalidEventListenerMethodSignatureException(
                    method.getDeclaringClass().getName(), method.getName());
        }
    }

    /**
     * Create EventListener wrappers for the given listener-method pairs.
     *
     * @param pairs list of (listener, method) pairs
     * @return list of EventListener wrappers
     */
    private List<EventListener> createEventListeners(List<Pair<Object, Method>> pairs) {
        return pairs.stream()
                .<EventListener>map(pair ->
                        new MethodInvokingEventListener(pair.getLeft(), pair.getRight()))
                .toList();
    }

    /**
     * Wrapper that invokes a method on an object when an event is received.
     * <p>
     * This internal class adapts annotated methods to the EventListener interface.
     *
     * @param object the listener object
     * @param method the method to invoke
     */
    private record MethodInvokingEventListener(Object object, Method method) implements EventListener {

        @Override
        public List<Class<? extends Event>> events() {
            return List.of((Class<? extends Event>) method.getParameterTypes()[0]);
        }

        @Override
        public void onEvent(Event event) {
            try {
                method.invoke(object, event);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new EventListenerInvocationException(object, event, e);
            }
        }
    }
}
