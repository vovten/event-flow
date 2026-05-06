package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventListener;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Registry that discovers and manages event listeners based on the {@code @EventListener} annotation.
 * <p>
 * This registry scans objects for methods annotated with {@link EventListener}
 * and registers them as event handlers. Each annotated method must accept exactly one parameter
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
 * EventHandlerRegistry registry = new EventListenerRegistry();
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
 * {@link EventHandlerInvocationException} wraps the underlying exception.
 *
 * @author Vladimir Aleshkov
 * @since 2024-12-07
 * @see EventListener
 * @see Event
 * @see InvalidEventListenerMethodSignatureException
 * @see EventHandlerInvocationException
 */
public class EventListenerRegistry implements EventHandlerRegistry {

    /**
     * Map of event types to event handlers.
     * Key: Event class (or any class for non-Event payloads)
     * Value: List of EventHandler instances
     * <p>
     * Thread-safe: uses ConcurrentHashMap + CopyOnWriteArrayList for read-heavy workload.
     * EventHandler wrappers are created once during registration and reused.
     */
    private final Map<Class<?>, List<EventHandler>> eventListeners;

    /**
     * Cache for combined handler lists to avoid reallocation on repeated getHandlers() calls.
     * Key: Event class (or any class for non-Event payloads)
     * Value: Unmodifiable combined list (generic + specific handlers)
     * <p>
     * Cache is invalidated on register/unregister operations.
     */
    private final Map<Class<?>, List<EventHandler>> combinedHandlersCache;

    /**
     * Creates a new annotation-based event listener registry.
     * Thread-safe for concurrent register/unregister/dispatch operations.
     */
    public EventListenerRegistry() {
        this.eventListeners = new ConcurrentHashMap<>();
        this.combinedHandlersCache = new ConcurrentHashMap<>();
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
     * @return list of listeners for this event type (unmodifiable)
     */
    @Override
    public List<EventHandler> getHandlers(Event event) {
        Class<?> eventType = resolveEventType(event);
        List<EventHandler> genericHandlers = eventListeners.get(Event.class);
        List<EventHandler> specificHandlers = eventListeners.get(eventType);

        if (genericHandlers == null || genericHandlers.isEmpty()) {
            return (specificHandlers == null || specificHandlers.isEmpty())
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(specificHandlers);
        }
        if (specificHandlers == null || specificHandlers.isEmpty()) {
            return Collections.unmodifiableList(genericHandlers);
        }
        return combinedHandlersCache.computeIfAbsent(eventType,
                key -> buildCombinedList(genericHandlers, specificHandlers));
    }

    /**
     * Resolve the event type to search for handlers.
     * If the event is an Envelope, resolves to the payload type.
     *
     * @param event the event or envelope
     * @return resolved event type
     */
    private Class<?> resolveEventType(Event event) {
        if (event instanceof Envelope<?> envelope) {
            return envelope.payload().getClass();
        }
        return event.getClass();
    }

    /**
     * <p>
     * Scans all public methods and registers those with the annotation.
     *
     * @param bean the listener object to scan
     * @throws InvalidEventListenerMethodSignatureException if method signature is invalid
     */
    protected void registerIfAnnotationPresent(Object bean) {
        Method[] methods = bean.getClass().getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(EventListener.class)) {
                checkMethodSignature(method);
                EventListener annotation = method.getAnnotation(EventListener.class);
                Class<?> eventType = resolveListenerEventType(method, annotation);
                registerListener(bean, method, eventType);
            }
        }
    }

    /**
     * Resolve the event type for listener registration.
     * If method parameter is Envelope and annotation value is not specified,
     * throws exception requiring explicit domain event type.
     *
     * @param method the annotated method
     * @param annotation the annotation
     * @return the event type to register
     * @throws IllegalArgumentException if Envelope is used without annotation value
     */
    private Class<?> resolveListenerEventType(Method method, EventListener annotation) {
        Class<?> annotationValue = annotation.value();
        Class<?> paramType = method.getParameterTypes()[0];
        if (Envelope.class.isAssignableFrom(paramType)) {
            if (annotationValue == null || annotationValue.equals(Event.class)) {
                throw new IllegalArgumentException(
                        "Listener '" + method.getDeclaringClass().getSimpleName() + "." + method.getName() +
                                "()': When method parameter is Envelope, annotation value must specify domain event type. " +
                                "Use @EventListener(YourDomainEvent.class) instead of @EventListener");
            }
            return annotationValue;
        }
        if (annotationValue != null && !annotationValue.equals(Event.class)) {
            return annotationValue;
        }
        return paramType;
    }

    /**
     * Register a specific method as an event listener.
     * <p>
     * The method's first parameter determines the event type it will handle.
     * Duplicate registrations (same bean and method) are ignored.
     *
     * @param bean the listener object
     * @param method the method to register
     * @param eventType the event type to register for
     */
    protected void registerListener(Object bean, Method method, Class<?> eventType) {
        var handlers = eventListeners.computeIfAbsent(eventType, k -> new CopyOnWriteArrayList<>());
        var newHandler = new MethodInvokingEventHandler(bean, method);
        boolean exists = handlers.stream()
                .filter(h -> h instanceof MethodInvokingEventHandler mih)
                .anyMatch(h -> {
                    MethodInvokingEventHandler mih = (MethodInvokingEventHandler) h;
                    return mih.object().equals(bean) && mih.method().equals(method);
                });
        if (!exists) {
            handlers.add(newHandler);
            combinedHandlersCache.clear();
        }
    }

    private List<EventHandler> buildCombinedList(List<EventHandler> genericHandlers,
                                                   List<EventHandler> specificHandlers) {
        List<EventHandler> combined = new ArrayList<>(genericHandlers.size() + specificHandlers.size());
        combined.addAll(genericHandlers);
        combined.addAll(specificHandlers);
        return Collections.unmodifiableList(combined);
    }

    /**
     * Get the number of registered event types.
     * <p>
     * Note: This counts unique event types, not individual listener methods.
     *
     * @return number of registered event types
     */
    @Override
    public int handlerCount() {
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
        boolean removed = false;
        for (List<EventHandler> handlers : eventListeners.values()) {
            if (handlers.removeIf(handler -> isHandlerForBean(handler, eventListener))) {
                removed = true;
            }
        }
        if (removed) {
            combinedHandlersCache.clear();
        }
        return removed;
    }

    /**
     * Check if an EventHandler wraps the given bean.
     *
     * @param handler the event handler to check
     * @param bean the bean to compare against
     * @return true if the handler wraps the given bean, false otherwise
     */
    private boolean isHandlerForBean(EventHandler handler, Object bean) {
        if (handler instanceof MethodInvokingEventHandler mih) {
            return mih.object().equals(bean);
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
                .anyMatch(handler -> isHandlerForBean(handler, eventListener));
    }

    /**
     * Merging is not supported for this registry implementation.
     *
     * @param registry the registry to merge
     * @throws UnsupportedOperationException always thrown
     */
    @Override
    public void merge(EventHandlerRegistry registry) {
        throw new UnsupportedOperationException("Merging registries is not supported");
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
        if (types.length != 1) {
            throw new InvalidEventListenerMethodSignatureException(
                    method.getDeclaringClass().getName(), method.getName());
        }
    }

    @Override
    public String name() {
        return "annotation";
    }

    /**
     * Wrapper that invokes a method on an object when an event is received.
     * <p>
     * This internal class adapts annotated methods to the EventHandler interface.
     * Created once during registration and reused, avoiding allocation overhead.
     *
     * @param object the listener object
     * @param method the method to invoke
     */
    public record MethodInvokingEventHandler(Object object, Method method) implements EventHandler {

        @Override
        public void onEvent(Event event) {
            try {
                Object arg = adaptEventToMethodParameter(event, method.getParameterTypes()[0]);
                method.invoke(object, arg);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new EventHandlerInvocationException(object, event, e);
            }
        }

        private Object adaptEventToMethodParameter(Event event, Class<?> expectedType) {
            if (event instanceof Envelope<?> envelope) {
                if (expectedType.isAssignableFrom(Envelope.class)) {
                    return envelope;
                }
                return envelope.payload();
            }
            return event;
        }
    }
}
