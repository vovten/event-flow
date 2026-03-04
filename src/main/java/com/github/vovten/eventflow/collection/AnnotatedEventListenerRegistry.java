package com.github.vovten.eventflow.collection;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.annotation.EventListener;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Registry of event listeners based on annotations
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class AnnotatedEventListenerRegistry implements EventListenerRegistry {
    private final ExecutorService executorService;
    private final Map<Class<? extends Event>, List<Pair<Object, Method>>> eventListeners;

    /**
     * Constructor for event listener registry
     *
     * @param executorService service for background event processing
     */
    public AnnotatedEventListenerRegistry(ExecutorService executorService) {
        this.eventListeners = new HashMap<>();
        this.executorService = executorService;
    }

    @Override
    public boolean dispatch(Event event) {
        if (eventListeners.isEmpty()) {
            return false;
        }
        var listeners = eventListeners.get(event.getClass());
        boolean hasListeners = listeners != null && !listeners.isEmpty();

        // Also check for generic Event.class listeners
        if (eventListeners.containsKey(Event.class)) {
            if (hasListeners) {
                listeners = new ArrayList<>(listeners);
                listeners.addAll(eventListeners.get(Event.class));
            } else {
                listeners = eventListeners.get(Event.class);
                hasListeners = true;
            }
        }

        if (hasListeners && listeners != null) {
            listeners.forEach(pair -> executorService.execute(() ->
                    invokeEventListener(pair.getLeft(), pair.getRight(), event)));
            return true;
        }
        return false;
    }

    @Override
    public int listenerCount() {
        return eventListeners.size();
    }

    @Override
    public boolean hasListeners() {
        return listenerCount() > 0;
    }

    @Override
    public void register(Object eventListener) {
        registerIfAnnotationPresent(eventListener);
    }

    @Override
    public boolean isRegistered(Object eventListener) {
        return eventListeners.values().stream()
                .flatMap(List::stream)
                .anyMatch(pair -> pair.getLeft().equals(eventListener));
    }

    @Override
    public void merge(EventListenerRegistry registry) {
        throw new UnsupportedOperationException("Merging registries is not supported");
    }

    /**
     * Register a listener if its methods have @EventListener annotation
     *
     * @param bean the listener object
     */
    public void registerIfAnnotationPresent(Object bean) {
        Method[] methods = bean.getClass().getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(EventListener.class)) {
                checkMethodSignature(method);
                registerListener(bean, method);
            }
        }
    }

    /**
     * Register a listener with the specified method
     *
     * @param bean the listener object
     * @param method the method to invoke
     */
    protected void registerListener(Object bean, Method method) {
        var eventType = (Class<? extends Event>) method.getParameterTypes()[0];
        var listeners = eventListeners.computeIfAbsent(eventType, k -> new ArrayList<>());
        listeners.add(new ImmutablePair<>(bean, method));
    }

    /**
     * Check method signature
     *
     * @param method the method to check
     */
    protected void checkMethodSignature(Method method) {
        var types = method.getParameterTypes();
        if (types.length != 1 || !Event.class.isAssignableFrom(types[0])) {
            throw new InvalidEventListenerMethodSignatureException(
                    method.getDeclaringClass().getName(), method.getName());
        }
    }

    private void invokeEventListener(Object bean, Method method, Event event) {
        try {
            method.invoke(bean, event);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new EventListenerInvocationException(bean, event, e);
        }
    }
}
