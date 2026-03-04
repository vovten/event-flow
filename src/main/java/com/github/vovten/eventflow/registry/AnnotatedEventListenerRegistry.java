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
 * Registry of event listeners based on annotations
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class AnnotatedEventListenerRegistry implements EventListenerRegistry {
    private final Map<Class<? extends Event>, List<Pair<Object, Method>>> eventListeners;

    /**
     * Constructor for event listener registry
     */
    public AnnotatedEventListenerRegistry() {
        this.eventListeners = new HashMap<>();
    }

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

    @Override
    public int listenerCount() {
        return eventListeners.size();
    }

    @Override
    public boolean isEmpty() {
        return eventListeners.isEmpty();
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
            if (method.isAnnotationPresent(com.github.vovten.eventflow.annotation.EventListener.class)) {
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

    private List<EventListener> createEventListeners(List<Pair<Object, Method>> pairs) {
        return pairs.stream()
                .<EventListener>map(pair -> new MethodInvokingEventListener(pair.getLeft(), pair.getRight()))
                .toList();
    }

    /**
     * Wrapper that invokes a method on an object when an event is received
     */
    private static class MethodInvokingEventListener implements EventListener {
        private final Object bean;
        private final Method method;

        MethodInvokingEventListener(Object bean, Method method) {
            this.bean = bean;
            this.method = method;
        }

        @Override
        public List<Class<? extends Event>> events() {
            return List.of((Class<? extends Event>) method.getParameterTypes()[0]);
        }

        @Override
        public void onEvent(Event event) {
            try {
                method.invoke(bean, event);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new EventListenerInvocationException(bean, event, e);
            }
        }
    }
}
