package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Registry of event listeners based on interface
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class InterfaceEventListenerRegistry implements EventListenerRegistry {
    private final Map<Class<? extends Event>, List<EventListener>> eventListeners;

    /**
     * Constructor for event listener registry
     */
    public InterfaceEventListenerRegistry() {
        this.eventListeners = new HashMap<>();
    }

    @Override
    public List<EventListener> getListeners(Event event) {
        List<EventListener> listeners = new ArrayList<>(eventListeners.getOrDefault(event.getClass(), List.of()));

        // Also add listeners for generic Event.class
        if (eventListeners.containsKey(Event.class)) {
            listeners.addAll(eventListeners.get(Event.class));
        }
        return listeners;
    }

    @Override
    public int listenerCount() {
        return eventListeners.size();
    }

    @Override
    public void register(Object eventListener) {
        if (eventListener instanceof EventListener listener) {
            registerListener(listener);
        }
    }

    @Override
    public boolean unregister(Object eventListener) {
        if (!(eventListener instanceof EventListener)) {
            return false;
        }
        for (List<EventListener> listeners : eventListeners.values()) {
            if (listeners.remove(eventListener)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isRegistered(Object eventListener) {
        if (!(eventListener instanceof EventListener)) {
            return false;
        }
        return eventListeners.values().stream()
                .anyMatch(listeners -> listeners.contains(eventListener));
    }

    @Override
    public void merge(EventListenerRegistry registry) {
        throw new UnsupportedOperationException("Merging registries is not supported");
    }

    /**
     * Register an event listener
     *
     * @param listener the listener to register
     */
    protected void registerListener(EventListener listener) {
        for (Class<? extends Event> event : listener.events()) {
            var listeners = eventListeners.getOrDefault(event, new ArrayList<>());
            listeners.add(listener);
            eventListeners.put(event, listeners);
        }
    }
}
