package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Registry of event listeners based on interface
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class InterfaceBasedEventListenerRegistry implements EventListenerRegistry {
    private final ExecutorService executorService;
    private final Map<Class<? extends Event>, List<EventListener>> eventListeners;

    /**
     * Constructor for event listener registry
     *
     * @param executorService service for background event processing
     */
    public InterfaceBasedEventListenerRegistry(ExecutorService executorService) {
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
            listeners.forEach(eventListener ->
                executorService.execute(() -> eventListener.onEvent(event)));
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
        if (eventListener instanceof EventListener listener) {
            for (Class<? extends Event> event : listener.events()) {
                var listeners = eventListeners.getOrDefault(event, new ArrayList<>());
                listeners.add(listener);
                eventListeners.put(event, listeners);
            }
        }
    }

    @Override
    public boolean isRegistered(Object eventListener) {
        if (!(eventListener instanceof EventListener)) {
            return false;
        } else {
            return eventListeners.values().stream()
                    .anyMatch(listeners -> listeners.contains(eventListener));
        }
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
