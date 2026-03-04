package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;

import java.util.List;

/**
 * Composition of event listener registries
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class CompositeEventListenerRegistry implements EventListenerRegistry {
    private final List<EventListenerRegistry> registries;

    /**
     * Constructor for creating composition of event listener registries
     */
    public CompositeEventListenerRegistry(List<EventListenerRegistry> registries) {
        this.registries = registries;
    }

    @Override
    public boolean dispatch(Event event) {
        long count = registries.stream()
                .map(registry -> registry.dispatch(event))
                .filter(b -> b)
                .count();
        return count > 0;
    }

    @Override
    public int listenerCount() {
        int count = 0;
        for (var registry : registries) {
            count += registry.listenerCount();
        }
        return count;
    }

    @Override
    public boolean hasListeners() {
        return listenerCount() > 0;
    }

    @Override
    public void register(Object eventListener) {
        registries.forEach(registry -> registry.register(eventListener));
    }

    @Override
    public boolean isRegistered(Object eventListener) {
        return registries.stream().anyMatch(registry -> registry.isRegistered(eventListener));
    }

    public void merge(EventListenerRegistry registry) {
        if (registry instanceof CompositeEventListenerRegistry composite) {
            this.registries.addAll(composite.registries);
        } else {
            this.registries.add(registry);
        }
    }
}
