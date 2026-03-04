package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;

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
    public List<EventListener> getListeners(Event event) {
        return registries.stream()
                .flatMap(registry -> registry.getListeners(event).stream())
                .toList();
    }

    @Override
    public int listenerCount() {
        return registries.stream()
                .mapToInt(EventListenerRegistry::listenerCount)
                .sum();
    }

    @Override
    public boolean isEmpty() {
        return registries.stream().allMatch(EventListenerRegistry::isEmpty);
    }

    @Override
    public void register(Object eventListener) {
        registries.forEach(registry -> registry.register(eventListener));
    }

    @Override
    public boolean isRegistered(Object eventListener) {
        return registries.stream().anyMatch(registry -> registry.isRegistered(eventListener));
    }

    @Override
    public void merge(EventListenerRegistry registry) {
        if (registry instanceof CompositeEventListenerRegistry composite) {
            this.registries.addAll(composite.registries);
        } else {
            this.registries.add(registry);
        }
    }
}
