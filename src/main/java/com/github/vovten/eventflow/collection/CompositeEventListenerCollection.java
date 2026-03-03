package com.github.vovten.eventflow.collection;

import com.github.vovten.eventflow.Event;

import java.util.List;

/**
 * Composition of event listener collections
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class CompositeEventListenerCollection implements EventListenerCollection {
    private final List<EventListenerCollection> collections;

    /**
     * Constructor for creating compositions of event listener collections
     */
    public CompositeEventListenerCollection(List<EventListenerCollection> collections) {
        this.collections = collections;
    }

    @Override
    public boolean pass(Event event) {
        long count = collections.stream()
                .map(collection -> collection.pass(event))
                .filter(b -> b)
                .count();
        return count > 0;
    }

    @Override
    public int size() {
        int size = 0;
        for (var collection : collections) {
            size += collection.size();
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void add(Object eventListener) {
        collections.forEach(collection -> collection.add(eventListener));
    }

    @Override
    public boolean contains(Object eventListener) {
        return collections.stream().anyMatch(collection -> collection.contains(eventListener));
    }

    public void add(EventListenerCollection eventListenerCollection) {
        collections.add(eventListenerCollection);
    }
}
