package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;

import java.util.List;
import java.util.Map;

/**
 * Simple {@link EventHandlerRegistry} backed by a {@link Map} of event class → handler list.
 * <p>
 * Intended for use in unit tests where a real registry is needed without mocking.
 *
 * @since 1.1.0
 */
class MapBasedHandlerRegistry implements EventHandlerRegistry {

    private final Map<Class<?>, List<EventHandler>> handlerMap;

    MapBasedHandlerRegistry(Map<Class<?>, List<EventHandler>> handlerMap) {
        this.handlerMap = handlerMap;
    }

    @Override
    public List<EventHandler> getHandlers(Event event) {
        return handlerMap.getOrDefault(event.type(), List.of());
    }

    @Override
    public void register(Object eventHandler) {
    }

    @Override
    public boolean unregister(Object eventHandler) {
        return false;
    }

    @Override
    public boolean isRegistered(Object eventHandler) {
        return false;
    }

    @Override
    public void merge(EventHandlerRegistry registry) {
    }

    @Override
    public int handlerCount() {
        return 0;
    }

    @Override
    public String name() {
        return "test-registry";
    }
}
