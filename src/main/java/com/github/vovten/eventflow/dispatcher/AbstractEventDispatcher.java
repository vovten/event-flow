package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Abstract event dispatcher
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
@Slf4j
public abstract class AbstractEventDispatcher implements EventDispatcher {

    private final ExecutorService executorService;
    private final EventListenerRegistry listenerRegistry;

    protected AbstractEventDispatcher(ExecutorService executorService,
                                      EventListenerRegistry listenerRegistry) {
        this.executorService = executorService;
        this.listenerRegistry = listenerRegistry;
    }

    @Override
    public void dispatch(Event event) {
        List<EventListener> listeners = listenerRegistry.getListeners(event);
        if (listeners.isEmpty()) {
            log.debug("No listeners found for event: {}", event);
            return;
        }
        for (EventListener listener : listeners) {
            executorService.execute(() -> listener.onEvent(event));
        }
    }

    @Override
    public void register(Object listener) {
        if (!isRegistered(listener)) {
            listenerRegistry.register(listener);
        }
    }

    @Override
    public boolean isRegistered(Object listener) {
        return listenerRegistry.isRegistered(listener);
    }
}
