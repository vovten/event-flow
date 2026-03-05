package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceEventListenerRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Abstract event dispatcher
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
@Slf4j
public abstract class AbstractEventDispatcher implements EventDispatcher {

    private final ExecutorService executorService;
    private final EventListenerRegistry listenerRegistry;

    protected AbstractEventDispatcher(ExecutorService executorService) {
        this(executorService, EMPTY);
    }

    protected AbstractEventDispatcher(ExecutorService executorService, String eventListenerScanPackage) {
        this(executorService, eventListenerScanPackage, createDefaultRegistry(eventListenerScanPackage));
    }

    protected AbstractEventDispatcher(ExecutorService executorService,
                                      String eventListenerScanPackage,
                                      EventListenerRegistry listenerRegistry) {
        this.executorService = executorService;
        this.listenerRegistry = listenerRegistry;
    }

    private static EventListenerRegistry createDefaultRegistry(String eventListenerScanPackage) {
        return new CompositeEventListenerRegistry(new ArrayList<>(
                List.of(new SpringAnnotationEventListenerRegistry(eventListenerScanPackage, null),
                        new SpringInterfaceEventListenerRegistry()
                )
        ));
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
