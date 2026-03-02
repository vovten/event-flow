package com.github.vovten.eventflow.event.collection;

import org.springframework.context.ApplicationContext;
import org.springframework.util.CollectionUtils;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.EventListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * Collection of event listeners based on interface, extracted from the Spring context
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class SpringEventListenerInterfaceCollection implements EventListenerCollection {
    private final ExecutorService executorService;
    private final Map<Class<? extends Event>, List<EventListener>> eventListeners;
    private final ApplicationContext applicationContext;

    public SpringEventListenerInterfaceCollection(ExecutorService executorService, ApplicationContext applicationContext) {
        this.eventListeners = new HashMap<>();
        this.executorService = executorService;
        this.applicationContext = applicationContext;
        this.init();
    }

    public SpringEventListenerInterfaceCollection(ExecutorService executorService) {
        this.eventListeners = new HashMap<>();
        this.executorService = executorService;
        this.applicationContext = null;
    }

    @Override
    public boolean pass(Event event) {
        if (eventListeners.isEmpty()) {
            return false;
        }
        var listeners = eventListeners.get(event.getClass());
        if (CollectionUtils.isEmpty(listeners)) {
            return false;
        } else {
            listeners.forEach(eventListener -> 
                executorService.execute(() -> eventListener.onEvent(event)));
            return true;
        }
    }

    @Override
    public int size() {
        return eventListeners.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void add(Object eventListener) {
        if (eventListener instanceof EventListener listener) {
            for (Class<? extends Event> event : listener.events()) {
                var listeners = eventListeners.getOrDefault(event, new ArrayList<>());
                listeners.add(listener);
                eventListeners.put(event, listeners);
            }
        }
    }

    @Override
    public boolean contains(Object eventListener) {
        if (!(eventListener instanceof EventListener)) {
            return false;
        } else {
            return eventListeners.values().stream()
                    .anyMatch(listeners -> listeners.contains(eventListener));
        }
    }

    @Override
    public void add(EventListenerCollection eventListenerCollection) {
        throw new UnsupportedOperationException("Adding listener collection is not supported");
    }

    private void init() {
        if (applicationContext != null) {
            for (EventListener listener : applicationContext.getBeansOfType(EventListener.class).values()) {
                for (Class<? extends Event> event : listener.events()) {
                    var listeners = eventListeners.get(event);
                    if (CollectionUtils.isEmpty(listeners)) {
                        eventListeners.put(event, new ArrayList<>(List.of(listener)));
                    } else {
                        listeners.add(listener);
                    }
                }
            }
        }
    }
}
