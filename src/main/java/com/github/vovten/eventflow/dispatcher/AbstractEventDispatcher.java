package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventDispatcher;
import com.github.vovten.eventflow.EventListener;
import com.github.vovten.eventflow.registry.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.registry.EventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringAnnotationBasedEventListenerRegistry;
import com.github.vovten.eventflow.registry.SpringInterfaceBasedEventListenerRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

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
public abstract class AbstractEventDispatcher implements EventDispatcher, ApplicationListener<ContextRefreshedEvent> {

    private final String eventListenerScanPackage;
    private final ExecutorService executorService;
    private final EventListenerRegistry listenerRegistry;

    protected AbstractEventDispatcher(ExecutorService executorService) {
        this(executorService, EMPTY);
    }

    protected AbstractEventDispatcher(ExecutorService executorService, String eventListenerScanPackage) {
        this.executorService = executorService;
        this.eventListenerScanPackage = eventListenerScanPackage;
        this.listenerRegistry = new CompositeEventListenerRegistry(new ArrayList<>(
                List.of(new SpringAnnotationBasedEventListenerRegistry(),
                        new SpringInterfaceBasedEventListenerRegistry()
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
    public void onApplicationEvent(ContextRefreshedEvent event) {
        var applicationContext = event.getApplicationContext();
        listenerRegistry.merge(
                new SpringAnnotationBasedEventListenerRegistry(
                        eventListenerScanPackage,
                        applicationContext
                )
        );
        listenerRegistry.merge(
                new SpringInterfaceBasedEventListenerRegistry(
                        applicationContext
                )
        );
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
