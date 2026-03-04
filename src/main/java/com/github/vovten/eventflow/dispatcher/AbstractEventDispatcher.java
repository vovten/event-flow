package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventDispatcher;
import com.github.vovten.eventflow.collection.CompositeEventListenerRegistry;
import com.github.vovten.eventflow.collection.EventListenerRegistry;
import com.github.vovten.eventflow.collection.SpringAnnotatedEventListenerRegistry;
import com.github.vovten.eventflow.collection.SpringInterfaceBasedEventListenerRegistry;
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
    private final EventListenerRegistry eventListenerRegistry;

    protected AbstractEventDispatcher(ExecutorService executorService) {
        this(executorService, EMPTY);
    }

    protected AbstractEventDispatcher(ExecutorService executorService, String eventListenerScanPackage) {
        this.executorService = executorService;
        this.eventListenerScanPackage = eventListenerScanPackage;
        this.eventListenerRegistry = new CompositeEventListenerRegistry(new ArrayList<>(
                List.of(
                        new SpringAnnotatedEventListenerRegistry(executorService),
                        new SpringInterfaceBasedEventListenerRegistry(executorService)
                )
        ));
    }

    @Override
    public void dispatch(Event event) {
        boolean dispatched = eventListenerRegistry.dispatch(event);
        if (!dispatched) {
            log.debug("No listeners found for event: {}", event);
        }
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        var applicationContext = event.getApplicationContext();
        eventListenerRegistry.merge(
                new SpringAnnotatedEventListenerRegistry(
                        eventListenerScanPackage,
                        executorService,
                        applicationContext
                )
        );
        eventListenerRegistry.merge(
                new SpringInterfaceBasedEventListenerRegistry(
                        executorService,
                        applicationContext
                )
        );
    }

    @Override
    public void register(Object listener) {
        if (!isRegistered(listener)) {
            eventListenerRegistry.register(listener);
        }
    }

    @Override
    public boolean isRegistered(Object listener) {
        return eventListenerRegistry.isRegistered(listener);
    }
}
