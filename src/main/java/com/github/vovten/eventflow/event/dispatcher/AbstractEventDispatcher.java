package com.github.vovten.eventflow.event.dispatcher;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.EventDispatcher;
import com.github.vovten.eventflow.event.collection.CompositeEventListenerCollection;
import com.github.vovten.eventflow.event.collection.EventListenerCollection;
import com.github.vovten.eventflow.event.collection.SpringEventListenerAnnotationCollection;
import com.github.vovten.eventflow.event.collection.SpringEventListenerInterfaceCollection;
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
    private final EventListenerCollection eventListenerCollection;

    protected AbstractEventDispatcher(ExecutorService executorService) {
        this(executorService, EMPTY);
    }

    protected AbstractEventDispatcher(ExecutorService executorService, String eventListenerScanPackage) {
        this.executorService = executorService;
        this.eventListenerScanPackage = eventListenerScanPackage;
        this.eventListenerCollection = new CompositeEventListenerCollection(new ArrayList<>(
                List.of(
                        new SpringEventListenerAnnotationCollection(executorService),
                        new SpringEventListenerInterfaceCollection(executorService)
                )
        ));
    }

    @Override
    public void dispatch(Event event) {
        boolean passed = eventListenerCollection.pass(event);
        if (!passed) {
            log.debug("No listeners found for event: {}", event);
        }
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        var applicationContext = event.getApplicationContext();
        eventListenerCollection.add(new SpringEventListenerAnnotationCollection(
                eventListenerScanPackage, executorService, applicationContext));
        eventListenerCollection.add(new SpringEventListenerInterfaceCollection(
                executorService, applicationContext));
    }

    @Override
    public void register(Object listener) {
        if (!isRegistered(listener)) {
            eventListenerCollection.add(listener);
        }
    }

    @Override
    public boolean isRegistered(Object listener) {
        return eventListenerCollection.contains(listener);
    }
}
