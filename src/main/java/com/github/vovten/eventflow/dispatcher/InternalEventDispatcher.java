package com.github.vovten.eventflow.dispatcher;

import com.github.vovten.eventflow.Event;
import com.github.vovten.eventflow.EventBus;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;

/**
 * Event dispatcher that listens to events within a single application (see {@link EventBus#INTERNAL})
 *
 * @author Vladimir Aleshkov, 21.11.2024.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "event.internal.dispatcher.enabled", havingValue = "true")
public class InternalEventDispatcher extends AbstractEventDispatcher {

    private final BlockingDeque<Event> eventQueue;
    private final ExecutorService executorService;

    public InternalEventDispatcher(ExecutorService executorService, BlockingDeque<Event> eventQueue) {
        super(executorService);
        this.eventQueue = eventQueue;
        this.executorService = executorService;
    }

    @Autowired
    public InternalEventDispatcher(ExecutorService executorService,
                                   BlockingDeque<Event> eventQueue,
                                   @Value("${event.listener.scan.package:}") String eventListenerScanPackage) {
        super(executorService, eventListenerScanPackage);
        this.eventQueue = eventQueue;
        this.executorService = executorService;
    }

    @PostConstruct
    public void init() {
        executorService.execute(new EventQueueListener(eventQueue));
    }

    private final class EventQueueListener implements Runnable {
        private final BlockingDeque<Event> eventQueue;

        private EventQueueListener(BlockingDeque<Event> eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    tryDispatch(eventQueue.take());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new EventDispatcherException("Error getting event from queue. Thread interrupted.", e);
            }
        }

        private void tryDispatch(Event event) {
            try {
                dispatch(event);
            } catch (Exception e) {
                log.error("Error processing event from queue: " + event, e);
            }
        }
    }
}
