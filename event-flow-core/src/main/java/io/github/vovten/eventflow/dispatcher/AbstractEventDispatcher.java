package io.github.vovten.eventflow.dispatcher;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.registry.EventHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Abstract event dispatcher
 *
 * @author Vladimir Aleshkov
 * @since 2024-11-21
 */
public abstract class AbstractEventDispatcher implements EventDispatcher {

    private static final Logger log = LoggerFactory.getLogger(AbstractEventDispatcher.class);

    private final ExecutorService executorService;
    private final EventHandlerRegistry handlerRegistry;

    protected AbstractEventDispatcher(ExecutorService executorService,
                                      EventHandlerRegistry handlerRegistry) {
        this.executorService = executorService;
        this.handlerRegistry = handlerRegistry;
    }

    @Override
    public void dispatch(Event event) {
        List<EventHandler> handlers = handlerRegistry.getHandlers(event);
        if (handlers.isEmpty()) {
            log.debug("No handlers found for event: {}", event);
            return;
        }
        for (EventHandler handler : handlers) {
            executorService.execute(() -> handler.onEvent(event));
        }
    }

    @Override
    public void register(Object handler) {
        if (!isRegistered(handler)) {
            handlerRegistry.register(handler);
        }
    }

    @Override
    public boolean isRegistered(Object handler) {
        return handlerRegistry.isRegistered(handler);
    }
}
