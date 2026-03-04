package com.github.vovten.eventflow.registry;

import org.springframework.context.ApplicationContext;
import org.springframework.util.CollectionUtils;
import com.github.vovten.eventflow.EventListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Registry of event listeners based on interface, extracted from the Spring context
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class SpringInterfaceBasedEventListenerRegistry extends InterfaceBasedEventListenerRegistry {
    private final ApplicationContext applicationContext;

    /**
     * Constructor for event listener registry
     *
     * @param executorService    service for background event processing
     * @param applicationContext application context
     */
    public SpringInterfaceBasedEventListenerRegistry(ExecutorService executorService, ApplicationContext applicationContext) {
        super(executorService);
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Constructor for event listener registry
     *
     * @param executorService service for background event processing
     */
    public SpringInterfaceBasedEventListenerRegistry(ExecutorService executorService) {
        super(executorService);
        this.applicationContext = null;
    }

    @Override
    public void register(Object eventListener) {
        if (eventListener instanceof EventListener listener) {
            registerListener(listener);
        }
    }

    private void init() {
        if (applicationContext != null) {
            for (EventListener listener : applicationContext.getBeansOfType(EventListener.class).values()) {
                registerListener(listener);
            }
        }
    }
}
