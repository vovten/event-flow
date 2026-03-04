package com.github.vovten.eventflow.registry;

import org.springframework.context.ApplicationContext;
import com.github.vovten.eventflow.EventListener;

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
     * @param applicationContext application context
     */
    public SpringInterfaceBasedEventListenerRegistry(ApplicationContext applicationContext) {
        super();
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Constructor for event listener registry
     */
    public SpringInterfaceBasedEventListenerRegistry() {
        super();
        this.applicationContext = null;
    }

    private void init() {
        if (applicationContext != null) {
            for (EventListener listener : applicationContext.getBeansOfType(EventListener.class).values()) {
                register(listener);
            }
        }
    }
}
