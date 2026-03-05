package com.github.vovten.eventflow.registry;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import com.github.vovten.eventflow.EventListener;

import java.util.concurrent.ExecutorService;

/**
 * Registry of event listeners based on interface, extracted from the Spring context
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class SpringInterfaceEventListenerRegistry extends InterfaceEventListenerRegistry
        implements ApplicationListener<ContextRefreshedEvent> {

    private ApplicationContext applicationContext;

    /**
     * Constructor for event listener registry
     *
     * @param applicationContext application context
     */
    public SpringInterfaceEventListenerRegistry(ApplicationContext applicationContext) {
        super();
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Constructor for event listener registry
     */
    public SpringInterfaceEventListenerRegistry() {
        super();
        this.applicationContext = null;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        this.applicationContext = event.getApplicationContext();
        this.init();
    }

    private void init() {
        if (applicationContext != null) {
            for (EventListener listener : applicationContext.getBeansOfType(EventListener.class).values()) {
                register(listener);
            }
        }
    }
}
