package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.EventListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Spring-aware registry that discovers event listeners implementing {@link EventListener}
 * from the application context.
 * <p>
 * This registry extends {@link InterfaceEventListenerRegistry} with Spring integration:
 * <ul>
 *   <li>Automatically discovers EventListener beans from Spring context</li>
 *   <li>Integrates with Spring's application lifecycle</li>
 *   <li>Supports all Spring bean scopes and features</li>
 * </ul>
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Automatic EventListener bean discovery</li>
 *   <li>Lifecycle integration via ContextRefreshedEvent</li>
 *   <li>Support for Spring dependency injection in listeners</li>
 *   <li>Compatible with all Spring bean scopes</li>
 * </ul>
 * <p>
 * <b>Usage with Spring Boot:</b>
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventListenerRegistry listenerRegistry(ApplicationContext context) {
 *         return new SpringInterfaceEventListenerRegistry(context);
 *     }
 * }
 *
 * // Listener bean — automatically discovered
 * @Component
 * public class OrderCreatedListener implements EventListener {
 *
 *     private final OrderService orderService;
 *
 *     public OrderCreatedListener(OrderService orderService) {
 *         this.orderService = orderService;
 *     }
 *
 *     @Override
 *     public List<Class<? extends Event>> events() {
 *         return List.of(OrderCreatedEvent.class);
 *     }
 *
 *     @Override
 *     public void onEvent(Event event) {
 *         OrderCreatedEvent orderEvent = (OrderCreatedEvent) event;
 *         orderService.processOrder(orderEvent.getOrderId());
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Spring lifecycle:</b>
 * This registry implements {@link ApplicationListener<ContextRefreshedEvent>}.
 * When the Spring context is refreshed, it automatically discovers and registers
 * all beans implementing {@code EventListener}.
 * <p>
 * <b>Dependency injection:</b>
 * Since listeners are Spring beans, they can have any dependencies injected
 * via constructor, setter, or field injection.
 * <p>
 * <b>Combining with annotation-based registry:</b>
 * For maximum flexibility, use both registries in a composite:
 * <pre>{@code
 * @Bean
 * public EventListenerRegistry listenerRegistry(ApplicationContext context) {
 *     return new CompositeEventListenerRegistry(List.of(
 *         new SpringAnnotationEventListenerRegistry("com.example", context),
 *         new SpringInterfaceEventListenerRegistry(context)
 *     ));
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 07.12.2024
 * @see InterfaceEventListenerRegistry
 * @see SpringAnnotationEventListenerRegistry
 * @see EventListener
 */
public class SpringInterfaceEventListenerRegistry extends InterfaceEventListenerRegistry
        implements ApplicationListener<ContextRefreshedEvent> {

    /**
     * Spring application context.
     */
    private ApplicationContext applicationContext;

    /**
     * Creates a Spring-aware interface-based registry.
     * <p>
     * Immediately scans and registers EventListener beans from the application context.
     * Also registers itself as an ApplicationListener to receive ContextRefreshedEvent.
     *
     * @param applicationContext Spring application context (required)
     * @throws IllegalArgumentException if applicationContext is null
     */
    public SpringInterfaceEventListenerRegistry(ApplicationContext applicationContext) {
        super();
        if (applicationContext == null) {
            throw new IllegalArgumentException("ApplicationContext is required");
        }
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Handle Spring context refresh event.
     * <p>
     * When the context is refreshed, re-scans EventListener beans from the context.
     *
     * @param event the context refreshed event
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        this.applicationContext = event.getApplicationContext();
        this.init();
    }

    /**
     * Initialize by scanning and registering EventListener beans from the context.
     */
    private void init() {
        if (applicationContext != null) {
            for (EventListener listener : applicationContext.getBeansOfType(EventListener.class).values()) {
                register(listener);
            }
        }
    }
}
