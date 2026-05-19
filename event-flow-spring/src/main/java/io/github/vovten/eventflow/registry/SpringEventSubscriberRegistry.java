package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventSubscriber;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

/**
 * Spring-aware registry that discovers event subscribers implementing {@link EventSubscriber}
 * from the application context.
 * <p>
 * This registry extends {@link EventSubscriberRegistry} with Spring integration:
 * <ul>
 *   <li>Automatically discovers EventSubscriber beans from Spring context</li>
 *   <li>Integrates with Spring's application lifecycle</li>
 *   <li>Supports all Spring bean scopes and features</li>
 * </ul>
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Automatic EventSubscriber bean discovery</li>
 *   <li>Lifecycle integration via ContextRefreshedEvent</li>
 *   <li>Support for Spring dependency injection in subscribers</li>
 *   <li>Compatible with all Spring bean scopes</li>
 * </ul>
 * <p>
 * <b>Usage with Spring Boot:</b>
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventHandlerRegistry listenerRegistry(ApplicationContext context) {
 *         return new SpringEventSubscriberRegistry(context);
 *     }
 * }
 *
 * // Subscriber bean — automatically discovered
 * @Component
 * public class OrderCreatedSubscriber implements EventSubscriber {
 *
 *     private final OrderService orderService;
 *
 *     public OrderCreatedSubscriber(OrderService orderService) {
 *         this.orderService = orderService;
 *     }
 *
 *     @Override
 *     public List<Class<?>> events() {
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
 * This registry implements ApplicationListener of ContextRefreshedEvent.
 * When the Spring context is refreshed, it automatically discovers and registers
 * all beans implementing {@code EventSubscriber}.
 * <p>
 * <b>Dependency injection:</b>
 * Since subscribers are Spring beans, they can have any dependencies injected
 * via constructor, setter, or field injection.
 * <p>
 * <b>Combining with annotation-based registry:</b>
 * For maximum flexibility, use both registries in a composite:
 * <pre>{@code
 * @Bean
 * public EventHandlerRegistry listenerRegistry(ApplicationContext context) {
 *     return new CompositeEventHandlerRegistry(List.of(
 *         new SpringEventListenerRegistry("com.example", context),
 *         new SpringEventSubscriberRegistry(context)
 *     ));
 * }
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventSubscriberRegistry
 * @see SpringEventListenerRegistry
 * @see EventSubscriber
 */
public class SpringEventSubscriberRegistry extends EventSubscriberRegistry
        implements ApplicationListener<ContextRefreshedEvent> {

    /**
     * Spring application context.
     */
    private ApplicationContext applicationContext;

    /**
     * Creates a Spring-aware interface-based registry.
     * <p>
     * Immediately scans and registers EventSubscriber beans from the application context.
     * Also registers itself as an ApplicationListener to receive ContextRefreshedEvent.
     *
     * @param applicationContext Spring application context (required)
     * @throws IllegalArgumentException if applicationContext is null
     */
    public SpringEventSubscriberRegistry(ApplicationContext applicationContext) {
        super();
        if (applicationContext == null) {
            throw new IllegalArgumentException("ApplicationContext is required");
        }
        this.applicationContext = applicationContext;
    }

    /**
     * Initializes after construction
     */
    public void postConstructInitialize() {
        init();
    }

    /**
     * Handle Spring context refresh event.
     * <p>
     * When the context is refreshed, re-scans EventSubscriber beans from the context.
     *
     * @param event the context refreshed event
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        this.applicationContext = event.getApplicationContext();
        this.init();
    }

    /**
     * Initialize by scanning and registering EventSubscriber beans from the context.
     */
    private void init() {
        if (applicationContext != null) {
            for (EventSubscriber subscriber : applicationContext.getBeansOfType(EventSubscriber.class).values()) {
                register(subscriber);
            }
        }
    }

    @Override
    public String name() {
        return "spring-interface";
    }
}
