package com.github.vovten.eventflow.registry;

import com.github.vovten.eventflow.annotation.EventListener;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Spring-aware registry that discovers event listeners from the application context
 * based on the {@code @EventListener} annotation.
 * <p>
 * This registry extends {@link AnnotationEventListenerRegistry} with Spring integration:
 * <ul>
 *   <li>Automatically scans Spring beans for annotated methods</li>
 *   <li>Supports package-based filtering of beans</li>
 *   <li>Integrates with Spring's application lifecycle</li>
 *   <li>Handles proxy classes correctly</li>
 * </ul>
 * <p>
 * <b>Key features:</b>
 * <ul>
 *   <li>Automatic bean discovery from Spring context</li>
 *   <li>Package scan filtering — register only beans from specific packages</li>
 *   <li>Support for Spring proxy classes (AOP, transactions, etc.)</li>
 *   <li>Lifecycle integration via ContextRefreshedEvent</li>
 * </ul>
 * <p>
 * <b>Usage with Spring Boot:</b>
 * <pre>{@code
 * @Configuration
 * public class EventConfig {
 *
 *     @Bean
 *     public EventListenerRegistry listenerRegistry(ApplicationContext context) {
 *         return new SpringAnnotationEventListenerRegistry("com.example", context);
 *     }
 * }
 *
 * // Listener bean — automatically discovered
 * @Component
 * public class OrderEventListener {
 *
 *     @EventListener
 *     public void handleOrderCreated(OrderCreatedEvent event) {
 *         // Handle event
 *     }
 * }
 * }</pre>
 * <p>
 * <b>Package filtering:</b>
 * The {@code scanPackage} parameter controls which beans are scanned:
 * <ul>
 *   <li>Empty string — all beans in the context</li>
 *   <li>"com.example" — beans in com.example and subpackages</li>
 *   <li>"com.example.service" — beans in com.example.service only</li>
 * </ul>
 * <p>
 * <b>Spring lifecycle:</b>
 * This registry implements {@link ApplicationListener<ContextRefreshedEvent>}.
 * When the Spring context is refreshed, it automatically scans and registers
 * all eligible beans.
 * <p>
 * <b>Proxy support:</b>
 * Uses {@link ClassUtils#getUserClass(Class)} to handle Spring proxies correctly,
 * ensuring annotated methods are discovered even on proxied beans.
 *
 * @author Vladimir Aleshkov
 * @since 07.12.2024
 * @see AnnotationEventListenerRegistry
 * @see SpringInterfaceEventListenerRegistry
 * @see EventListener
 */
public class SpringAnnotationEventListenerRegistry extends AnnotationEventListenerRegistry
        implements ApplicationListener<ContextRefreshedEvent> {

    private static final Pattern packagePattern =
            Pattern.compile("^([a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)*)?$", Pattern.CASE_INSENSITIVE);

    /**
     * Package name to scan for listener beans.
     * Empty string means all beans.
     */
    private final String scanPackage;

    /**
     * Spring application context.
     */
    private ApplicationContext applicationContext;

    /**
     * Creates a Spring-aware annotation-based registry with package filtering.
     * <p>
     * Immediately scans and registers beans from the application context.
     * Also registers itself as an ApplicationListener to receive ContextRefreshedEvent.
     *
     * @param applicationContext Spring application context (required)
     * @param scanPackage        package prefix for filtering beans (required)
     * @throws IllegalArgumentException if applicationContext is null
     */
    public SpringAnnotationEventListenerRegistry(ApplicationContext applicationContext, String scanPackage) {
        super();
        if (applicationContext == null) {
            throw new IllegalArgumentException("ApplicationContext is required");
        }
        if (scanPackage == null || scanPackage.isEmpty()) {
            throw new IllegalStateException("Scan package is required");
        }
        if (!packagePattern.matcher(scanPackage).matches()) {
            throw new IllegalArgumentException("Invalid Java package name: " + scanPackage);
        }
        this.scanPackage = scanPackage;
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Register a Spring bean if its methods have @EventListener annotation.
     * <p>
     * Uses {@link ClassUtils#getUserClass(Class)} to handle Spring proxies correctly.
     *
     * @param bean the Spring bean to scan
     */
    @Override
    protected void registerIfAnnotationPresent(Object bean) {
        Method[] methods = ClassUtils.getUserClass(bean.getClass()).getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(EventListener.class)) {
                checkMethodSignature(method);
                registerListener(bean, method);
            }
        }
    }

    /**
     * Handle Spring context refresh event.
     * <p>
     * When the context is refreshed, updates the application context reference
     * and re-initializes bean scanning.
     *
     * @param event the context refreshed event
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        this.applicationContext = event.getApplicationContext();
        this.init();
    }

    /**
     * Initialize by scanning and registering beans from the context.
     */
    private void init() {
        if (applicationContext != null) {
            allBeans().forEach(this::register);
        }
    }

    /**
     * Get all eligible beans from the application context.
     * <p>
     * Beans are filtered by the configured scan package.
     *
     * @return list of beans to register as listeners
     */
    private List<Object> allBeans() {
        return Arrays.stream(applicationContext.getBeanDefinitionNames())
                .map(applicationContext::getBean)
                .filter(this::isWithinScanPackage)
                .toList();
    }

    /**
     * Check if a bean is in the configured scan package.
     *
     * @param bean the bean to check
     * @return true if the bean should be scanned
     */
    private boolean isWithinScanPackage(Object bean) {
        String beanClassName = ClassUtils.getUserClass(bean.getClass()).getPackageName();
        return beanClassName.equals(scanPackage) || beanClassName.startsWith(scanPackage + ".");
    }
}
