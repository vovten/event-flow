package io.github.vovten.eventflow.registry;

import io.github.vovten.eventflow.EventHandler;
import io.github.vovten.eventflow.EventListener;
import io.github.vovten.eventflow.event.Envelope;
import io.github.vovten.eventflow.event.Event;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Spring-aware registry that discovers event handlers from the application context
 * based on the {@code @EventListener} annotation.
 * <p>
 * This registry extends {@link EventListenerRegistry} with Spring integration:
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
 *     public EventHandlerRegistry listenerRegistry(ApplicationContext context) {
 *         return new SpringEventListenerRegistry("com.example", context);
 *     }
 * }
 *
 * // Handler bean — automatically discovered
 * @Component
 * public class OrderEventHandler {
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
 * This registry implements ApplicationListener of ContextRefreshedEvent.
 * When the Spring context is refreshed, it automatically scans and registers
 * all eligible beans.
 * <p>
 * <b>Proxy support:</b>
 * Uses {@link ClassUtils#getUserClass(Class)} to handle Spring proxies correctly,
 * ensuring annotated methods are discovered even on proxied beans.
 *
 * @author Vladimir Aleshkov
 * @since 1.0.0
 * @see EventListenerRegistry
 * @see SpringEventSubscriberRegistry
 * @see EventHandler
 */
public class SpringEventListenerRegistry extends EventListenerRegistry
        implements ApplicationListener<ContextRefreshedEvent> {

    private static final Pattern packagePattern =
            Pattern.compile("^([a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)*)?$", Pattern.CASE_INSENSITIVE);

    /**
     * Package name to scan for handler beans.
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
    public SpringEventListenerRegistry(ApplicationContext applicationContext, String scanPackage) {
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
    }

    /**
     * Initializes after construction
     */
    public void postConstructInitialize() {
        init();
    }

    /**
     * Register a Spring bean if its methods have @EventListener annotation.
     * <p>
     * Uses {@link ClassUtils#getUserClass(Class)} to handle Spring proxies correctly.
     * Supports annotation value for specifying domain event type.
     *
     * @param bean the Spring bean to scan
     */
    @Override
    protected void registerIfAnnotationPresent(Object bean) {
        Method[] methods = ClassUtils.getUserClass(bean.getClass()).getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(EventListener.class)) {
                checkMethodSignature(method);
                EventListener annotation = method.getAnnotation(EventListener.class);
                Class<?> eventType = resolveListenerEventType(method, annotation);
                registerListener(bean, method, eventType);
            }
        }
    }

    /**
     * Resolve the event type for listener registration.
     * If method parameter is Envelope and annotation value is not specified,
     * throws exception requiring explicit domain event type.
     *
     * @param method the annotated method
     * @param annotation the annotation
     * @return the event type to register
     * @throws IllegalArgumentException if Envelope is used without annotation value
     */
    private Class<?> resolveListenerEventType(Method method, EventListener annotation) {
        Class<?> annotationValue = annotation.value();
        Class<?> paramType = method.getParameterTypes()[0];
        if (Envelope.class.isAssignableFrom(paramType)) {
            if (annotationValue == null || annotationValue.equals(Event.class)) {
                throw new IllegalArgumentException(
                        "Listener '" + method.getDeclaringClass().getSimpleName() + "." + method.getName() +
                                "()': When method parameter is Envelope, annotation value must specify domain event type. " +
                                "Use @EventListener(YourDomainEvent.class) instead of @EventListener");
            }
            return annotationValue;
        }
        if (annotationValue != null && !annotationValue.equals(Event.class)) {
            return annotationValue;
        }
        return paramType;
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

    @Override
    public String name() {
        return "spring-annotation[" + scanPackage + "]";
    }

    /**
     * Get the package name used for scanning handler beans.
     *
     * @return the scan package name
     */
    public String getScanPackage() {
        return scanPackage;
    }

    /**
     * Get all eligible beans from the application context.
     * <p>
     * Beans are filtered by the configured scan package.
     *
     * @return list of beans to register as handlers
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
