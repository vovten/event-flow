package com.github.vovten.eventflow.registry;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Registry of event listeners based on annotations, extracted from the Spring context
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class SpringAnnotationBasedEventListenerRegistry extends AnnotationBasedEventListenerRegistry {
    private final String scanPackage;
    private final ApplicationContext applicationContext;

    /**
     * Constructor for event listener registry
     *
     * @param scanPackage        package to scan for event listeners
     * @param executorService    service for background event processing
     * @param applicationContext application context
     */
    public SpringAnnotationBasedEventListenerRegistry(String scanPackage, ExecutorService executorService,
                                                      ApplicationContext applicationContext) {
        super(executorService);
        this.scanPackage = scanPackage;
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Constructor for event listener registry
     *
     * @param executorService service for background event processing
     */
    public SpringAnnotationBasedEventListenerRegistry(ExecutorService executorService) {
        super(executorService);
        this.scanPackage = EMPTY;
        this.applicationContext = null;
    }

    @Override
    public void registerIfAnnotationPresent(Object bean) {
        Method[] methods = ClassUtils.getUserClass(bean.getClass()).getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(com.github.vovten.eventflow.annotation.EventListener.class)) {
                checkMethodSignature(method);
                registerListener(bean, method);
            }
        }
    }

    private void init() {
        if (applicationContext != null) {
            allBeans().forEach(this::registerIfAnnotationPresent);
        }
    }

    private List<Object> allBeans() {
        return Arrays.stream(applicationContext.getBeanDefinitionNames())
                .map(name -> applicationContext.getBean(name))
                .filter(this::beanInScanPackage)
                .toList();
    }

    private boolean beanInScanPackage(Object bean) {
        if (scanPackage.isEmpty()) {
            return true;
        } else {
            return StringUtils.startsWithIgnoreCase(bean.getClass().getName(), scanPackage);
        }
    }

    @Override
    protected void checkMethodSignature(Method method) {
        var types = method.getParameterTypes();
        if (types.length != 1 || !com.github.vovten.eventflow.Event.class.isAssignableFrom(types[0])) {
            throw new InvalidEventListenerMethodSignatureException(
                    method.getDeclaringClass().getName(), method.getName());
        }
    }
}
