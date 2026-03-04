package com.github.vovten.eventflow.registry;

import org.apache.commons.lang3.StringUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Registry of event listeners based on annotations, extracted from the Spring context
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class SpringAnnotationBasedEventListenerRegistry extends AnnotatedEventListenerRegistry {
    private final String scanPackage;
    private final ApplicationContext applicationContext;

    /**
     * Constructor for event listener registry
     *
     * @param scanPackage        package to scan for event listeners
     * @param applicationContext application context
     */
    public SpringAnnotationBasedEventListenerRegistry(String scanPackage, ApplicationContext applicationContext) {
        super();
        this.scanPackage = scanPackage;
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Constructor for event listener registry
     */
    public SpringAnnotationBasedEventListenerRegistry() {
        super();
        this.scanPackage = EMPTY;
        this.applicationContext = null;
    }

    @Override
    protected void registerIfAnnotationPresent(Object bean) {
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
            allBeans().forEach(this::register);
        }
    }

    private List<Object> allBeans() {
        return Arrays.stream(applicationContext.getBeanDefinitionNames())
                .map(applicationContext::getBean)
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
}
