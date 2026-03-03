package com.github.vovten.eventflow.event.collection;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.event.annotation.EventListener;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.apache.commons.lang3.StringUtils.EMPTY;

/**
 * Collection of event listeners based on annotations, extracted from the Spring context
 *
 * @author Vladimir Aleshkov, 07.12.2024.
 */
public class SpringEventListenerAnnotationCollection implements EventListenerCollection {
    private final ExecutorService executorService;
    private final Map<Class<? extends Event>, List<Pair<Object, Method>>> eventListeners;
    private final String scanPackage;
    private final ApplicationContext applicationContext;

    /**
     * Constructor for event listener collection
     *
     * @param scanPackage        package to scan for event listeners
     * @param executorService    service for background event processing
     * @param applicationContext application context
     */
    public SpringEventListenerAnnotationCollection(String scanPackage, ExecutorService executorService,
                                                    ApplicationContext applicationContext) {
        this.scanPackage = scanPackage;
        this.eventListeners = new HashMap<>();
        this.executorService = executorService;
        this.applicationContext = applicationContext;
        this.init();
    }

    /**
     * Constructor for event listener collection
     *
     * @param executorService service for background event processing
     */
    public SpringEventListenerAnnotationCollection(ExecutorService executorService) {
        this.scanPackage = EMPTY;
        this.eventListeners = new HashMap<>();
        this.executorService = executorService;
        this.applicationContext = null;
    }

    @Override
    public boolean pass(Event event) {
        if (eventListeners.isEmpty()) {
            return false;
        }
        var listeners = eventListeners.get(event.getClass());
        boolean hasListeners = !CollectionUtils.isEmpty(listeners);
        
        // Also check for generic Event.class listeners
        if (eventListeners.containsKey(Event.class)) {
            if (hasListeners) {
                listeners = new ArrayList<>(listeners);
                listeners.addAll(eventListeners.get(Event.class));
            } else {
                listeners = eventListeners.get(Event.class);
                hasListeners = true;
            }
        }
        
        if (hasListeners && listeners != null) {
            listeners.forEach(pair -> executorService.execute(() ->
                    invokeEventListener(pair.getLeft(), pair.getRight(), event)));
            return true;
        }
        return false;
    }

    @Override
    public int size() {
        return eventListeners.size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public void add(Object eventListener) {
        addIfAnnotationPresent(eventListener);
    }

    @Override
    public boolean contains(Object eventListener) {
        return eventListeners.values().stream()
                .flatMap(List::stream)
                .anyMatch(pair -> pair.getLeft().equals(eventListener));
    }

    @Override
    public void add(EventListenerCollection eventListenerCollection) {
        throw new UnsupportedOperationException("Adding listener collection is not supported");
    }

    private void invokeEventListener(Object bean, Method method, Event event) {
        try {
            method.invoke(bean, event);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new EventListenerInvocationException(bean, event, e);
        }
    }

    private void init() {
        if (applicationContext != null) {
            allBeans().forEach(this::addIfAnnotationPresent);
        }
    }

    private void addIfAnnotationPresent(Object bean) {
        Method[] methods = ClassUtils.getUserClass(bean.getClass()).getMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(EventListener.class)) {
                checkMethodSignature(method);
                addListener(bean, method);
            }
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

    private void checkMethodSignature(Method method) {
        var types = method.getParameterTypes();
        if (types.length != 1 || !Event.class.isAssignableFrom(types[0])) {
            throw new IllegalEventListenerMethodSignatureException(
                    method.getDeclaringClass().getName(), method.getName());
        }
    }

    private void addListener(Object bean, Method method) {
        var eventType = (Class<? extends Event>) method.getParameterTypes()[0];
        var listeners = eventListeners.computeIfAbsent(eventType, k -> new ArrayList<>());
        listeners.add(new ImmutablePair<>(bean, method));
    }
}
