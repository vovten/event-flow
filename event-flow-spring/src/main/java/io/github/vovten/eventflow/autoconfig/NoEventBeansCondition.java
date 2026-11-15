package io.github.vovten.eventflow.autoconfig;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.publisher.EventPublisher;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.lang.NonNull;

import java.util.Objects;

/**
 * Spring condition that checks for the absence of both {@link EventPublisher} and
 * {@link EventDispatcher} bean definitions.
 * <p>
 * The check inspects bean definitions only ({@code allowEagerInit=false}): it must
 * not instantiate publishers, dispatchers or the beans behind their factory
 * methods while Spring is still evaluating conditions. Like every
 * {@link Condition}, it sees the bean definitions registered at evaluation time;
 * user-defined beans from configuration classes processed later are not visible.
 *
 * @since 1.0.0
 */
public class NoEventBeansCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        ConfigurableListableBeanFactory beanFactory = Objects.requireNonNull(context.getBeanFactory());
        return hasNoBeanDefinitions(beanFactory, EventPublisher.class)
                && hasNoBeanDefinitions(beanFactory, EventDispatcher.class);
    }

    private static boolean hasNoBeanDefinitions(ListableBeanFactory beanFactory, Class<?> type) {
        // allowEagerInit=false: inspect bean definitions without instantiation.
        return beanFactory.getBeanNamesForType(type, true, false).length == 0;
    }
}
