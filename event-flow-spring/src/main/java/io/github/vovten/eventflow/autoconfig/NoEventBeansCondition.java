package io.github.vovten.eventflow.autoconfig;

import io.github.vovten.eventflow.dispatcher.EventDispatcher;
import io.github.vovten.eventflow.publisher.EventPublisher;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.lang.NonNull;

import java.util.Objects;

/**
 * Custom Spring condition that checks for the absence of both EventPublisher and EventDispatcher beans
 * @since 1.0.0
 */
public class NoEventBeansCondition implements Condition {

    @Override
    public boolean matches(ConditionContext context, @NonNull AnnotatedTypeMetadata metadata) {
        ConfigurableListableBeanFactory beanFactory = Objects.requireNonNull(context.getBeanFactory());
        boolean hasNoPublisher = beanFactory.getBeansOfType(EventPublisher.class).isEmpty();
        boolean hasNoDispatcher = beanFactory.getBeansOfType(EventDispatcher.class).isEmpty();
        return hasNoPublisher && hasNoDispatcher;
    }
}
