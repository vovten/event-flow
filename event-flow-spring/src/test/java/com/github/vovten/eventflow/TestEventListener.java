package com.github.vovten.eventflow;

import com.github.vovten.eventflow.event.Event;
import com.github.vovten.eventflow.test.CompositeTestEvent;
import com.github.vovten.eventflow.test.ExternalTestEvent;
import com.github.vovten.eventflow.test.TestEvent;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Getter
@Setter
@Component
public class TestEventListener implements EventSubscriber {

    private String annotationResult;
    private String compositeResult;
    private String interfaceResult;
    private CountDownLatch latch;

    @EventListener
    public void onEvent(TestEvent event) {
        annotationResult = event.getId();
        if (latch != null) {
            latch.countDown();
        }
    }

    @EventListener
    public void onEvent(ExternalTestEvent event) {
        annotationResult = event.getId();
        if (latch != null) {
            latch.countDown();
        }
    }

    @EventListener
    public void onEvent(CompositeTestEvent event) {
        compositeResult = event.getMessage();
        if (latch != null) {
            latch.countDown();
        }
    }

    @Override
    public void onEvent(Event event) {
        if (event instanceof TestEvent testEvent) {
            interfaceResult = testEvent.getId();
        } else if (event instanceof ExternalTestEvent externalTestEvent) {
            interfaceResult = externalTestEvent.getId();
        }
        if (latch != null) {
            latch.countDown();
        }
    }

    @Override
    public List<Class<? extends Event>> events() {
        return List.of(TestEvent.class, ExternalTestEvent.class);
    }
}
