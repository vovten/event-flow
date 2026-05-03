package io.github.vovten.eventflow;

import io.github.vovten.eventflow.event.Event;
import io.github.vovten.eventflow.test.CompositeTestEvent;
import io.github.vovten.eventflow.test.ExternalTestEvent;
import io.github.vovten.eventflow.test.TestEvent;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Component
public class TestEventListener implements EventSubscriber {

    private String annotationResult;
    private String compositeResult;
    private String interfaceResult;
    private CountDownLatch latch;

    public String getAnnotationResult() {
        return annotationResult;
    }

    public void setAnnotationResult(String annotationResult) {
        this.annotationResult = annotationResult;
    }

    public String getCompositeResult() {
        return compositeResult;
    }

    public void setCompositeResult(String compositeResult) {
        this.compositeResult = compositeResult;
    }

    public String getInterfaceResult() {
        return interfaceResult;
    }

    public void setInterfaceResult(String interfaceResult) {
        this.interfaceResult = interfaceResult;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

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
    public List<Class<?>> events() {
        return List.of(TestEvent.class, ExternalTestEvent.class);
    }
}
