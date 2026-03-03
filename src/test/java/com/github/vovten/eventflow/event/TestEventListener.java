package com.github.vovten.eventflow.event;

import com.github.vovten.eventflow.event.annotation.EventListener;
import lombok.Getter;
import lombok.Setter;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CountDownLatch;

@Getter
@Setter
@Component
public class TestEventListener implements com.github.vovten.eventflow.event.EventListener {

    private String annotationResult;
    private String interfaceResult;
    private CountDownLatch latch;

    @EventListener
    public void onEvent(TestEvent event) {
        annotationResult = event.id();
        if (latch != null) {
            latch.countDown();
        }
    }

    @Override
    public List<Class<? extends Event>> events() {
        return List.of(TestEvent.class);
    }

    @Override
    public void onEvent(Event event) {
        TestEvent testEvent = (TestEvent) event;
        interfaceResult = testEvent.id();
    }
}
