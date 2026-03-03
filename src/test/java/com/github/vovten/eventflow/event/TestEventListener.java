package com.github.vovten.eventflow.event;

import com.github.vovten.eventflow.event.annotation.EventListener;
import lombok.Getter;
import org.springframework.stereotype.Component;

@Getter
@Component
public class TestEventListener {

    private String result;

    @EventListener
    public void onEvent(TestEvent event) {
        result = event.id();
    }
}
