package com.github.vovten.eventflow;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

/**
 * Событие, которое возникает в приложении и может быть передано всем заинтересованным
 * (компоненты внутри приложения, компоненты сторонних приложений (микросервисы)).
 *
 * @author Vladimir Aleshkov, 20.11.2024.
 */

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Event {

    /**
     * @return тип события
     */
    Class<? extends Event> type();

    /**
     * Список шин, в которые будет происходить публикация данного события.
     * По умолчанию событие публикуется только во внутреннюю шину {@link EventBus#INTERNAL}.
     */
    default List<EventBus> eventBusTypes() {
        return List.of(EventBus.INTERNAL);
    }

    /**
     * @return событие в формате JSON
     */
    default String asJson() {
        return EventUtils.toJson(this);
    }
}
