package com.github.vovten.eventflow.transport;

import com.github.vovten.eventflow.Event;

import java.util.concurrent.BlockingDeque;

/**
 * Провайдер очередей для in-memory транспортов.
 * <p>
 * Позволяет получать общую очередь по имени транспорта,
 * что обеспечивает связь между publisher и dispatcher.
 */
public interface QueueProvider {

    /**
     * Получает очередь для транспорта с указанным именем.
     *
     * @param transportName имя транспорта
     * @return очередь для событий
     * @throws IllegalArgumentException если транспорт с таким именем не найден
     */
    BlockingDeque<Event> getQueue(String transportName);

    /**
     * Проверяет наличие транспорта с указанным именем.
     *
     * @param transportName имя транспорта
     * @return true если транспорт существует
     */
    boolean hasTransport(String transportName);
}
