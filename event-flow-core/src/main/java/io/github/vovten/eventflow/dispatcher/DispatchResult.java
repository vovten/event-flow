package io.github.vovten.eventflow.dispatcher;

import java.util.List;

/**
 * Result of event dispatch operation.
 * Contains information about handlers that were invoked.
 *
 * @author Vladimir Aleshkov
 * @since 2026-05-11
 */
public record DispatchResult(
        int totalHandlers,
        int invokedHandlers,
        List<String> handlerNames
) {

    /**
     * Creates a dispatch result with empty handler names.
     *
     * @param totalHandlers     total handlers found
     * @param invokedHandlers   handlers that were invoked
     */
    public DispatchResult(int totalHandlers, int invokedHandlers) {
        this(totalHandlers, invokedHandlers, List.of());
    }
}