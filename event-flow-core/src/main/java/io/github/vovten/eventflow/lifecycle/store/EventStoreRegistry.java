package io.github.vovten.eventflow.lifecycle.store;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for resolving {@link EventStore} implementations by type name.
 * <p>
 * Stores are registered by their {@link EventStore#getType()} identifier
 * and resolved at configuration time via the {@code store.type} property.
 * <p>
 * Built-in types:
 * <ul>
 *   <li>{@code "db"} — {@link JdbcEventStore} (database-backed, requires DataSource)</li>
 *   <li>{@code "in-memory"} — {@link InMemoryEventStore} (non-persistent)</li>
 * </ul>
 * <p>
 * Custom stores can be registered using {@link #register(EventStore)}.
 * The type identifier is taken automatically from the store itself.
 *
 * @author Vladimir Aleshkov
 * @see EventStore
 * @see JdbcEventStore
 * @see InMemoryEventStore
 * @since 1.3.0
 */
public class EventStoreRegistry {

    private static final Logger log = LoggerFactory.getLogger(EventStoreRegistry.class);

    private final Map<String, EventStore> stores = new ConcurrentHashMap<>();

    /**
     * Register an event store implementation.
     * <p>
     * If a store with the same type identifier already exists, it will be
     * overwritten (a warning is logged). This allows user-provided stores
     * to override built-in ones.
     *
     * @param store the store instance (must not be null)
     * @throws IllegalArgumentException if store is null
     */
    public void register(EventStore store) {
        if (store == null) {
            throw new IllegalArgumentException("EventStore must not be null");
        }
        String type = store.getType();
        if (stores.containsKey(type)) {
            log.warn("Overwriting existing EventStore: type='{}'", type);
        }
        stores.put(type, store);
    }

    /**
     * Resolve an event store by its type name.
     *
     * @param type the store type identifier (e.g., "db", "in-memory")
     * @return the event store
     * @throws IllegalArgumentException if no store is registered for the given type
     */
    public EventStore resolve(String type) {
        EventStore store = stores.get(type);
        if (store == null) {
            throw new IllegalArgumentException(
                    "No EventStore found for type: '" + type + "'. " +
                            "Available types: " + stores.keySet());
        }
        return store;
    }

    /**
     * Get a set of all registered store type names.
     *
     * @return unmodifiable set of registered type identifiers
     */
    public Set<String> getRegisteredTypes() {
        return Collections.unmodifiableSet(stores.keySet());
    }
}
