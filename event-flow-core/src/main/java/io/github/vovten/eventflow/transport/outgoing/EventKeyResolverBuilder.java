package io.github.vovten.eventflow.transport.outgoing;

import java.util.Objects;
import java.util.function.Function;

/**
 * Builder that creates an {@link EventKeyResolver} from a configuration string,
 * as found in transport configuration (e.g. Spring's
 * {@code event-flow.channels[*].transports[*].key}).
 * <p>
 * <b>Supported specifications</b> (case-insensitive):
 * <ul>
 *   <li>{@code "none"}, blank or {@code null} — {@link EventKeyResolver#none()} (round-robin)</li>
 *   <li>{@code "process-id"} — {@link EventKeyResolver#processId()}</li>
 *   <li>{@code "event-type"} — {@link EventKeyResolver#eventType()}</li>
 *   <li>{@code "metadata:<name>"} — {@link EventKeyResolver#metadata(String)}</li>
 *   <li>{@code "bean:<name>"} — an externally registered resolver looked up by name
 *       through {@link #withBeanLookup(Function)}</li>
 * </ul>
 * <p>
 * The builder depends on no DI framework: the {@code bean:} strategy is resolved via a
 * pluggable lookup function, so Spring can wire
 * {@code name -> beanFactory.getBean(name, EventKeyResolver.class)} while any other
 * environment can supply its own registry (e.g. a {@code Map}).
 * <p>
 * Examples:
 * <pre>{@code
 * EventKeyResolver byProcessId = new EventKeyResolverBuilder("process-id").build();
 *
 * EventKeyResolver custom = new EventKeyResolverBuilder("bean:orderKeyResolver")
 *         .withBeanLookup(name -> applicationContext.getBean(name, EventKeyResolver.class))
 *         .build();
 * }</pre>
 *
 * @author Vladimir Aleshkov
 * @since 1.4.0
 * @see EventKeyResolver
 */
public final class EventKeyResolverBuilder {

    private static final String STRATEGY_NONE = "none";
    private static final String STRATEGY_PROCESS_ID = "process-id";
    private static final String STRATEGY_EVENT_TYPE = "event-type";
    private static final String PREFIX_METADATA = "metadata:";
    private static final String PREFIX_BEAN = "bean:";

    private final String spec;
    private Function<String, ? extends EventKeyResolver> beanLookup;

    /**
     * Creates a builder for the given key strategy specification.
     *
     * @param spec the configuration string (may be null or blank — means "none")
     */
    public EventKeyResolverBuilder(String spec) {
        this.spec = spec;
    }

    /**
     * Provides the lookup used to resolve {@code bean:<name>} specifications.
     * Without it, building a {@code bean:} specification fails.
     *
     * @param beanLookup function resolving a name to a key resolver
     * @return this builder
     * @throws NullPointerException if {@code beanLookup} is null
     */
    public EventKeyResolverBuilder withBeanLookup(Function<String, ? extends EventKeyResolver> beanLookup) {
        this.beanLookup = Objects.requireNonNull(beanLookup, "beanLookup must not be null");
        return this;
    }

    /**
     * Builds the resolver described by the specification. Surrounding whitespace of
     * the specification and of the name part after a prefix is ignored.
     *
     * @return the resolved {@link EventKeyResolver}, never null
     * @throws IllegalArgumentException if the specification is unknown or malformed,
     *                                  if a {@code bean:} specification is used without
     *                                  a configured lookup, or if the lookup returns null
     */
    public EventKeyResolver build() {
        String strategy = spec == null ? "" : spec.trim();
        if (strategy.isBlank() || STRATEGY_NONE.equalsIgnoreCase(strategy)) {
            return EventKeyResolver.none();
        }
        if (STRATEGY_PROCESS_ID.equalsIgnoreCase(strategy)) {
            return EventKeyResolver.processId();
        }
        if (STRATEGY_EVENT_TYPE.equalsIgnoreCase(strategy)) {
            return EventKeyResolver.eventType();
        }
        if (hasPrefix(strategy, PREFIX_METADATA)) {
            String metadataKey = extractName(strategy, PREFIX_METADATA, "metadata name", "orderId");
            return EventKeyResolver.metadata(metadataKey);
        }
        if (hasPrefix(strategy, PREFIX_BEAN)) {
            String beanName = extractName(strategy, PREFIX_BEAN, "bean name", "orderKeyResolver");
            return resolveBean(beanName);
        }
        throw unknownStrategy(strategy);
    }

    private EventKeyResolver resolveBean(String beanName) {
        if (beanLookup == null) {
            throw new IllegalArgumentException(
                    "Kafka 'key' strategy 'bean:" + beanName + "' requires a bean lookup, "
                            + "configure one via withBeanLookup(...)"
            );
        }
        EventKeyResolver resolver = beanLookup.apply(beanName);
        if (resolver == null) {
            throw new IllegalArgumentException(
                    "Kafka 'key' strategy 'bean:" + beanName + "' references unknown EventKeyResolver bean '" + beanName + "'"
            );
        }
        return resolver;
    }

    private static boolean hasPrefix(String strategy, String prefix) {
        return strategy.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * Extracts the name part after a strategy prefix, e.g. {@code orderId} from
     * {@code metadata:orderId}.
     *
     * @param strategy the specification, starting with {@code prefix}
     * @param prefix   the strategy prefix including the colon (e.g. {@code metadata:})
     * @param what     human-readable name of the missing part for the error message
     * @param example  sample value for the error message (e.g. {@code orderId})
     * @return the non-blank name part following the prefix
     * @throws IllegalArgumentException if the name part is blank
     */
    private static String extractName(String strategy, String prefix, String what, String example) {
        String name = strategy.substring(prefix.length()).trim();
        if (name.isBlank()) {
            throw new IllegalArgumentException(
                    "Kafka 'key' strategy '" + prefix + "' requires a " + what + ", e.g. '" + prefix + example + "'"
            );
        }
        return name;
    }

    private static IllegalArgumentException unknownStrategy(String strategy) {
        return new IllegalArgumentException(
                "Unknown Kafka 'key' strategy '" + strategy + "'. Supported values: "
                        + "none, process-id, event-type, metadata:<name>, bean:<beanName>"
        );
    }
}
