package io.github.vovten.eventflow.benchmark;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;

/**
 * Benchmark logging utilities.
 * Controls log levels for benchmark tests.
 */
public final class BenchmarkLogConfig {

    private BenchmarkLogConfig() {
    }

    /**
     * Set INFO level for benchmark tests, WARN for other event-flow logging.
     * Call this in @BeforeAll or static initializer of benchmark tests.
     */
    public static void configureForBenchmarks() {
        // Set benchmark package to INFO
        Logger benchmark = (Logger) org.slf4j.LoggerFactory.getLogger("io.github.vovten.eventflow.benchmark");
        benchmark.setLevel(Level.INFO);
        
        // Set main event-flow to WARN (reduce noise)
        Logger eventflow = (Logger) org.slf4j.LoggerFactory.getLogger("io.github.vovten.eventflow");
        eventflow.setLevel(Level.WARN);
        
        // Keep Kafka INFO level
        Logger kafka = (Logger) org.slf4j.LoggerFactory.getLogger("org.apache.kafka");
        kafka.setLevel(Level.WARN);
    }
}