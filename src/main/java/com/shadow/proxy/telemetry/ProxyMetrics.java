package com.shadow.proxy.telemetry;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.resilience.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Per-process metrics. Labels deliberately exclude SQL, users, and session IDs. */
public final class ProxyMetrics implements AutoCloseable {
    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final AtomicInteger clients = new AtomicInteger();
    private final AtomicInteger backendInFlight = new AtomicInteger();
    private final JvmGcMetrics gcMetrics = new JvmGcMetrics();

    public ProxyMetrics() {
        registry.config().commonTags("application", "shadow-proxy");
        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        gcMetrics.bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new UptimeMetrics().bindTo(registry);
        Gauge.builder("shadow.client.connections", clients, AtomicInteger::get)
                .description("Open frontend TCP connections, including clients before startup").register(registry);
        Gauge.builder("shadow.backend.inflight", backendInFlight, AtomicInteger::get)
                .description("Backend tasks outstanding, including connection-pool waits").register(registry);
        // Register zero-valued series so an idle dashboard is useful immediately.
        for (CommandType type : CommandType.values()) {
            registry.counter("shadow.queries.received", "type", type.name());
        }
        for (String layer : new String[]{"l1", "l2"}) {
            for (String result : new String[]{"hit", "miss", "error"}) {
                registry.counter("shadow.cache.lookups", "layer", layer, "result", result);
            }
        }
    }

    public PrometheusMeterRegistry registry() { return registry; }

    public void clientConnected() {
        clients.incrementAndGet();
        registry.counter("shadow.client.accepted").increment();
    }

    public void clientDisconnected() {
        clients.decrementAndGet();
        registry.counter("shadow.client.disconnected").increment();
    }

    public void queryReceived(CommandType type) {
        registry.counter("shadow.queries.received", "type", type.name()).increment();
    }

    public void cacheLookup(String layer, String result) {
        registry.counter("shadow.cache.lookups", "layer", layer, "result", result).increment();
    }

    public void backendStarted() { backendInFlight.incrementAndGet(); }
    public void backendFinished() { backendInFlight.decrementAndGet(); }

    /** Includes pool acquisition, SQL execution, and response encoding; excludes socket delivery. */
    public void recordBackend(String node, CommandType type, String outcome, long elapsedNanos) {
        Timer.builder("shadow.backend.query")
                .tags("node", node, "type", type.name(), "outcome", outcome)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                .maximumExpectedValue(Duration.ofSeconds(30))
                .register(registry).record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    public void observeCircuit(CircuitBreaker breaker) {
        Gauge.builder("shadow.circuit.state", breaker, b -> switch (b.getState()) {
            case CLOSED -> 0;
            case OPEN -> 1;
            case HALF_OPEN -> 2;
        }).tag("node", breaker.getNodeId())
                .description("Circuit state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN").register(registry);
    }

    @Override
    public void close() {
        gcMetrics.close();
        registry.close();
    }
}
