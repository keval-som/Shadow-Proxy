package com.shadow.proxy.telemetry;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.postgres.PostgresCommand;
import com.shadow.proxy.resilience.CircuitBreaker;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProxyMetricsTest {
    @Test
    void connectionGaugeTracksMultipleClientsAndDisconnects() {
        try (var metrics = new ProxyMetrics()) {
            var first = new EmbeddedChannel(new ConnectionMetricsHandler(metrics));
            var second = new EmbeddedChannel(new ConnectionMetricsHandler(metrics));
            try {
                assertEquals(2, metrics.registry().get("shadow.client.connections").gauge().value());
                first.close();
                first.close(); // duplicate close must not decrement twice
                assertEquals(1, metrics.registry().get("shadow.client.connections").gauge().value());
                var command = PostgresCommand.of(CommandType.SELECT, "SELECT 'secret'", "private-session");
                second.writeInbound(command);
                assertSame(command, second.readInbound());
                assertEquals(1, metrics.registry().get("shadow.queries.received").tag("type", "SELECT").counter().count());
            } finally {
                first.finishAndReleaseAll();
                second.finishAndReleaseAll();
            }
            assertEquals(0, metrics.registry().get("shadow.client.connections").gauge().value());
            assertEquals(2, metrics.registry().get("shadow.client.accepted").counter().count());
        }
    }

    @Test
    void scrapeExposesJvmHistogramsAndCircuitWithoutSensitiveLabels() {
        try (var metrics = new ProxyMetrics()) {
            var breaker = new CircuitBreaker("primary", 1, 100, 0);
            metrics.observeCircuit(breaker);
            assertEquals(0, metrics.registry().get("shadow.circuit.state").gauge().value());
            breaker.recordFailure();
            assertEquals(1, metrics.registry().get("shadow.circuit.state").gauge().value());
            assertTrue(breaker.allowRequest());
            assertEquals(2, metrics.registry().get("shadow.circuit.state").gauge().value());
            metrics.recordBackend("primary", CommandType.SELECT, "success", 2_000_000);
            String scrape = metrics.registry().scrape();
            assertTrue(scrape.contains("jvm_memory_used_bytes"));
            assertTrue(scrape.contains("shadow_backend_query_seconds_bucket"));
            assertTrue(scrape.contains("shadow_queries_received_total"));
            assertTrue(scrape.contains("application=\"shadow-proxy\""));
            assertFalse(scrape.contains("sessionId="));
            assertFalse(scrape.contains("sql="));
        }
    }
}
