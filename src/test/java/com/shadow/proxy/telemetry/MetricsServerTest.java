package com.shadow.proxy.telemetry;

import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class MetricsServerTest {
    @Test
    void servesPrometheusOnSeparatePortAndRejectsInvalidRequests() throws Exception {
        try (var metrics = new ProxyMetrics();
             var server = new MetricsServer("127.0.0.1", 0, metrics);
             var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            server.start();
            URI uri = URI.create("http://127.0.0.1:" + server.port() + "/metrics");
            var response = client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3)).build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.headers().firstValue("Content-Type").orElseThrow().startsWith("text/plain"));
            assertTrue(response.body().contains("shadow_client_connections"));
            assertTrue(response.body().contains("jvm_memory_used_bytes"));
            var post = client.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3))
                    .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.discarding());
            assertEquals(405, post.statusCode());
            assertEquals("GET", post.headers().firstValue("Allow").orElseThrow());
            var missing = client.send(HttpRequest.newBuilder(URI.create(uri + "/missing"))
                    .timeout(Duration.ofSeconds(3)).build(), HttpResponse.BodyHandlers.discarding());
            assertEquals(404, missing.statusCode());
        }
    }
}
