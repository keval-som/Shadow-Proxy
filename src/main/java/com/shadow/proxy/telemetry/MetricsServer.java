package com.shadow.proxy.telemetry;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/** Separate HTTP listener: PostgreSQL clients never encounter HTTP traffic. */
public final class MetricsServer implements AutoCloseable {
    private final HttpServer server;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExecutorService executor = Executors.newFixedThreadPool(2, Thread.ofPlatform()
            .daemon(true).name("metrics-http-", 0).factory());

    public MetricsServer(String host, int port, ProxyMetrics metrics) throws IOException {
        server = HttpServer.create(new InetSocketAddress(host, port), 16);
        server.setExecutor(executor);
        server.createContext("/metrics", exchange -> {
            try (exchange) {
                if (!exchange.getRequestURI().getPath().equals("/metrics")) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                if (!exchange.getRequestMethod().equals("GET")) {
                    exchange.getResponseHeaders().set("Allow", "GET");
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] body = metrics.registry().scrape().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
        });
    }

    public void start() { server.start(); }
    public int port() { return server.getAddress().getPort(); }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
