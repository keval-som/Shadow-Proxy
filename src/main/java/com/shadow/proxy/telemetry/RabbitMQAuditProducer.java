package com.shadow.proxy.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Publishes AuditEvent records to RabbitMQ asynchronously.
 *
 * Each publish is submitted to a virtual-thread executor so the caller
 * (BackendHandler on a Netty I/O thread) is never blocked by AMQP I/O.
 *
 * Exchange:    shadow.audit   (topic, durable)
 * Routing key: proxy.commands
 * Messages:    JSON, persistent (delivery-mode 2)
 *
 * Connection loss: the producer logs the error and drops the event rather
 * than blocking the proxy's critical path. A dead-letter queue or
 * reconnect strategy can be added for production use.
 */
public class RabbitMQAuditProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RabbitMQAuditProducer.class);

    private final String         exchange;
    private final String         routingKey;
    private final Connection     connection;
    private final Channel        channel;
    private final ObjectMapper   mapper;
    private final ExecutorService publisher;

    public RabbitMQAuditProducer(String host,
                                   int    port,
                                   String username,
                                   String password,
                                   String vhost,
                                   String exchange,
                                   String routingKey) throws Exception {
        this.exchange   = exchange;
        this.routingKey = routingKey;

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(vhost);
        factory.setConnectionTimeout(5_000);
        factory.setAutomaticRecoveryEnabled(true);   // reconnects on network blip
        factory.setNetworkRecoveryInterval(5_000);

        this.connection = factory.newConnection("shadow-proxy-audit");
        this.channel    = connection.createChannel();

        // Declare a durable topic exchange — survives broker restart
        this.channel.exchangeDeclare(exchange, "topic", true);

        // Jackson with Java 8 time module for Instant serialisation
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        this.publisher = Executors.newVirtualThreadPerTaskExecutor();

        log.info("RabbitMQ audit producer connected: {}:{} exchange={}", host, port, exchange);
    }

    /**
     * Non-blocking publish — submits to a virtual thread and returns immediately.
     * Safe to call from Netty I/O threads.
     */
    public void publish(AuditEvent event) {
        publisher.submit(() -> doPublish(event));
    }

    @Override
    public void close() {
        publisher.shutdown();
        try {
            if (channel.isOpen())    channel.close();
            if (connection.isOpen()) connection.close();
            log.info("RabbitMQ audit producer closed");
        } catch (Exception e) {
            log.warn("Error closing RabbitMQ producer: {}", e.getMessage());
        }
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void doPublish(AuditEvent event) {
        try {
            byte[] body = mapper.writeValueAsBytes(event);
            channel.basicPublish(
                    exchange,
                    routingKey,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, // delivery-mode 2
                    body);
            log.debug("Audit published eventId={} type={} latency={}ms",
                    event.eventId(), event.commandType(), event.latencyMs());
        } catch (IOException e) {
            // Drop the event — never propagate back to the proxy hot path
            log.warn("Audit publish failed, event dropped eventId={}: {}",
                    event.eventId(), e.getMessage());
        }
    }
}
