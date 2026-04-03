package com.shadow.proxy;

import com.shadow.proxy.backend.BackendConnectionPool;
import com.shadow.proxy.cache.L1CaffeineCache;
import com.shadow.proxy.cache.L2RedisCache;
import com.shadow.proxy.core.ShadowProxyServer;
import com.shadow.proxy.routing.BackendNode;
import com.shadow.proxy.routing.NodeRegistry;
import com.shadow.proxy.routing.ReadWriteRoutingStrategy;
import com.shadow.proxy.state.SessionManager;
import com.shadow.proxy.telemetry.RabbitMQAuditProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Entry point — reads application.properties, wires all components,
 * registers a JVM shutdown hook, and starts the Netty server.
 *
 * No DI framework is used deliberately: explicit wiring makes the
 * dependency graph readable and keeps startup time under 1 second.
 */
public class ShadowProxyApplication {

    private static final Logger log = LoggerFactory.getLogger(ShadowProxyApplication.class);

    public static void main(String[] args) throws Exception {
        Properties cfg = loadConfig();

        // ── Backend nodes ─────────────────────────────────────────────────────
        BackendNode primary = new BackendNode(
                "primary",
                cfg.getProperty("db.primary.host", "localhost"),
                Integer.parseInt(cfg.getProperty("db.primary.port", "5433")),
                BackendNode.NodeRole.PRIMARY);

        BackendNode replica = new BackendNode(
                "replica-1",
                cfg.getProperty("db.replica.host", "localhost"),
                Integer.parseInt(cfg.getProperty("db.replica.port", "5434")),
                BackendNode.NodeRole.REPLICA);

        NodeRegistry nodeRegistry = new NodeRegistry(primary, List.of(replica));

        // ── Connection pools (one per node) ───────────────────────────────────
        String dbName  = cfg.getProperty("db.name",     "shadowdb");
        String dbUser  = cfg.getProperty("db.username", "shadowuser");
        String dbPass  = cfg.getProperty("db.password", "shadowpass");
        int    maxPool = Integer.parseInt(cfg.getProperty("pool.primary.max-size", "20"));

        BackendConnectionPool primaryPool = new BackendConnectionPool(
                primary, dbName, dbUser, dbPass, maxPool);
        BackendConnectionPool replicaPool = new BackendConnectionPool(
                replica, dbName, dbUser, dbPass,
                Integer.parseInt(cfg.getProperty("pool.replica.max-size", "20")));

        Map<String, BackendConnectionPool> pools = Map.of(
                primary.id(), primaryPool,
                replica.id(), replicaPool);

        // ── Routing ───────────────────────────────────────────────────────────
        ReadWriteRoutingStrategy router = new ReadWriteRoutingStrategy(nodeRegistry);

        // ── L1 Cache (Caffeine) ───────────────────────────────────────────────
        L1CaffeineCache l1Cache = new L1CaffeineCache(
                Integer.parseInt(cfg.getProperty("cache.l1.max-size", "10000")),
                Integer.parseInt(cfg.getProperty("cache.l1.expire-after-write-seconds", "60")));

        // ── L2 Cache (Redis) ──────────────────────────────────────────────────
        L2RedisCache l2Cache = new L2RedisCache(
                cfg.getProperty("redis.host", "localhost"),
                Integer.parseInt(cfg.getProperty("redis.port", "6379")),
                Long.parseLong(cfg.getProperty("redis.cache.ttl-seconds", "300")));

        // ── Session Manager (Redis) ───────────────────────────────────────────
        SessionManager sessionManager = new SessionManager(
                cfg.getProperty("redis.host", "localhost"),
                Integer.parseInt(cfg.getProperty("redis.port", "6379")),
                Long.parseLong(cfg.getProperty("redis.session.ttl-seconds", "1800")));

        // ── Telemetry (RabbitMQ) ──────────────────────────────────────────────
        RabbitMQAuditProducer auditProducer = new RabbitMQAuditProducer(
                cfg.getProperty("rabbitmq.host",       "localhost"),
                Integer.parseInt(cfg.getProperty("rabbitmq.port", "5672")),
                cfg.getProperty("rabbitmq.username",   "shadowmq"),
                cfg.getProperty("rabbitmq.password",   "shadowmqpass"),
                cfg.getProperty("rabbitmq.vhost",      "shadow"),
                cfg.getProperty("rabbitmq.exchange",   "shadow.audit"),
                cfg.getProperty("rabbitmq.routing-key","proxy.commands"));

        // ── Server ────────────────────────────────────────────────────────────
        int proxyPort = Integer.parseInt(cfg.getProperty("proxy.port", "5432"));

        ShadowProxyServer server = new ShadowProxyServer(
                proxyPort, sessionManager, l1Cache, l2Cache,
                nodeRegistry, router, pools, auditProducer);

        // Graceful shutdown on SIGTERM / Ctrl+C
        Runtime.getRuntime().addShutdownHook(Thread.ofVirtual().unstarted(() -> {
            log.info("Shutdown signal received");
            server.shutdown();
            auditProducer.close();
            l2Cache.close();
            sessionManager.close();
            primaryPool.close();
            replicaPool.close();
            log.info("Shadow Proxy stopped cleanly");
        }));

        log.info("Starting Shadow Proxy on port {}", proxyPort);
        server.start(); // blocks until channel closes
    }

    // ── Config loader ─────────────────────────────────────────────────────────

    private static Properties loadConfig() throws IOException {
        Properties props = new Properties();
        try (InputStream in = ShadowProxyApplication.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in != null) {
                props.load(in);
                // Resolve ${ENV_VAR:default} placeholders
                resolveEnvOverrides(props);
                log.info("Configuration loaded from application.properties");
            } else {
                log.warn("application.properties not found — using defaults and env vars");
            }
        }
        return props;
    }

    /**
     * For each property value matching ${ENV_VAR:default}, replaces it with
     * the environment variable value if set, otherwise with the default.
     */
    private static void resolveEnvOverrides(Properties props) {
        for (String key : props.stringPropertyNames()) {
            String value = props.getProperty(key);
            if (value != null && value.startsWith("${") && value.endsWith("}")) {
                String inner        = value.substring(2, value.length() - 1);
                int    colonIdx     = inner.indexOf(':');
                String envVar       = colonIdx >= 0 ? inner.substring(0, colonIdx) : inner;
                String defaultValue = colonIdx >= 0 ? inner.substring(colonIdx + 1) : "";
                String envValue     = System.getenv(envVar);
                props.setProperty(key, envValue != null ? envValue : defaultValue);
            }
        }
    }
}
