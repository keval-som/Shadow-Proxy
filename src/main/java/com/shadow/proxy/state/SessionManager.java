package com.shadow.proxy.state;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Manages client session state in Redis using HASH commands.
 *
 * Each session is stored as:
 *   HSET session:{id}  authState AUTHENTICATED  inTx false  txNode ""  ...
 *   EXPIRE session:{id}  1800
 *
 * All operations are async (Lettuce RedisFuture → CompletableFuture).
 * The Netty I/O thread is never blocked.
 */
public class SessionManager {

    private static final Logger log = LoggerFactory.getLogger(SessionManager.class);

    private static final String PREFIX = "session:";

    // Redis hash field names
    private static final String F_AUTH     = "authState";
    private static final String F_IN_TX    = "inTx";
    private static final String F_TX_NODE  = "txNode";
    private static final String F_USERNAME = "username";
    private static final String F_DATABASE = "database";
    private static final String F_CREATED  = "createdAt";

    private final RedisClient                        client;
    private final StatefulRedisConnection<String, String> connection;
    private final RedisAsyncCommands<String, String> async;
    private final long                               ttlSeconds;

    public SessionManager(String host, int port, long ttlSeconds) {
        RedisURI uri = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withTimeout(Duration.ofSeconds(3))
                .build();
        this.client     = RedisClient.create(uri);
        this.connection = client.connect();
        this.async      = connection.async();
        this.ttlSeconds = ttlSeconds;
        log.info("SessionManager connected to Redis {}:{}", host, port);
    }

    // ── Write ─────────────────────────────────────────────────────────────────

    /**
     * Persists a new or updated session and refreshes its TTL.
     * Uses HSET (sets multiple fields atomically) + EXPIRE.
     */
    public CompletableFuture<Void> save(SessionState session) {
        String key = key(session.sessionId());
        Map<String, String> fields = Map.of(
                F_AUTH,     session.authState(),
                F_IN_TX,    String.valueOf(session.inTransaction()),
                F_TX_NODE,  session.txNode(),
                F_USERNAME, session.username(),
                F_DATABASE, session.database(),
                F_CREATED,  String.valueOf(session.createdAt())
        );
        return async.hset(key, fields)
                .thenCompose(n -> async.expire(key, ttlSeconds))
                .thenAccept(ok -> log.debug("Session saved id={}", session.sessionId()))
                .toCompletableFuture();
    }

    // ── Read ──────────────────────────────────────────────────────────────────

    /**
     * Loads a session and refreshes TTL on access.
     * Returns an empty CompletableFuture if the session key does not exist.
     */
    public CompletableFuture<SessionState> load(String sessionId) {
        String key = key(sessionId);
        return async.hgetall(key)
                .thenCompose(fields -> {
                    if (fields == null || fields.isEmpty()) {
                        log.debug("Session not found id={}", sessionId);
                        return CompletableFuture.completedFuture(null);
                    }
                    // Refresh TTL on every access (sliding expiry)
                    async.expire(key, ttlSeconds);
                    return CompletableFuture.completedFuture(toSession(sessionId, fields));
                })
                .toCompletableFuture();
    }

    // ── Partial updates ───────────────────────────────────────────────────────

    /** Marks a transaction as open on the given node. */
    public CompletableFuture<Void> openTransaction(String sessionId, String nodeId) {
        String key = key(sessionId);
        return async.hset(key, Map.of(F_IN_TX, "true", F_TX_NODE, nodeId))
                .thenCompose(n -> async.expire(key, ttlSeconds))
                .thenAccept(ok -> log.debug("Session tx opened id={} node={}", sessionId, nodeId))
                .toCompletableFuture();
    }

    /** Clears the open transaction (after COMMIT or ROLLBACK). */
    public CompletableFuture<Void> closeTransaction(String sessionId) {
        String key = key(sessionId);
        return async.hset(key, Map.of(F_IN_TX, "false", F_TX_NODE, ""))
                .thenCompose(n -> async.expire(key, ttlSeconds))
                .thenAccept(ok -> log.debug("Session tx closed id={}", sessionId))
                .toCompletableFuture();
    }

    // ── Delete ────────────────────────────────────────────────────────────────

    /** Removes the session when the TCP channel closes. */
    public CompletableFuture<Void> delete(String sessionId) {
        return async.del(key(sessionId))
                .thenAccept(n -> log.debug("Session deleted id={}", sessionId))
                .toCompletableFuture();
    }

    public void close() {
        connection.close();
        client.shutdown();
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private String key(String sessionId) {
        return PREFIX + sessionId;
    }

    private SessionState toSession(String sessionId, Map<String, String> fields) {
        return new SessionState(
                sessionId,
                fields.getOrDefault(F_AUTH,     SessionState.AUTH_NONE),
                Boolean.parseBoolean(fields.getOrDefault(F_IN_TX, "false")),
                fields.getOrDefault(F_TX_NODE,  ""),
                fields.getOrDefault(F_USERNAME, ""),
                fields.getOrDefault(F_DATABASE, ""),
                Long.parseLong(fields.getOrDefault(F_CREATED, "0"))
        );
    }
}
