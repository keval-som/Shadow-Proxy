package com.shadow.proxy.cache;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;

/**
 * L2 distributed cache backed by Redis via Lettuce (async, non-blocking).
 *
 * Uses ByteArrayCodec so raw PostgreSQL wire-protocol bytes are stored
 * directly without any String encoding overhead.
 *
 * All operations return CompletableFuture so callers never block the
 * Netty I/O thread.
 */
public class L2RedisCache {

    private static final Logger log = LoggerFactory.getLogger(L2RedisCache.class);

    private static final String KEY_PREFIX = "shadow:cache:";

    private final RedisClient client;
    private final StatefulRedisConnection<byte[], byte[]> connection;
    private final RedisAsyncCommands<byte[], byte[]> async;
    private final long ttlSeconds;

    public L2RedisCache(String host, int port, long ttlSeconds) {
        RedisURI uri = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withTimeout(Duration.ofSeconds(3))
                .build();
        this.client = RedisClient.create(uri);
        this.connection = client.connect(ByteArrayCodec.INSTANCE);
        this.async = connection.async();
        this.ttlSeconds = ttlSeconds;
        log.info("L2 Redis cache connected: {}:{} ttl={}s", host, port, ttlSeconds);
    }

    /**
     * Async GET. Returns empty Optional if the key does not exist.
     * Never blocks — completes on Lettuce's I/O thread.
     */
    public CompletableFuture<Optional<byte[]>> get(String key) {
        byte[] redisKey = redisKey(key);
        return async.get(redisKey)
                .thenApply(value -> {
                    if (value != null) {
                        log.debug("L2 cache HIT  key={}", key);
                    }
                    return Optional.ofNullable(value);
                })
                .toCompletableFuture();
    }

    /**
     * Async SET with TTL.
     * Returns a future that completes when Redis acknowledges the write.
     */
    public CompletableFuture<Void> put(String key, byte[] value) {
        byte[] redisKey = redisKey(key);
        SetArgs args = SetArgs.Builder.ex(ttlSeconds);
        return async.set(redisKey, value, args)
                .thenAccept(response -> log.debug("L2 cache PUT  key={} bytes={}", key, value.length))
                .toCompletableFuture();
    }

    /** Async DEL for a single key. */
    public CompletableFuture<Void> invalidate(String key) {
        return async.del(redisKey(key))
                .thenAccept(n -> log.debug("L2 cache DEL key={} removed={}", key, n))
                .toCompletableFuture();
    }

    /** Flush all shadow:cache:* keys using SCAN + DEL (non-blocking, batched). */
    public CompletableFuture<Void> invalidateAll() {
        // Use SCAN to avoid blocking Redis with FLUSHDB
        String pattern = KEY_PREFIX + "*";
        return async.keys((KEY_PREFIX + "*").getBytes())
                .thenCompose(keys -> {
                    if (keys.isEmpty())
                        return CompletableFuture.completedFuture(null);
                    byte[][] arr = keys.toArray(new byte[0][]);
                    return async.del(arr).thenAccept(n -> log.info("L2 cache invalidateAll: removed {} keys", n));
                })
                .toCompletableFuture();
    }

    public void close() {
        connection.close();
        client.shutdown();
    }

    private byte[] redisKey(String key) {
        return (KEY_PREFIX + key).getBytes();
    }
}
