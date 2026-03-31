package com.shadow.proxy.cache;

import java.time.Duration;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * L1 in-process cache backed by Caffeine.
 *
 * Stores raw PostgreSQL wire-protocol response bytes so a cache hit
 * can be written directly to the client channel without re-encoding.
 *
 * Thread-safe: Caffeine's internal implementation uses striped locking.
 */
public class L1CaffeineCache {

    private static final Logger log = LoggerFactory.getLogger(L1CaffeineCache.class);

    private final Cache<String, byte[]> cache;

    public L1CaffeineCache(int maxSize, int expireAfterWriteSeconds) {
        this.cache = Caffeine.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(Duration.ofSeconds(expireAfterWriteSeconds))
                .recordStats() // enables hit-rate metrics via cache.stats()
                .build();
        log.info("L1 Caffeine cache initialised: maxSize={}, ttl={}s",
                maxSize, expireAfterWriteSeconds);
    }

    /** Returns the cached response bytes, or empty if not present. */
    public Optional<byte[]> get(String key) {
        byte[] value = cache.getIfPresent(key);
        if (value != null) {
            log.debug("L1 cache HIT  key={}", key);
        }
        return Optional.ofNullable(value);
    }

    /** Stores response bytes under the given key. */
    public void put(String key, byte[] value) {
        cache.put(key, value);
        log.debug("L1 cache PUT  key={} bytes={}", key, value.length);
    }

    /** Evicts a single entry (e.g. after a write invalidates it). */
    public void invalidate(String key) {
        cache.invalidate(key);
    }

    /** Evicts all entries — called on DDL or full-table writes. */
    public void invalidateAll() {
        cache.invalidateAll();
        log.info("L1 cache fully invalidated");
    }

    /** Exposes Caffeine stats (hit rate, eviction count, etc.) for monitoring. */
    public com.github.benmanes.caffeine.cache.stats.CacheStats stats() {
        return cache.stats();
    }
}
