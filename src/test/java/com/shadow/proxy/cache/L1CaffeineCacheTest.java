package com.shadow.proxy.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class L1CaffeineCacheTest {
    @Test
    void tracksHitsAndMissesAndPreservesBytes() {
        var cache = new L1CaffeineCache(10, 60);
        assertTrue(cache.get("missing").isEmpty());
        byte[] bytes = {0, 1, -1, 42};
        cache.put("key", bytes);
        assertArrayEquals(bytes, cache.get("key").orElseThrow());
        assertEquals(1, cache.stats().hitCount());
        assertEquals(1, cache.stats().missCount());
    }

    @Test
    void canInvalidateOneEntryWithoutRemovingOthers() {
        var cache = new L1CaffeineCache(10, 60);
        cache.put("a", new byte[]{1});
        cache.put("b", new byte[]{2});
        cache.invalidate("a");
        assertTrue(cache.get("a").isEmpty());
        assertTrue(cache.get("b").isPresent());
        cache.invalidateAll();
        assertTrue(cache.get("b").isEmpty());
    }

    @Test
    void zeroTtlDoesNotServeExpiredData() {
        var cache = new L1CaffeineCache(10, 0);
        cache.put("key", new byte[]{1});
        assertTrue(cache.get("key").isEmpty());
    }
}
