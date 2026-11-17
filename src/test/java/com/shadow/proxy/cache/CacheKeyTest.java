package com.shadow.proxy.cache;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CacheKeyTest {
    @Test
    void keyIsStableAndIgnoresSurroundingWhitespace() {
        assertEquals(CacheKey.compute("SELECT 1"), CacheKey.compute("  SELECT 1\n"));
        assertTrue(CacheKey.compute("SELECT 1").matches("[0-9a-f]{64}"));
    }

    @Test
    void differentPredicatesHaveDifferentKeys() {
        assertNotEquals(CacheKey.compute("SELECT * FROM users WHERE id = 1"),
                CacheKey.compute("SELECT * FROM users WHERE id = 2"));
    }
}
