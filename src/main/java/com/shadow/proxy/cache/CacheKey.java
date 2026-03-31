package com.shadow.proxy.cache;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Produces a stable, normalised SHA-256 cache key from a SQL string.
 * Normalisation (trim + lower-case) ensures "SELECT 1" and " select 1 "
 * hit the same cache entry.
 */
public final class CacheKey {

    private CacheKey() {
    }

    public static String compute(String sql) {
        String normalised = sql.strip().toLowerCase();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(normalised.getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            // SHA-256 is guaranteed present in every JVM
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
