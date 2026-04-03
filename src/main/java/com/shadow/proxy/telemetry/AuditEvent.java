package com.shadow.proxy.telemetry;

import com.shadow.proxy.protocol.CommandType;

import java.time.Instant;

/**
 * Immutable audit record published to RabbitMQ after every proxied command.
 *
 * Serialised as JSON:
 * {
 *   "eventId":    "a1b2c3...",
 *   "sessionId":  "channel-long-id",
 *   "commandType": "SELECT",
 *   "sql":        "SELECT * FROM users",
 *   "targetNode": "replica-1",
 *   "cacheHit":   true,
 *   "latencyMs":  4,
 *   "success":    true,
 *   "errorMsg":   null,
 *   "occurredAt": "2026-03-23T20:00:00Z"
 * }
 */
public record AuditEvent(
        String      eventId,
        String      sessionId,
        CommandType commandType,
        String      sql,
        String      targetNode,
        boolean     cacheHit,
        long        latencyMs,
        boolean     success,
        String      errorMsg,
        Instant     occurredAt
) {
    /** Factory for a successful command execution. */
    public static AuditEvent success(String sessionId,
                                      CommandType type,
                                      String sql,
                                      String targetNode,
                                      boolean cacheHit,
                                      long latencyMs) {
        return new AuditEvent(
                java.util.UUID.randomUUID().toString(),
                sessionId, type, sql, targetNode,
                cacheHit, latencyMs, true, null, Instant.now());
    }

    /** Factory for a failed command execution. */
    public static AuditEvent failure(String sessionId,
                                      CommandType type,
                                      String sql,
                                      String targetNode,
                                      long latencyMs,
                                      String errorMsg) {
        return new AuditEvent(
                java.util.UUID.randomUUID().toString(),
                sessionId, type, sql, targetNode,
                false, latencyMs, false, errorMsg, Instant.now());
    }
}
