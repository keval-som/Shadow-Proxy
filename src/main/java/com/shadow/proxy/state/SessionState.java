package com.shadow.proxy.state;

/**
 * Immutable snapshot of a client session stored in Redis.
 *
 * Redis key:  session:{channelId}
 * Redis type: HASH  (field → value pairs)
 *
 * Fields:
 *   authState    → AUTHENTICATED | UNAUTHENTICATED
 *   inTx         → true | false  (open transaction in progress)
 *   txNode       → node ID that owns the open transaction (e.g. "primary")
 *   username     → PostgreSQL username from the startup packet
 *   database     → target database name from the startup packet
 *   createdAt    → epoch millis (for TTL refresh decisions)
 */
public record SessionState(
        String  sessionId,
        String  authState,
        boolean inTransaction,
        String  txNode,
        String  username,
        String  database,
        long    createdAt
) {
    public static final String AUTH_OK   = "AUTHENTICATED";
    public static final String AUTH_NONE = "UNAUTHENTICATED";

    /** Creates a fresh authenticated session for a new connection. */
    public static SessionState authenticated(String sessionId,
                                              String username,
                                              String database) {
        return new SessionState(
                sessionId, AUTH_OK, false, "", username, database,
                System.currentTimeMillis());
    }

    /** Returns a copy with an open transaction bound to the given node. */
    public SessionState withTransaction(String nodeId) {
        return new SessionState(sessionId, authState, true, nodeId,
                username, database, createdAt);
    }

    /** Returns a copy with the transaction cleared (after COMMIT/ROLLBACK). */
    public SessionState withoutTransaction() {
        return new SessionState(sessionId, authState, false, "",
                username, database, createdAt);
    }

    public boolean isAuthenticated() {
        return AUTH_OK.equals(authState);
    }
}
