package com.shadow.proxy.protocol;

import java.time.Instant;
import java.util.List;

public interface DatabaseCommand {
    CommandType type();        // what kind of operation
    String sql();              // raw SQL string (null for non-SQL protocols)
    List<Object> parameters(); // bind parameters (for prepared statements)
    String sessionId();        // links this command to a Redis session
    boolean isCacheable();     // false for writes, transactions, volatile functions
    Instant receivedAt();      // used for latency telemetry
}
