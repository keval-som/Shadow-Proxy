package com.shadow.proxy.protocol;

public enum CommandType {
    SELECT,   // routed to replica
    INSERT,   // routed to primary
    UPDATE,   // routed to primary
    DELETE,   // routed to primary
    DDL,      // routed to primary (CREATE, ALTER, DROP, TRUNCATE)
    CONTROL   // not routed to DB (BEGIN, COMMIT, ROLLBACK, SET, SHOW)
}
