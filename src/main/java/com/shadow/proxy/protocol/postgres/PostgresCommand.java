package com.shadow.proxy.protocol.postgres;

// imports: CommandType, DatabaseCommand, Instant, List, Collections

import java.time.Instant;
import java.util.List;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.DatabaseCommand;

public record PostgresCommand(
        CommandType type,
        String sql,
        List<Object> parameters,
        String sessionId,
        boolean isCacheable,
        Instant receivedAt) implements DatabaseCommand {

    public static PostgresCommand of(CommandType type, String sql, String sessionId) {
        return new PostgresCommand(type, sql, List.of(), sessionId,
                type == CommandType.SELECT, Instant.now());
    }
}
