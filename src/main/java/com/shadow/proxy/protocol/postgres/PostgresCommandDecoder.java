package com.shadow.proxy.protocol.postgres;

import java.nio.charset.StandardCharsets;
import java.util.List;

import com.shadow.proxy.protocol.CommandType;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class PostgresCommandDecoder extends ByteToMessageDecoder {

    private boolean startupComplete = false;

    private void handleStartup(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // read and discard the startup packet
        int length = in.readInt();
        in.skipBytes(length - 4);
        startupComplete = true;
        // AuthenticationOk + ReadyForQuery will be written by the encoder
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // Phase 1: startup (no type byte)
        if (!startupComplete) {
            if (in.readableBytes() < 4)
                return; // wait for more data
            // read length, read protocol version, send AuthenticationOk + ReadyForQuery
            handleStartup(ctx, in, out);
            return;
        }

        // Phase 2: normal messages
        if (in.readableBytes() < 5)
            return; // need type + length
        in.markReaderIndex();

        byte messageType = in.readByte();
        int length = in.readInt() - 4; // length includes itself

        if (in.readableBytes() < length) {
            in.resetReaderIndex(); // not enough data yet, wait
            return;
        }

        switch ((char) messageType) {
            case 'Q' -> handleSimpleQuery(in, length, out); // simple query
            case 'X' -> ctx.close(); // terminate
            default -> in.skipBytes(length); // ignore others for now
        }
    }

    private void handleSimpleQuery(ByteBuf in, int length, List<Object> out) {
        // The payload is a null-terminated string
        String sql = in.readCharSequence(length - 1, StandardCharsets.UTF_8).toString();
        in.skipBytes(1); // null terminator

        CommandType type = classify(sql);
        out.add(PostgresCommand.of(type, sql, "session-placeholder"));
    }

    private CommandType classify(String sql) {
        String upper = sql.stripLeading().toUpperCase();
        if (upper.startsWith("SELECT"))
            return CommandType.SELECT;
        if (upper.startsWith("INSERT"))
            return CommandType.INSERT;
        if (upper.startsWith("UPDATE"))
            return CommandType.UPDATE;
        if (upper.startsWith("DELETE"))
            return CommandType.DELETE;
        if (upper.startsWith("BEGIN") || upper.startsWith("COMMIT")
                || upper.startsWith("ROLLBACK"))
            return CommandType.CONTROL;
        return CommandType.DDL;
    }
}
