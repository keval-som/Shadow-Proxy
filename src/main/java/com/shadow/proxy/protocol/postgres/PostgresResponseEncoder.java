package com.shadow.proxy.protocol.postgres;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PostgresResponseEncoder extends MessageToByteEncoder<Object> {

    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws Exception {
        if (msg instanceof PostgresCommand command) {
            encodeCommand(ctx, command, out);
        } else {
            throw new IllegalArgumentException("Unsupported message type: " + msg.getClass());
        }
    }

    private void encodeCommand(ChannelHandlerContext ctx, PostgresCommand command, ByteBuf out) {
        // ReadyForQuery: 'Z' + length(5) + 'I' (idle)
        out.writeByte('Z');
        out.writeInt(5);
        out.writeByte('I');
    }
}
