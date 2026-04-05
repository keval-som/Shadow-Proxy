package com.shadow.proxy.protocol.postgres;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class PostgresResponseEncoder extends MessageToByteEncoder<PostgresCommand> {

    @Override
    protected void encode(ChannelHandlerContext ctx, PostgresCommand msg, ByteBuf out) {
        // ReadyForQuery: 'Z' + length(5) + 'I' (idle)
        out.writeByte('Z');
        out.writeInt(5);
        out.writeByte('I');
    }
}
