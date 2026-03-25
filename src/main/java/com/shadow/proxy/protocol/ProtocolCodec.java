package com.shadow.proxy.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public interface ProtocolCodec<C extends DatabaseCommand> {

    /**
     * Decode raw bytes from the wire into a DatabaseCommand.
     * Called by the Netty inbound decoder on each complete frame.
     */
    C decode(ByteBuf in) throws ProtocolException;

    /**
     * Encode a result (rows, error, ok) back into wire-format bytes.
     * Called by the Netty outbound encoder before writing to the client channel.
     */
    ByteBuf encode(Object result, ByteBufAllocator allocator);

    /**
     * Protocol identifier — used for logging and future protocol negotiation.
     */
    String protocolName(); // e.g. "postgresql-v3", "mongodb-wire", "redis-resp"
}
