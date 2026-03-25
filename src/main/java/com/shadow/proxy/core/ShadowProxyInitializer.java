package com.shadow.proxy.core;

import com.shadow.proxy.protocol.postgres.PostgresCommandDecoder;
import com.shadow.proxy.protocol.postgres.PostgresResponseEncoder;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class ShadowProxyInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("decoder", new PostgresCommandDecoder());
        pipeline.addLast("encoder", new PostgresResponseEncoder());
    }
}
