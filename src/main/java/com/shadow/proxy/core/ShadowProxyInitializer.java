package com.shadow.proxy.core;

import com.shadow.proxy.backend.BackendConnectionPool;
import com.shadow.proxy.backend.BackendHandler;
import com.shadow.proxy.cache.CacheHandler;
import com.shadow.proxy.cache.L1CaffeineCache;
import com.shadow.proxy.cache.L2RedisCache;
import com.shadow.proxy.protocol.postgres.PostgresCommandDecoder;
import com.shadow.proxy.protocol.postgres.PostgresResponseEncoder;
import com.shadow.proxy.routing.NodeRegistry;
import com.shadow.proxy.routing.RoutingStrategy;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

import java.util.Map;

public class ShadowProxyInitializer extends ChannelInitializer<SocketChannel> {

    private final L1CaffeineCache                    l1Cache;
    private final L2RedisCache                       l2Cache;
    private final NodeRegistry                       nodeRegistry;
    private final RoutingStrategy                    router;
    private final Map<String, BackendConnectionPool> pools;

    public ShadowProxyInitializer(L1CaffeineCache l1Cache,
                                   L2RedisCache l2Cache,
                                   NodeRegistry nodeRegistry,
                                   RoutingStrategy router,
                                   Map<String, BackendConnectionPool> pools) {
        this.l1Cache      = l1Cache;
        this.l2Cache      = l2Cache;
        this.nodeRegistry = nodeRegistry;
        this.router       = router;
        this.pools        = pools;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        ChannelPipeline pipeline = ch.pipeline();

        // Inbound order: decoder → cache → backend
        // Outbound order: backend → cache → encoder (reverse)
        pipeline.addLast("decoder", new PostgresCommandDecoder());
        pipeline.addLast("encoder", new PostgresResponseEncoder());
        pipeline.addLast("cache",   new CacheHandler(l1Cache, l2Cache));
        pipeline.addLast("backend", new BackendHandler(nodeRegistry, router, pools));
    }
}
