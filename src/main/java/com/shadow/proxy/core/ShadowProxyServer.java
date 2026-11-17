package com.shadow.proxy.core;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shadow.proxy.backend.BackendConnectionPool;
import com.shadow.proxy.cache.L1CaffeineCache;
import com.shadow.proxy.cache.L2RedisCache;
import com.shadow.proxy.routing.NodeRegistry;
import com.shadow.proxy.routing.RoutingStrategy;
import com.shadow.proxy.state.SessionManager;
import com.shadow.proxy.telemetry.RabbitMQAuditProducer;
import com.shadow.proxy.telemetry.ProxyMetrics;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class ShadowProxyServer {

    private static final Logger log = LoggerFactory.getLogger(ShadowProxyServer.class);

    private final int                                port;
    private final SessionManager                     sessionManager;
    private final L1CaffeineCache                    l1Cache;
    private final L2RedisCache                       l2Cache;
    private final NodeRegistry                       nodeRegistry;
    private final RoutingStrategy                    router;
    private final Map<String, BackendConnectionPool> pools;
    private final RabbitMQAuditProducer              auditProducer;
    private final ProxyMetrics metrics;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public ShadowProxyServer(int port,
                              SessionManager sessionManager,
                              L1CaffeineCache l1Cache,
                              L2RedisCache l2Cache,
                              NodeRegistry nodeRegistry,
                              RoutingStrategy router,
                              Map<String, BackendConnectionPool> pools,
                              RabbitMQAuditProducer auditProducer, ProxyMetrics metrics) {
        this.port           = port;
        this.sessionManager = sessionManager;
        this.l1Cache        = l1Cache;
        this.l2Cache        = l2Cache;
        this.nodeRegistry   = nodeRegistry;
        this.router         = router;
        this.pools          = pools;
        this.auditProducer  = auditProducer;
        this.metrics = metrics;
    }

    public void start() throws Exception {
        Executor virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();

        bossGroup  = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(0, virtualThreadExecutor);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(new ShadowProxyInitializer(
                            sessionManager, l1Cache, l2Cache, nodeRegistry, router, pools, auditProducer, metrics));

            ChannelFuture future = bootstrap.bind(port).sync();
            log.info("Shadow Proxy listening on port {}", port);
            future.channel().closeFuture().sync();
        } finally {
            shutdown();
        }
    }

    public void shutdown() {
        log.info("Shutting down Shadow Proxy...");
        if (bossGroup  != null) bossGroup.shutdownGracefully();
        if (workerGroup != null) workerGroup.shutdownGracefully();
    }
}
