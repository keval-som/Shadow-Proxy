package com.shadow.proxy.telemetry;

import com.shadow.proxy.protocol.DatabaseCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/** One instance per client. Counts queries before the cache can short-circuit them. */
public final class ConnectionMetricsHandler extends ChannelInboundHandlerAdapter {
    private final ProxyMetrics metrics;
    private boolean counted;

    public ConnectionMetricsHandler(ProxyMetrics metrics) { this.metrics = metrics; }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        if (!counted) {
            counted = true;
            metrics.clientConnected();
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (counted) {
            counted = false;
            metrics.clientDisconnected();
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof DatabaseCommand command) metrics.queryReceived(command.type());
        super.channelRead(ctx, msg);
    }
}
