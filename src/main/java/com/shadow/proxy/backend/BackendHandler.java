package com.shadow.proxy.backend;

import com.shadow.proxy.protocol.DatabaseCommand;
import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.resilience.CircuitBreaker;
import com.shadow.proxy.routing.BackendNode;
import com.shadow.proxy.routing.NodeRegistry;
import com.shadow.proxy.routing.RoutingStrategy;
import com.shadow.proxy.telemetry.AuditEvent;
import com.shadow.proxy.telemetry.RabbitMQAuditProducer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Terminal inbound handler — executes SQL against the real PostgreSQL instance.
 *
 * Execution model:
 *   - JDBC is blocking; we submit each query to a virtual-thread executor
 *     so the Netty I/O thread is never parked waiting on a DB response.
 *   - After the JDBC call completes the response is written back via
 *     ctx.writeAndFlush(), which is safe to call from any thread.
 *
 * Circuit breaker integration:
 *   - allowRequest() is checked before every DB call.
 *   - recordSuccess() / recordFailure() are called depending on outcome.
 */
public class BackendHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(BackendHandler.class);

    private static final Executor JDBC_EXECUTOR =
            Executors.newVirtualThreadPerTaskExecutor();

    private final NodeRegistry                       nodeRegistry;
    private final RoutingStrategy                    router;
    private final Map<String, BackendConnectionPool> pools;
    private final RabbitMQAuditProducer              auditProducer;

    public BackendHandler(NodeRegistry nodeRegistry,
                          RoutingStrategy router,
                          Map<String, BackendConnectionPool> pools,
                          RabbitMQAuditProducer auditProducer) {
        this.nodeRegistry  = nodeRegistry;
        this.router        = router;
        this.pools         = pools;
        this.auditProducer = auditProducer;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof DatabaseCommand command)) {
            ctx.fireChannelRead(msg);
            return;
        }

        // Submit blocking JDBC work to a virtual thread — never block I/O loop
        JDBC_EXECUTOR.execute(() -> executeCommand(ctx, command));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("BackendHandler unhandled exception", cause);
        ByteBuf err = PostgresWireEncoder.encodeError(
                ctx.alloc(), "FATAL", "08006", cause.getMessage());
        ctx.writeAndFlush(err).addListener(f -> ctx.close());
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private void executeCommand(ChannelHandlerContext ctx, DatabaseCommand command) {
        BackendNode     target  = router.route(command);
        CircuitBreaker  breaker = nodeRegistry.breakerFor(target);

        // Circuit open → fail fast
        if (!breaker.allowRequest()) {
            log.warn("Circuit OPEN for node={}, rejecting command", target.id());
            ByteBuf err = PostgresWireEncoder.encodeError(
                    ctx.alloc(), "ERROR", "53300",
                    "Backend node " + target.id() + " is unavailable (circuit open)");
            ctx.writeAndFlush(err);
            return;
        }

        BackendConnectionPool pool = pools.get(target.id());
        if (pool == null) {
            writeError(ctx, "08001", "No connection pool for node: " + target.id());
            return;
        }

        long start = System.currentTimeMillis();
        try (Connection conn = pool.getConnection()) {
            ByteBuf response = dispatch(ctx, conn, command);
            long latency = System.currentTimeMillis() - start;
            breaker.recordSuccess();
            ctx.writeAndFlush(response);
            auditProducer.publish(AuditEvent.success(
                    command.sessionId(), command.type(),
                    command.sql(), target.id(), false, latency));
            log.debug("Query executed on node={} latency={}ms sql={}",
                    target.id(), latency, abbreviated(command.sql()));
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - start;
            breaker.recordFailure();
            log.error("JDBC error on node={}: {}", target.id(), e.getMessage());
            auditProducer.publish(AuditEvent.failure(
                    command.sessionId(), command.type(),
                    command.sql(), target.id(), latency, e.getMessage()));
            writeError(ctx, "08006", e.getMessage());
        }
    }

    /** Routes to the correct JDBC execution based on command type. */
    private ByteBuf dispatch(ChannelHandlerContext ctx,
                              Connection conn,
                              DatabaseCommand command) throws Exception {
        return switch (command.type()) {
            case SELECT  -> executeQuery(ctx, conn, command.sql());
            case INSERT,
                 UPDATE,
                 DELETE   -> executeUpdate(ctx, conn, command.sql(), command.type());
            case DDL,
                 CONTROL  -> executeDdl(ctx, conn, command.sql());
        };
    }

    private ByteBuf executeQuery(ChannelHandlerContext ctx,
                                  Connection conn,
                                  String sql) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            return PostgresWireEncoder.encodeResultSet(ctx.alloc(), rs);
        }
    }

    private ByteBuf executeUpdate(ChannelHandlerContext ctx,
                                   Connection conn,
                                   String sql,
                                   CommandType type) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int affected = ps.executeUpdate();
            return PostgresWireEncoder.encodeUpdateResult(
                    ctx.alloc(), type.name(), affected);
        }
    }

    private ByteBuf executeDdl(ChannelHandlerContext ctx,
                                Connection conn,
                                String sql) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.execute();
            return PostgresWireEncoder.encodeUpdateResult(ctx.alloc(), "DDL", 0);
        }
    }

    private void writeError(ChannelHandlerContext ctx, String sqlState, String msg) {
        ByteBuf err = PostgresWireEncoder.encodeError(ctx.alloc(), "ERROR", sqlState, msg);
        ctx.writeAndFlush(err);
    }

    private String abbreviated(String sql) {
        return sql.length() > 80 ? sql.substring(0, 80) + "…" : sql;
    }
}
