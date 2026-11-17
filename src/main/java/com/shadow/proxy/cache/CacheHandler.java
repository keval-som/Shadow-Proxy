package com.shadow.proxy.cache;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.DatabaseCommand;
import com.shadow.proxy.telemetry.ProxyMetrics;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Netty pipeline handler implementing a two-level read-through cache.
 *
 * Inbound (client → proxy):
 * 1. If command is not cacheable (write/DDL/CONTROL) → pass downstream,
 * and invalidate cache on the way back out for write commands.
 * 2. L1 hit → write cached bytes to client, stop pipeline.
 * 3. L2 hit → populate L1, write cached bytes to client, stop pipeline.
 * 4. Miss → pass downstream; cache the response on the outbound write.
 *
 * Outbound (proxy → client):
 * If a pending cache key exists (set during a cache miss), the response
 * bytes are captured and stored in both L1 and L2 before being forwarded
 * to the client.
 *
 * Not @Sharable — holds per-connection state (pendingCacheKey).
 */
public class CacheHandler extends ChannelDuplexHandler {

    private static final Logger log = LoggerFactory.getLogger(CacheHandler.class);

    private final L1CaffeineCache l1;
    private final L2RedisCache l2;
    private final ProxyMetrics metrics;

    // Set when a SELECT misses cache; cleared after the response is written
    private String pendingCacheKey = null;

    public CacheHandler(L1CaffeineCache l1, L2RedisCache l2, ProxyMetrics metrics) {
        this.l1 = l1;
        this.l2 = l2;
        this.metrics = metrics;
    }

    // ── Inbound: client sends a SQL command ──────────────────────────────────

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof DatabaseCommand command)) {
            super.channelRead(ctx, msg);
            return;
        }

        // Non-cacheable commands pass straight through
        if (!command.isCacheable()) {
            invalidateOnWrite(ctx, command);
            super.channelRead(ctx, msg);
            return;
        }

        String key = CacheKey.compute(command.sql());

        // ── L1 check ─────────────────────────────────────────────────────────
        Optional<byte[]> l1Hit = l1.get(key);
        if (l1Hit.isPresent()) {
            metrics.cacheLookup("l1", "hit");
            log.debug("Cache L1 HIT sql={}", abbreviated(command.sql()));
            writeBytes(ctx, l1Hit.get());
            return; // do NOT call super — pipeline stops here
        }

        // ── L2 check (async, must not block the I/O thread) ──────────────────
        metrics.cacheLookup("l1", "miss");
        l2.get(key).whenComplete((l2Hit, ex) -> {
            if (ex != null) {
                metrics.cacheLookup("l2", "error");
                log.warn("L2 cache GET error, passing through: {}", ex.getMessage());
                passThrough(ctx, msg, key);
                return;
            }
            if (l2Hit.isPresent()) {
                metrics.cacheLookup("l2", "hit");
                log.debug("Cache L2 HIT sql={}", abbreviated(command.sql()));
                byte[] bytes = l2Hit.get();
                l1.put(key, bytes); // warm L1 from L2
                ctx.executor().execute(() -> writeBytes(ctx, bytes));
            } else {
                metrics.cacheLookup("l2", "miss");
                log.debug("Cache MISS sql={}", abbreviated(command.sql()));
                passThrough(ctx, msg, key);
            }
        });
    }

    // ── Outbound: proxy writes DB response back to client ────────────────────

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
        if (pendingCacheKey != null && msg instanceof ByteBuf buf) {
            // Capture bytes for caching (retain a copy before it's released)
            byte[] bytes = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), bytes);

            String key = pendingCacheKey;
            pendingCacheKey = null;

            l1.put(key, bytes);
            l2.put(key, bytes).whenComplete((v, ex) -> {
                if (ex != null)
                    log.warn("L2 cache PUT error: {}", ex.getMessage());
            });
        }
        super.write(ctx, msg, promise);
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /** For writes/DDL, invalidate the affected cache entries. */
    private void invalidateOnWrite(ChannelHandlerContext ctx, DatabaseCommand command) {
        if (command.type() == CommandType.DDL) {
            l1.invalidateAll();
            l2.invalidateAll().whenComplete((v, ex) -> {
                if (ex != null)
                    log.warn("L2 invalidateAll error: {}", ex.getMessage());
            });
        }
        // INSERT/UPDATE/DELETE: fine-grained invalidation would require
        // table-name extraction (JSQLParser). Full flush is safe for now.
    }

    /** Records the pending key and forwards the command downstream. */
    private void passThrough(ChannelHandlerContext ctx, Object msg, String key) {
        pendingCacheKey = key;
        ctx.executor().execute(() -> {
            try {
                ctx.fireChannelRead(msg);
            } catch (Exception e) {
                ctx.fireExceptionCaught(e);
            }
        });
    }

    /** Writes raw bytes to the channel as an immediate response. */
    private void writeBytes(ChannelHandlerContext ctx, byte[] bytes) {
        ByteBuf buf = ctx.alloc().buffer(bytes.length);
        buf.writeBytes(bytes);
        ctx.writeAndFlush(buf);
    }

    private String abbreviated(String sql) {
        return sql.length() > 60 ? sql.substring(0, 60) + "…" : sql;
    }
}
