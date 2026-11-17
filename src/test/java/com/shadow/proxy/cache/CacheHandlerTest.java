package com.shadow.proxy.cache;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.postgres.PostgresCommand;
import com.shadow.proxy.telemetry.ConnectionMetricsHandler;
import com.shadow.proxy.telemetry.ProxyMetrics;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.*;

class CacheHandlerTest {
    private final ProxyMetrics metrics = new ProxyMetrics();
    private final L1CaffeineCache l1 = new L1CaffeineCache(10, 60);
    private final L2RedisCache l2 = mock(L2RedisCache.class);
    private final EmbeddedChannel channel = new EmbeddedChannel(
            new ConnectionMetricsHandler(metrics), new CacheHandler(l1, l2, metrics));
    private final PostgresCommand query = PostgresCommand.of(CommandType.SELECT, "SELECT 42", "session");

    @AfterEach
    void cleanup() {
        channel.finishAndReleaseAll();
        metrics.close();
    }

    @Test
    void l1HitReturnsBytesWithoutRedisOrBackendAndCountsQuery() {
        l1.put(CacheKey.compute(query.sql()), new byte[]{42});
        assertFalse(channel.writeInbound(query));
        assertResponse(new byte[]{42});
        verifyNoInteractions(l2);
        assertEquals(1, counter("l1", "hit"));
        assertEquals(1, metrics.registry().get("shadow.queries.received").tag("type", "SELECT").counter().count());
    }

    @Test
    void l2HitWarmsL1AndStopsBackendExecution() {
        when(l2.get(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.of(new byte[]{7})));
        channel.writeInbound(query);
        channel.runPendingTasks();
        assertResponse(new byte[]{7});
        assertNull(channel.readInbound());
        assertArrayEquals(new byte[]{7}, l1.get(CacheKey.compute(query.sql())).orElseThrow());
        assertEquals(1, counter("l1", "miss"));
        assertEquals(1, counter("l2", "hit"));
    }

    @Test
    void missPassesQueryAndCachesOutboundResponse() {
        when(l2.get(anyString())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
        when(l2.put(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        channel.writeInbound(query);
        channel.runPendingTasks();
        assertSame(query, channel.readInbound());
        channel.writeOutbound(Unpooled.wrappedBuffer(new byte[]{'C', 0, 0, 0, 4}));
        assertResponse(new byte[]{'C', 0, 0, 0, 4});
        assertTrue(l1.get(CacheKey.compute(query.sql())).isPresent());
        verify(l2).put(eq(CacheKey.compute(query.sql())), aryEq(new byte[]{'C', 0, 0, 0, 4}));
        assertEquals(1, counter("l2", "miss"));
    }

    @Test
    void redisFailurePassesThroughAndIsCountedAsError() {
        when(l2.get(anyString())).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("offline")));
        channel.writeInbound(query);
        channel.runPendingTasks();
        assertSame(query, channel.readInbound());
        assertEquals(1, counter("l2", "error"));
    }

    @Test
    void delayedRedisReplyDoesNotLoseQuery() {
        var lookup = new CompletableFuture<Optional<byte[]>>();
        when(l2.get(anyString())).thenReturn(lookup);
        assertFalse(channel.writeInbound(query));
        assertNull(channel.readInbound());
        lookup.complete(Optional.empty());
        channel.runPendingTasks();
        assertSame(query, channel.readInbound());
    }

    @Test
    void ddlInvalidatesBothLayersAndContinuesDownstream() {
        when(l2.invalidateAll()).thenReturn(CompletableFuture.completedFuture(null));
        l1.put("key", new byte[]{1});
        var ddl = PostgresCommand.of(CommandType.DDL, "ALTER TABLE users ADD COLUMN x INT", "session");
        assertTrue(channel.writeInbound(ddl));
        assertSame(ddl, channel.readInbound());
        assertTrue(l1.get("key").isEmpty());
        verify(l2).invalidateAll();
    }

    @Test
    void unknownControlCommandDoesNotFlushCache() {
        l1.put("key", new byte[]{1});
        var control = PostgresCommand.of(CommandType.CONTROL, "INVALID JUNK", "session");
        assertTrue(channel.writeInbound(control));
        assertSame(control, channel.readInbound());
        assertTrue(l1.get("key").isPresent());
        verifyNoInteractions(l2);
    }

    private double counter(String layer, String result) {
        return metrics.registry().get("shadow.cache.lookups").tags("layer", layer, "result", result).counter().count();
    }

    private void assertResponse(byte[] expected) {
        ByteBuf bytes = channel.readOutbound();
        assertNotNull(bytes);
        try {
            byte[] actual = new byte[bytes.readableBytes()];
            bytes.readBytes(actual);
            assertArrayEquals(expected, actual);
        } finally { bytes.release(); }
    }
}
