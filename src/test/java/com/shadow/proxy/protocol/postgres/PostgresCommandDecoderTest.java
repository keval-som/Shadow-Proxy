package com.shadow.proxy.protocol.postgres;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.state.SessionManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PostgresCommandDecoderTest {
    private final SessionManager sessions = mock(SessionManager.class);
    private final EmbeddedChannel channel = new EmbeddedChannel(new PostgresCommandDecoder(sessions));

    @AfterEach
    void cleanup() { channel.finishAndReleaseAll(); }

    @Test
    void sslNegotiationDeclinesTlsWithoutCreatingSession() {
        channel.writeInbound(Unpooled.buffer().writeInt(8).writeInt(80877103));
        ByteBuf response = channel.readOutbound();
        try {
            assertEquals('N', response.readByte());
            assertFalse(response.isReadable());
        } finally { response.release(); }
        verifyNoInteractions(sessions);
    }

    @Test
    void startupAcknowledgesAndDisconnectDeletesSession() {
        startup();
        verify(sessions).save(argThat(s -> s.isAuthenticated()));
        channel.close();
        verify(sessions).delete(channel.id().asLongText());
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "SELECT * FROM users|SELECT",
            "INSERT INTO users (id) VALUES (1)|INSERT",
            "UPDATE users SET id = 2|UPDATE",
            "DELETE FROM users WHERE id = 1|DELETE",
            "CREATE TABLE demo (id INT)|DDL",
            "INVALID JUNK 99999|CONTROL"
    })
    void classifiesSupportedStatements(String sql, CommandType expected) {
        startup();
        assertTrue(channel.writeInbound(query(sql)));
        PostgresCommand command = channel.readInbound();
        assertEquals(expected, command.type());
        assertEquals(sql, command.sql());
        assertEquals(channel.id().asLongText(), command.sessionId());
    }

    @Test
    void waitsForCompleteSimpleQueryPayload() {
        startup();
        ByteBuf full = query("SELECT 12345");
        try {
            assertFalse(channel.writeInbound(full.readRetainedSlice(7)));
            assertNull(channel.readInbound());
            assertTrue(channel.writeInbound(full.readRetainedSlice(full.readableBytes())));
            PostgresCommand command = channel.readInbound();
            assertEquals("SELECT 12345", command.sql());
        } finally { full.release(); }
    }

    @Test
    void decodesTwoQueriesInOneTcpRead() {
        startup();
        ByteBuf first = query("SELECT 1");
        ByteBuf second = query("SELECT 2");
        try {
            first.writeBytes(second);
            channel.writeInbound(first.retain());
            assertEquals("SELECT 1", ((PostgresCommand) channel.readInbound()).sql());
            assertEquals("SELECT 2", ((PostgresCommand) channel.readInbound()).sql());
            assertNull(channel.readInbound());
        } finally {
            first.release();
            second.release();
        }
    }

    private void startup() {
        when(sessions.save(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(sessions.delete(anyString())).thenReturn(CompletableFuture.completedFuture(null));
        // Protocol 3.0, followed by the parameter-list terminator.
        channel.writeInbound(Unpooled.buffer().writeInt(9).writeInt(196608).writeByte(0));
        ByteBuf response = channel.readOutbound();
        try {
            assertEquals('R', response.readByte());
            assertEquals(8, response.readInt());
            assertEquals(0, response.readInt());
            assertEquals('Z', response.readByte());
            assertEquals(5, response.readInt());
            assertEquals('I', response.readByte());
            assertFalse(response.isReadable());
        } finally { response.release(); }
    }

    private ByteBuf query(String sql) {
        byte[] bytes = sql.getBytes(StandardCharsets.UTF_8);
        return Unpooled.buffer().writeByte('Q').writeInt(bytes.length + 5).writeBytes(bytes).writeByte(0);
    }
}
