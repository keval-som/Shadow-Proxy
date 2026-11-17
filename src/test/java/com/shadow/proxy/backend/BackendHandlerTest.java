package com.shadow.proxy.backend;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.postgres.PostgresCommand;
import com.shadow.proxy.routing.*;
import com.shadow.proxy.telemetry.ProxyMetrics;
import com.shadow.proxy.telemetry.RabbitMQAuditProducer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class BackendHandlerTest {
    private final ProxyMetrics metrics = new ProxyMetrics();
    private final BackendNode primary = new BackendNode("primary", "localhost", 5433, BackendNode.NodeRole.PRIMARY);
    private final NodeRegistry nodes = new NodeRegistry(primary, List.of());
    private final BackendConnectionPool pool = mock(BackendConnectionPool.class);
    private final RabbitMQAuditProducer audit = mock(RabbitMQAuditProducer.class);
    private final EmbeddedChannel channel = new EmbeddedChannel(new BackendHandler(nodes,
            new ReadWriteRoutingStrategy(nodes), Map.of("primary", pool), audit, metrics, Runnable::run));
    private final PostgresCommand update = PostgresCommand.of(CommandType.UPDATE, "UPDATE users SET id=2", "session");

    @AfterEach
    void cleanup() {
        channel.finishAndReleaseAll();
        metrics.close();
    }

    @Test
    void successfulUpdateRecordsTimerAuditsAndReturnsConnection() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(pool.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(update.sql())).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        channel.writeInbound(update);
        assertResponseType('C');
        verify(connection).close();
        verify(statement).close();
        verify(audit).publish(argThat(e -> e.success() && e.targetNode().equals("primary")));
        assertTimer("success");
    }

    @Test
    void connectionFailureRecordsErrorAndClearsInflight() throws Exception {
        when(pool.getConnection()).thenThrow(new SQLException("backend offline"));
        channel.writeInbound(update);
        assertResponseType('E');
        verify(audit).publish(argThat(e -> !e.success() && e.errorMsg().equals("backend offline")));
        assertTimer("error");
    }

    @Test
    void openCircuitRejectsBeforeBorrowingConnection() {
        for (int i = 0; i < 20; i++) nodes.breakerFor(primary).recordFailure();
        channel.writeInbound(update);
        assertResponseType('E');
        verifyNoInteractions(pool);
        assertTimer("rejected");
    }

    private void assertTimer(String outcome) {
        assertEquals(1, metrics.registry().get("shadow.backend.query")
                .tags("node", "primary", "type", "UPDATE", "outcome", outcome).timer().count());
        assertEquals(0, metrics.registry().get("shadow.backend.inflight").gauge().value());
    }

    private void assertResponseType(char expected) {
        ByteBuf response = channel.readOutbound();
        assertNotNull(response);
        try { assertEquals(expected, response.readByte()); }
        finally { response.release(); }
    }
}
