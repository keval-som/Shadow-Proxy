package com.shadow.proxy.routing;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.postgres.PostgresCommand;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ReadWriteRoutingStrategyTest {
    private final BackendNode primary = new BackendNode("primary", "localhost", 5433, BackendNode.NodeRole.PRIMARY);
    private final BackendNode replica = new BackendNode("replica", "localhost", 5434, BackendNode.NodeRole.REPLICA);

    @ParameterizedTest
    @EnumSource(CommandType.class)
    void routesReadsToReplicaAndOtherCommandsToPrimary(CommandType type) {
        var router = new ReadWriteRoutingStrategy(new NodeRegistry(primary, List.of(replica)));
        assertEquals(type == CommandType.SELECT ? replica : primary,
                router.route(PostgresCommand.of(type, "statement", "session")));
    }

    @Test
    void fallsBackToPrimaryWithoutReplicas() {
        assertEquals(primary, new NodeRegistry(primary, List.of()).getReplicaNode());
    }

    @Test
    void skipsOpenReplicaAndUsesNextHealthyReplica() {
        var second = new BackendNode("replica-2", "localhost", 5435, BackendNode.NodeRole.REPLICA);
        var registry = new NodeRegistry(primary, List.of(replica, second));
        for (int i = 0; i < 20; i++) registry.breakerFor(replica).recordFailure();
        assertEquals(second, registry.getReplicaNode());
    }

    @Test
    void fallsBackToPrimaryWhenAllReplicasAreOpen() {
        var registry = new NodeRegistry(primary, List.of(replica));
        for (int i = 0; i < 20; i++) registry.breakerFor(replica).recordFailure();
        assertEquals(primary, registry.getReplicaNode());
    }
}
