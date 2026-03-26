package com.shadow.proxy.routing;

import com.shadow.proxy.protocol.CommandType;
import com.shadow.proxy.protocol.DatabaseCommand;

public class ReadWriteRoutingStrategy implements RoutingStrategy {
    private final NodeRegistry nodeRegistry;

    public ReadWriteRoutingStrategy(NodeRegistry nodeRegistry) {
        this.nodeRegistry = nodeRegistry;
    }

    @Override
    public BackendNode route(DatabaseCommand command) {
        if (command.type() == CommandType.SELECT) {
            return nodeRegistry.getReplicaNode();
        }
        return nodeRegistry.getPrimaryNode();
    }
}
