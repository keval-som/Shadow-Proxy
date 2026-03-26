package com.shadow.proxy.routing;

import java.util.List;

public class NodeRegistry {

    private final BackendNode primaryNode;
    private final List<BackendNode> replicaNodes;

    public NodeRegistry(BackendNode primaryNode, List<BackendNode> replicaNodes) {
        this.primaryNode  = primaryNode;
        this.replicaNodes = List.copyOf(replicaNodes);
    }

    public BackendNode getPrimaryNode() {
        return primaryNode;
    }

    public BackendNode getReplicaNode() {
        if (replicaNodes.isEmpty()) {
            return primaryNode;
        }
        return replicaNodes.get(0);
    }

    public List<BackendNode> getAllReplicaNodes() {
        return replicaNodes;
    }
}
