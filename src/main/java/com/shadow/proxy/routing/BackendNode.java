package com.shadow.proxy.routing;

public record BackendNode(String id, String host, int port, NodeRole role) {
    public enum NodeRole {
        PRIMARY,
        REPLICA
    }
}
