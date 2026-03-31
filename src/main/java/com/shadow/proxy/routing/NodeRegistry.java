package com.shadow.proxy.routing;

import com.shadow.proxy.resilience.CircuitBreaker;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NodeRegistry {

    private static final int  CB_WINDOW_SIZE      = 20;
    private static final int  CB_FAILURE_THRESHOLD = 50; // 50%
    private static final long CB_OPEN_DURATION_MS  = 30_000L;

    private final BackendNode        primaryNode;
    private final List<BackendNode>  replicaNodes;
    private final Map<String, CircuitBreaker> breakers;

    public NodeRegistry(BackendNode primaryNode, List<BackendNode> replicaNodes) {
        this.primaryNode  = primaryNode;
        this.replicaNodes = List.copyOf(replicaNodes);
        this.breakers     = buildBreakers(primaryNode, replicaNodes);
    }

    public BackendNode getPrimaryNode() {
        return primaryNode;
    }

    public BackendNode getReplicaNode() {
        // Try each replica in order; fall back to primary if all are OPEN
        for (BackendNode replica : replicaNodes) {
            if (breakerFor(replica).allowRequest()) {
                return replica;
            }
        }
        return primaryNode;
    }

    public List<BackendNode> getAllReplicaNodes() {
        return replicaNodes;
    }

    /** Returns the circuit breaker for the given node. */
    public CircuitBreaker breakerFor(BackendNode node) {
        return breakers.get(node.id());
    }

    // ── Private ───────────────────────────────────────────────────────────────

    private Map<String, CircuitBreaker> buildBreakers(BackendNode primary,
                                                       List<BackendNode> replicas) {
        return Stream.concat(Stream.of(primary), replicas.stream())
                .collect(Collectors.toMap(
                        BackendNode::id,
                        n -> new CircuitBreaker(n.id(),
                                CB_WINDOW_SIZE,
                                CB_FAILURE_THRESHOLD,
                                CB_OPEN_DURATION_MS),
                        (a, b) -> a,
                        ConcurrentHashMap::new));
    }
}
