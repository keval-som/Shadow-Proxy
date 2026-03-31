package com.shadow.proxy.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sliding-window circuit breaker, one instance per BackendNode.
 *
 * State machine:
 * CLOSED ──(failure rate >= threshold)──► OPEN
 * ▲ │
 * │ (probe succeeds) │ (openDurationMs elapsed)
 * └────────── HALF_OPEN ◄─────────────────┘
 * │
 * └─(probe fails)──► OPEN
 */
public class CircuitBreaker {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    public enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    private final String nodeId;
    private final int windowSize; // number of requests in sliding window
    private final int failureThresholdPct; // e.g. 50 → trip at 50% failures
    private final long openDurationMs; // how long to stay OPEN before probing

    // Ring buffer: true = failure, false = success
    private final boolean[] window;
    private int windowIndex = 0;
    private int failureCount = 0;
    private int totalRecorded = 0; // guard: don't trip before window is full

    private volatile State state = State.CLOSED;
    private long openedAt = 0L;

    public CircuitBreaker(String nodeId,
            int windowSize,
            int failureThresholdPct,
            long openDurationMs) {
        this.nodeId = nodeId;
        this.windowSize = windowSize;
        this.failureThresholdPct = failureThresholdPct;
        this.openDurationMs = openDurationMs;
        this.window = new boolean[windowSize];
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /**
     * Called before attempting a backend DB call.
     * Returns true if the request should proceed, false to fail-fast.
     */
    public synchronized boolean allowRequest() {
        return switch (state) {
            case CLOSED -> true;
            case OPEN -> tryTransitionToHalfOpen();
            case HALF_OPEN -> false; // only one probe in flight at a time
        };
    }

    /** Called when the backend call completes successfully. */
    public synchronized void recordSuccess() {
        recordResult(false);
        if (state == State.HALF_OPEN) {
            transitionTo(State.CLOSED);
        }
    }

    /** Called when the backend call fails (timeout, connection refused, etc.). */
    public synchronized void recordFailure() {
        recordResult(true);
        if (state == State.HALF_OPEN) {
            transitionTo(State.OPEN);
        } else if (state == State.CLOSED && isWindowFull() && failureRatePct() >= failureThresholdPct) {
            transitionTo(State.OPEN);
        }
    }

    public State getState() {
        return state;
    }

    public String getNodeId() {
        return nodeId;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Writes a result into the ring buffer, maintaining the failure count.
     * Overwrites the oldest entry, subtracting it before adding the new one.
     */
    private void recordResult(boolean isFailure) {
        // Remove contribution of the slot we are about to overwrite
        if (window[windowIndex]) {
            failureCount--;
        }

        // Write new result
        window[windowIndex] = isFailure;
        if (isFailure) {
            failureCount++;
        }

        // Advance ring buffer pointer (wraps around at windowSize)
        windowIndex = (windowIndex + 1) % windowSize;

        if (totalRecorded < windowSize) {
            totalRecorded++;
        }
    }

    /** Failure rate as an integer percentage across the current window. */
    private int failureRatePct() {
        int effective = Math.min(totalRecorded, windowSize);
        if (effective == 0)
            return 0;
        return (failureCount * 100) / effective;
    }

    /** True once the window has seen at least windowSize requests. */
    private boolean isWindowFull() {
        return totalRecorded >= windowSize;
    }

    /**
     * When OPEN: checks whether enough time has passed to attempt a probe.
     * If yes, transitions to HALF_OPEN and allows the request through.
     */
    private boolean tryTransitionToHalfOpen() {
        if (System.currentTimeMillis() - openedAt >= openDurationMs) {
            transitionTo(State.HALF_OPEN);
            return true;
        }
        return false;
    }

    private void transitionTo(State next) {
        State previous = this.state;
        this.state = next;
        if (next == State.OPEN) {
            openedAt = System.currentTimeMillis();
        }
        log.warn("CircuitBreaker [{}]: {} → {}", nodeId, previous, next);
    }
}
