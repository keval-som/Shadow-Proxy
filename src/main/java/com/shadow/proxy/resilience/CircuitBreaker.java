package com.shadow.proxy.resilience;

public class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    private final String nodeId;           // which node this protects
    private final int    windowSize;       // e.g. 20 requests
    private final int    failureThreshold; // e.g. 50 → 50%
    private final long   openDurationMs;   // e.g. 30_000

    // Sliding window ring buffer — true = failure
    private final boolean[] window;
    private int  windowIndex  = 0;
    private int  failureCount = 0;

    // State machine
    private volatile State state = State.CLOSED;
    private long openedAt = 0L; // timestamp when circuit opened

    public CircuitBreaker(String nodeId, int windowSize,
                          int failureThresholdPct, long openDurationMs) {
        this.nodeId           = nodeId;
        this.windowSize       = windowSize;
        this.failureThreshold = failureThresholdPct;
        this.openDurationMs   = openDurationMs;
        this.window           = new boolean[windowSize];
    }

    // ── Called by BackendHandler before attempting a DB call ─────────────────
    public synchronized boolean allowRequest() {
        return switch (state) {
            case CLOSED    -> true;
            case OPEN      -> tryTransitionToHalfOpen();
            case HALF_OPEN -> false; // only 1 probe allowed at a time
        };
    }

    // ── Called by BackendHandler after a DB call succeeds ────────────────────
    public synchronized void recordSuccess() {
        recordResult(false); // false = not a failure
        if (state == State.HALF_OPEN) {
            transitionTo(State.CLOSED);
        }
    }

    // ── Called by BackendHandler after a DB call fails ───────────────────────
    public synchronized void recordFailure() {
        recordResult(true);
        if (state == State.HALF_OPEN) {
            transitionTo(State.OPEN); // probe failed → re-open
        } else if (failureRatePct() >= failureThreshold) {
            transitionTo(State.OPEN);
        }
    }

    public State getState() { return state; }

    // ── Private helpers ──────────────────────────────────────────────────────

    private void recordResult(boolean isFailure) {
        // subtract old value before overwriting
        // hint: if window[windowIndex] was a failure, decrement failureCount
        // then set window[windowIndex] = isFailure
        // add to failureCount if isFailure
        // advance windowIndex
    }

    private int failureRatePct() {
        // (failureCount * 100) / windowSize
    }

    private boolean tryTransitionToHalfOpen() {
        // check if openDurationMs has elapsed since openedAt
        // if yes → transitionTo(HALF_OPEN), return true
        // if no  → return false (still blocked)
    }

    private void transitionTo(State next) {
        // set state = next
        // if next == OPEN: record openedAt = System.currentTimeMillis()
        // log the transition
    }
} {
    
}
