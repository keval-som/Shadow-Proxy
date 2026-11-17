package com.shadow.proxy.resilience;

import org.junit.jupiter.api.Test;

import static com.shadow.proxy.resilience.CircuitBreaker.State.*;
import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerTest {
    @Test
    void doesNotOpenUntilWindowIsFull() {
        var breaker = new CircuitBreaker("node", 4, 50, Long.MAX_VALUE);
        for (int i = 0; i < 3; i++) breaker.recordFailure();
        assertEquals(CLOSED, breaker.getState());
        assertTrue(breaker.allowRequest());
    }

    @Test
    void opensAtThresholdAndRejectsRequestsDuringCooldown() {
        var breaker = new CircuitBreaker("node", 4, 50, Long.MAX_VALUE);
        breaker.recordSuccess();
        breaker.recordFailure();
        breaker.recordSuccess();
        breaker.recordFailure();
        assertEquals(OPEN, breaker.getState());
        assertFalse(breaker.allowRequest());
    }

    @Test
    void evictsOldFailuresFromSlidingWindow() {
        var breaker = new CircuitBreaker("node", 4, 50, Long.MAX_VALUE);
        breaker.recordFailure();
        breaker.recordSuccess();
        breaker.recordSuccess();
        breaker.recordSuccess();
        breaker.recordSuccess(); // overwrites the only failure
        breaker.recordFailure();
        assertEquals(CLOSED, breaker.getState());
    }

    @Test
    void allowsExactlyOneHalfOpenProbeAndClosesAfterSuccess() {
        var breaker = new CircuitBreaker("node", 1, 100, 0);
        breaker.recordFailure();
        assertTrue(breaker.allowRequest());
        assertEquals(HALF_OPEN, breaker.getState());
        assertFalse(breaker.allowRequest());
        breaker.recordSuccess();
        assertEquals(CLOSED, breaker.getState());
        assertTrue(breaker.allowRequest());
    }

    @Test
    void failedProbeReopensCircuit() {
        var breaker = new CircuitBreaker("node", 1, 100, 0);
        breaker.recordFailure();
        assertTrue(breaker.allowRequest());
        breaker.recordFailure();
        assertEquals(OPEN, breaker.getState());
    }
}
