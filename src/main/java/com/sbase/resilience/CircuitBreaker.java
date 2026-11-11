package com.sbase.resilience;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Small thread-safe circuit breaker for synchronous JDBC boundary calls.
 * CLOSED -> OPEN after consecutive failures; after cooldown exactly one caller
 * is admitted as HALF_OPEN probe. It is intentionally process-local: database
 * failover remains the responsibility of the JDBC endpoint list/load balancer.
 */
public final class CircuitBreaker {
    public enum State { CLOSED, OPEN, HALF_OPEN }
    private final int threshold;
    private final long openNanos;
    private final AtomicInteger failures = new AtomicInteger();
    private final AtomicLong openedAt = new AtomicLong();
    private final AtomicBoolean probeInFlight = new AtomicBoolean();

    public CircuitBreaker(int threshold, Duration openDuration) {
        if (threshold < 1 || openDuration.isNegative() || openDuration.isZero())
            throw new IllegalArgumentException("Invalid circuit breaker configuration");
        this.threshold = threshold;
        this.openNanos = openDuration.toNanos();
    }

    public <T> T execute(Supplier<T> operation) {
        boolean probe = acquirePermission();
        try {
            T result = operation.get();
            recordSuccess();
            return result;
        } catch (RuntimeException | Error failure) {
            recordFailure(probe);
            throw failure;
        }
    }

    /** Connection acquisition must not erase failures observed later at SQL execution. */
    public <T> T executeAcquisition(Supplier<T> operation) {
        boolean probe = acquirePermission();
        try {
            T result = operation.get();
            if (probe) recordSuccess();
            return result;
        } catch (RuntimeException | Error failure) {
            recordFailure(probe);
            throw failure;
        }
    }

    public void recordSuccess() {
        failures.set(0);
        openedAt.set(0);
        probeInFlight.set(false);
    }

    public void recordFailure() { recordFailure(false); }

    private void recordFailure(boolean probe) {
        if (probe) { openedAt.set(System.nanoTime()); failures.set(threshold); }
        else if (failures.incrementAndGet() >= threshold) openedAt.compareAndSet(0, System.nanoTime());
        probeInFlight.set(false);
    }

    private boolean acquirePermission() {
        long opened = openedAt.get();
        if (opened == 0) return false;
        if (System.nanoTime() - opened < openNanos)
            throw new CircuitOpenException("Dependency circuit is OPEN");
        if (!probeInFlight.compareAndSet(false, true))
            throw new CircuitOpenException("Dependency circuit HALF_OPEN probe is already running");
        return true;
    }

    public State state() {
        long opened = openedAt.get();
        if (opened == 0) return State.CLOSED;
        return System.nanoTime() - opened < openNanos ? State.OPEN : State.HALF_OPEN;
    }

    public static final class CircuitOpenException extends RuntimeException {
        public CircuitOpenException(String message) { super(message); }
    }
}
