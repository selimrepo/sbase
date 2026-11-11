package com.sbase.resilience;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

class CircuitBreakerTest {
    @Test void opensAfterThresholdAndAllowsSingleHalfOpenProbe() throws Exception {
        CircuitBreaker breaker = new CircuitBreaker(2, Duration.ofMillis(40));
        AtomicInteger calls = new AtomicInteger();
        assertThrows(IllegalStateException.class, () -> breaker.execute(() -> { calls.incrementAndGet(); throw new IllegalStateException(); }));
        assertThrows(IllegalStateException.class, () -> breaker.execute(() -> { calls.incrementAndGet(); throw new IllegalStateException(); }));
        assertEquals(CircuitBreaker.State.OPEN, breaker.state());
        assertThrows(CircuitBreaker.CircuitOpenException.class, () -> breaker.execute(calls::incrementAndGet));
        Thread.sleep(60);
        assertEquals(3, breaker.execute(calls::incrementAndGet));
        assertEquals(CircuitBreaker.State.CLOSED, breaker.state());
    }
}
