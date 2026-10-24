package io.latchkey.auth;

import java.util.function.LongSupplier;

/** Thread-safe token bucket. Time comes from an injected nanosecond clock so tests need no sleeps. */
public class TokenBucket {

  private final double capacity;
  private final double refillPerNano;
  private final LongSupplier nanoClock;
  private double tokens;
  private long lastRefill;

  public TokenBucket(double capacity, double refillPerSecond, LongSupplier nanoClock) {
    this.capacity = capacity;
    this.refillPerNano = refillPerSecond / 1_000_000_000d;
    this.nanoClock = nanoClock;
    this.tokens = capacity;
    this.lastRefill = nanoClock.getAsLong();
  }

  public synchronized boolean tryAcquire() {
    long now = nanoClock.getAsLong();
    tokens = Math.min(capacity, tokens + (now - lastRefill) * refillPerNano);
    lastRefill = now;
    if (tokens >= 1d) {
      tokens -= 1d;
      return true;
    }
    return false;
  }

  /** Whole seconds until one token is available (at least 1). */
  public synchronized int retryAfterSeconds() {
    double missing = Math.max(0d, 1d - tokens);
    return (int) Math.max(1, Math.ceil(missing / (refillPerNano * 1_000_000_000d)));
  }
}
