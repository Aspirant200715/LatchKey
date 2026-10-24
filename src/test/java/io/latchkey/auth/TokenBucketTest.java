package io.latchkey.auth;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketTest {

  private final AtomicLong now = new AtomicLong(0);

  @Test
  void allowsUpToCapacityThenRejects() {
    TokenBucket bucket = new TokenBucket(3, 1, now::get);
    assertThat(bucket.tryAcquire()).isTrue();
    assertThat(bucket.tryAcquire()).isTrue();
    assertThat(bucket.tryAcquire()).isTrue();
    assertThat(bucket.tryAcquire()).isFalse();
  }

  @Test
  void refillsOverTimeButNeverAboveCapacity() {
    TokenBucket bucket = new TokenBucket(2, 1, now::get);
    bucket.tryAcquire();
    bucket.tryAcquire();
    assertThat(bucket.tryAcquire()).isFalse();

    now.addAndGet(1_000_000_000L); // +1s => one token
    assertThat(bucket.tryAcquire()).isTrue();
    assertThat(bucket.tryAcquire()).isFalse();

    now.addAndGet(60_000_000_000L); // long idle must not exceed capacity
    assertThat(bucket.tryAcquire()).isTrue();
    assertThat(bucket.tryAcquire()).isTrue();
    assertThat(bucket.tryAcquire()).isFalse();
  }

  @Test
  void retryAfterIsAtLeastOneSecond() {
    TokenBucket bucket = new TokenBucket(1, 100, now::get);
    bucket.tryAcquire();
    assertThat(bucket.tryAcquire()).isFalse();
    assertThat(bucket.retryAfterSeconds()).isGreaterThanOrEqualTo(1);
  }
}
