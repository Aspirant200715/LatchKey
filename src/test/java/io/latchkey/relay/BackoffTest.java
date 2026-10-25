package io.latchkey.relay;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.Test;

class BackoffTest {

  private final Backoff backoff = new Backoff(1.0, 60.0, new Random(42));

  @Test
  void ceilingDoublesPerAttemptAndIsCapped() {
    assertThat(Backoff.ceilingSeconds(1, 1.0, 60.0)).isEqualTo(1.0);
    assertThat(Backoff.ceilingSeconds(2, 1.0, 60.0)).isEqualTo(2.0);
    assertThat(Backoff.ceilingSeconds(3, 1.0, 60.0)).isEqualTo(4.0);
    assertThat(Backoff.ceilingSeconds(7, 1.0, 60.0)).isEqualTo(60.0);
    assertThat(Backoff.ceilingSeconds(500, 1.0, 60.0)).isEqualTo(60.0); // no overflow
  }

  @Test
  void delayStaysWithinFloorAndCeilingForEveryAttempt() {
    for (int attempt = 1; attempt <= 100; attempt++) {
      double ceiling = Backoff.ceilingSeconds(attempt, 1.0, 60.0);
      for (int i = 0; i < 50; i++) {
        double d = backoff.delaySeconds(attempt);
        assertThat(d).isBetween(0.25, ceiling);
      }
    }
  }

  @Test
  void jitterActuallyVaries() {
    long distinct =
        java.util.stream.IntStream.range(0, 100)
            .mapToObj(i -> backoff.delaySeconds(6))
            .distinct()
            .count();
    assertThat(distinct).isGreaterThan(10);
  }

  @Test
  void nonPositiveAttemptIsTreatedAsFirst() {
    assertThat(Backoff.ceilingSeconds(0, 1.0, 60.0)).isEqualTo(1.0);
    assertThat(Backoff.ceilingSeconds(-3, 1.0, 60.0)).isEqualTo(1.0);
  }
}
