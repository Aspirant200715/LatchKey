package io.latchkey.relay;

import java.util.Random;

/** Exponential backoff with full jitter and a small floor so a retry never fires instantly. */
public class Backoff {

  private static final double FLOOR_SECONDS = 0.25;

  private final double baseSeconds;
  private final double capSeconds;
  private final Random random;

  public Backoff(double baseSeconds, double capSeconds, Random random) {
    this.baseSeconds = baseSeconds;
    this.capSeconds = capSeconds;
    this.random = random;
  }

  /** Delay before the next try after {@code attempt} failed tries (1 = first failure). */
  public double delaySeconds(int attempt) {
    double ceiling = ceilingSeconds(attempt, baseSeconds, capSeconds);
    double jittered;
    synchronized (random) {
      jittered = random.nextDouble() * ceiling;
    }
    return Math.max(FLOOR_SECONDS, Math.min(ceiling, jittered));
  }

  static double ceilingSeconds(int attempt, double base, double cap) {
    int exponent = Math.min(Math.max(attempt, 1) - 1, 30); // 2^30 already exceeds any sane cap
    return Math.min(cap, base * Math.pow(2, exponent));
  }
}
