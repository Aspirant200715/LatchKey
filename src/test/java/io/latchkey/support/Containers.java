package io.latchkey.support;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.redpanda.RedpandaContainer;

/** Containers shared by every test class in one run; Ryuk removes them when the JVM exits. */
public final class Containers {

  public static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

  private static RedpandaContainer redpanda;

  static {
    POSTGRES.start();
  }

  private Containers() {}

  /** Started on first use, so API-only tests never pay for a broker. */
  public static synchronized RedpandaContainer redpanda() {
    if (redpanda == null) {
      redpanda = new RedpandaContainer(
              DockerImageName.parse("redpandadata/redpanda:v26.2.6")
                  .asCompatibleSubstituteFor("docker.redpanda.com/redpandadata/redpanda"));
      redpanda.start();
    }
    return redpanda;
  }
}
