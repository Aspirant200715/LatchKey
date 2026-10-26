package io.latchkey.consumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/** Exposes broker reachability as the consumer's Actuator `kafka` readiness contributor. */
@Component("kafkaHealthIndicator")
@Profile({"consumer", "relay"})
public class KafkaReadinessHealthIndicator implements HealthIndicator, AutoCloseable {

  private static final long CHECK_TIMEOUT_SECONDS = 2;

  private final AdminClient admin;

  public KafkaReadinessHealthIndicator(KafkaAdmin kafkaAdmin) {
    Map<String, Object> properties = new HashMap<>(kafkaAdmin.getConfigurationProperties());
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 2_000);
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 2_000);
    this.admin = AdminClient.create(properties);
  }

  @Override
  public Health health() {
    try {
      var nodes = admin.describeCluster().nodes().get(CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (nodes == null || nodes.isEmpty()) {
        return Health.down().withDetail("reason", "broker returned no nodes").build();
      }
      return Health.up().withDetail("nodes", nodes.size()).build();
    } catch (Exception e) {
      return Health.down().withDetail("reason", e.getClass().getSimpleName()).build();
    }
  }

  @Override
  public void close() {
    admin.close(Duration.ofSeconds(CHECK_TIMEOUT_SECONDS));
  }
}
