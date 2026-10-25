package io.latchkey.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Boots the real relay against real PostgreSQL and a real Redpanda broker. The topic is shared by
 * all relay tests, so each test identifies its own events by event_id and never by "everything".
 * The context is dirtied after each class so no relay keeps polling the shared database.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("relay")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public abstract class RelayTestBase {

  public static final String TOPIC = "order.created";

  @DynamicPropertySource
  static void properties(DynamicPropertyRegistry registry) {
    registry.add("spring.datasource.url", Containers.POSTGRES::getJdbcUrl);
    registry.add("spring.datasource.username", Containers.POSTGRES::getUsername);
    registry.add("spring.datasource.password", Containers.POSTGRES::getPassword);
    registry.add("spring.kafka.bootstrap-servers", () -> Containers.redpanda().getBootstrapServers());
    registry.add("server.port", () -> "0");
    registry.add("management.server.port", () -> "0");
    registry.add("latchkey.relay.lease-seconds", () -> "2");
    registry.add("latchkey.relay.poll-ms", () -> "50");
    registry.add("latchkey.failpoints-enabled", () -> "true");
  }

  @Autowired protected JdbcTemplate jdbc;

  @BeforeEach
  void cleanOutbox() {
    jdbc.execute("TRUNCATE outbox_events");
  }

  protected UUID insertRow(UUID aggregateId, int version) {
    UUID eventId = UUID.randomUUID();
    jdbc.update(
        "INSERT INTO outbox_events (event_id, tenant_id, aggregate_type, aggregate_id,"
            + " aggregate_version, partition_key, event_type, payload, headers)"
            + " VALUES (?, ?, 'order', ?, ?, ?, 'order.created', ?::jsonb, ?::jsonb)",
        eventId,
        UUID.randomUUID(),
        aggregateId,
        version,
        aggregateId.toString(),
        "{\"order_id\":\"" + aggregateId + "\",\"version\":" + version + "}",
        "{\"traceparent\":\"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01\"}");
    return eventId;
  }

  protected List<UUID> insertRows(int n) {
    List<UUID> ids = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      ids.add(insertRow(UUID.randomUUID(), 1));
    }
    return ids;
  }

  protected long unpublished() {
    Long n = jdbc.queryForObject(
        "SELECT count(*) FROM outbox_events WHERE published_at IS NULL", Long.class);
    return n == null ? 0 : n;
  }

  protected static String headerValue(ConsumerRecord<String, String> record, String name) {
    var h = record.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
  }

  /** Reads the topic from the beginning until {@code done} accepts what has arrived, or fails. */
  protected List<ConsumerRecord<String, String>> consumeUntil(
      Predicate<List<ConsumerRecord<String, String>>> done, Duration timeout) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.redpanda().getBootstrapServers());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    List<ConsumerRecord<String, String>> all = new ArrayList<>();
    long deadline = System.nanoTime() + timeout.toNanos();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      List<TopicPartition> partitions = null;
      while (System.nanoTime() < deadline) {
        if (partitions == null) {
          List<PartitionInfo> infos = consumer.partitionsFor(TOPIC, Duration.ofSeconds(2));
          if (infos == null || infos.isEmpty()) {
            sleepQuietly(200);
            continue;
          }
          partitions = infos.stream().map(i -> new TopicPartition(TOPIC, i.partition())).toList();
          consumer.assign(partitions);
          consumer.seekToBeginning(partitions);
        }
        consumer.poll(Duration.ofMillis(300)).forEach(all::add);
        if (done.test(all)) {
          return all;
        }
      }
    }
    return all; // caller asserts on what arrived; the failure message shows the shortfall
  }

  private static void sleepQuietly(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
