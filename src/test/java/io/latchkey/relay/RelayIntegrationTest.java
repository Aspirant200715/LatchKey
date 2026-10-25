package io.latchkey.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.latchkey.fault.FailPoints;
import io.latchkey.support.Containers;
import io.latchkey.support.RelayTestBase;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class RelayIntegrationTest extends RelayTestBase {

  @Autowired FailPoints failPoints;

  @AfterEach
  void reset() {
    failPoints.set(FailPoints.AFTER_PUBLISH_BEFORE_MARK, null);
    var docker = Containers.redpanda().getDockerClient();
    try {
      docker.unpauseContainerCmd(Containers.redpanda().getContainerId()).exec();
    } catch (Exception ignored) {
      // not paused
    }
  }

  @Test
  void publishesEveryRowWithKeyHeadersAndMarksThemPublished() {
    List<UUID> ids = insertRows(50);

    await().atMost(Duration.ofSeconds(60)).until(() -> unpublished() == 0);

    Set<String> wanted = ids.stream().map(UUID::toString).collect(Collectors.toSet());
    List<ConsumerRecord<String, String>> records =
        consumeUntil(r -> countOurs(r, wanted) >= 50, Duration.ofSeconds(60));
    List<ConsumerRecord<String, String>> ours =
        records.stream().filter(r -> wanted.contains(headerValue(r, "event_id"))).toList();

    assertThat(ours).hasSize(50); // healthy path: exactly once on the topic
    for (ConsumerRecord<String, String> r : ours) {
      assertThat(headerValue(r, "aggregate_version")).isEqualTo("1");
      assertThat(headerValue(r, "schema_version")).isEqualTo("1");
      assertThat(headerValue(r, "event_type")).isEqualTo("order.created");
      assertThat(headerValue(r, "traceparent")).startsWith("00-0af7651916cd43dd8448eb211c80319c");
      assertThat(r.value()).contains(r.key()); // key is the aggregate id, which the payload carries
    }
    assertThat(jdbc.queryForObject(
            "SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL AND locked_by IS NULL",
            Long.class))
        .isEqualTo(50);
  }

  @Test
  void crashAfterPublishBeforeMarkRepublishesTheSameEventIdAndEventuallyMarks() {
    failPoints.set(FailPoints.AFTER_PUBLISH_BEFORE_MARK, "error");
    UUID eventId = insertRows(1).get(0);
    Set<String> wanted = Set.of(eventId.toString());

    // Published to the broker, but the row stays unmarked (the "crash").
    consumeUntil(r -> countOurs(r, wanted) >= 1, Duration.ofSeconds(60));
    assertThat(unpublished()).isEqualTo(1);

    // Recovery: the lease expires, the row is claimed again and published again.
    failPoints.set(FailPoints.AFTER_PUBLISH_BEFORE_MARK, null);
    await().atMost(Duration.ofSeconds(60)).until(() -> unpublished() == 0);

    List<ConsumerRecord<String, String>> records =
        consumeUntil(r -> countOurs(r, wanted) >= 2, Duration.ofSeconds(60));
    List<ConsumerRecord<String, String>> ours =
        records.stream().filter(r -> wanted.contains(headerValue(r, "event_id"))).toList();
    assertThat(ours.size()).isGreaterThanOrEqualTo(2); // a duplicate message...
    assertThat(ours.stream().map(r -> headerValue(r, "event_id")).distinct())
        .containsExactly(eventId.toString()); // ...with the SAME event id, so the inbox can absorb it
    assertThat(jdbc.queryForObject(
            "SELECT attempts FROM outbox_events WHERE event_id = ?", Integer.class, eventId))
        .isGreaterThanOrEqualTo(2);
  }

  @Test
  void brokerDownKeepsEveryRowAndDrainsAfterRecovery() {
    var docker = Containers.redpanda().getDockerClient();
    String broker = Containers.redpanda().getContainerId();

    docker.pauseContainerCmd(broker).exec();
    List<UUID> ids = insertRows(5);

    // Backlog grows: rows are failing and rescheduled, never deleted, never marked published.
    await().atMost(Duration.ofSeconds(90))
        .until(() -> jdbc.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE last_error IS NOT NULL", Long.class) == 5);
    assertThat(unpublished()).isEqualTo(5);
    assertThat(jdbc.queryForObject("SELECT count(*) FROM outbox_events", Long.class)).isEqualTo(5);

    docker.unpauseContainerCmd(broker).exec();
    await().atMost(Duration.ofSeconds(120)).until(() -> unpublished() == 0);

    Set<String> wanted = ids.stream().map(UUID::toString).collect(Collectors.toSet());
    List<ConsumerRecord<String, String>> records =
        consumeUntil(r -> distinctOurs(r, wanted) >= 5, Duration.ofSeconds(60));
    assertThat(distinctOurs(records, wanted)).isEqualTo(5);
  }

  @Test
  void oneAggregateKeepsVersionOrderWithinItsPartition() {
    UUID aggregate = UUID.randomUUID();
    for (int v = 1; v <= 1000; v++) {
      insertRow(aggregate, v);
    }

    await().atMost(Duration.ofSeconds(120)).until(() -> unpublished() == 0);

    List<ConsumerRecord<String, String>> records =
        consumeUntil(r -> countByKey(r, aggregate.toString()) >= 1000, Duration.ofSeconds(60));
    List<ConsumerRecord<String, String>> ours =
        records.stream().filter(r -> aggregate.toString().equals(r.key())).toList();

    assertThat(ours).hasSize(1000);
    assertThat(ours.stream().map(ConsumerRecord::partition).distinct()).hasSize(1);
    List<Integer> versions =
        ours.stream().map(r -> Integer.parseInt(headerValue(r, "aggregate_version"))).toList();
    for (int i = 1; i < versions.size(); i++) {
      assertThat(versions.get(i)).as("offset order at index %d", i).isGreaterThan(versions.get(i - 1));
    }
  }

  private static long countOurs(List<ConsumerRecord<String, String>> records, Set<String> wanted) {
    return records.stream().filter(r -> wanted.contains(headerValue(r, "event_id"))).count();
  }

  private static long distinctOurs(List<ConsumerRecord<String, String>> records, Set<String> wanted) {
    return new HashSet<>(records.stream()
            .map(r -> headerValue(r, "event_id"))
            .filter(wanted::contains)
            .toList())
        .size();
  }

  private static long countByKey(List<ConsumerRecord<String, String>> records, String key) {
    Map<String, Long> counts =
        records.stream().collect(Collectors.groupingBy(ConsumerRecord::key, Collectors.counting()));
    return counts.getOrDefault(key, 0L);
  }
}
